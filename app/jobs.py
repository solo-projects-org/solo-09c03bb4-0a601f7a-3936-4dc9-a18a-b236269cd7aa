"""Trial-restore job manager.

Guarantees:
  * at most ``slots`` batches run concurrently; extra submissions are rejected;
  * on success, failure or cancellation a batch only finishes (and its slot is
    released) after the pg_restore process has been reaped, all database
    sessions to the temp database are gone and the temp database is dropped;
  * once cancellation is accepted, no result report is ever published.
"""
from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
import uuid

import config
import pgadmin
from store import FINAL_STATES, JobStore
from tocfilter import filter_toc

log = logging.getLogger("jobs")

MAX_ERROR_LINES = 200


class Cancelled(Exception):
    """Raised inside the worker once cancellation has been accepted."""


class JobFailed(Exception):
    def __init__(self, message: str, errors: list[str] | None = None):
        super().__init__(message)
        self.message = message
        self.errors = list(errors or [])


class Job:
    def __init__(self, filename: str, size: int, dump_path: str, owned_file: bool):
        self.id = uuid.uuid4().hex
        self.filename = filename
        self.size = size
        self.dump_path = dump_path
        self.owned_file = owned_file  # delete the file when the job ends
        self.state = "queued"
        self.temp_db: str | None = None
        self.created_at = time.time()
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.message = ""
        self.errors: list[str] = []
        self.row_counts: list[dict] = []
        self.skipped: dict[str, int] = {}
        self.cancel_event = threading.Event()
        self.proc: subprocess.Popen | None = None
        self.lock = threading.Lock()

    def to_record(self) -> dict:
        return {
            "id": self.id,
            "filename": self.filename,
            "size": self.size,
            "state": self.state,
            "temp_db": self.temp_db,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "message": self.message,
            "errors": self.errors,
            "row_counts": self.row_counts,
            "skipped": self.skipped,
        }


class JobManager:
    def __init__(self, store: JobStore, slots: int, job_timeout_s: int):
        self.store = store
        self.slots = slots
        self.job_timeout_s = job_timeout_s
        self._sem = threading.BoundedSemaphore(slots)
        self._active: dict[str, Job] = {}
        self._active_lock = threading.Lock()

    # ---------------- submissions ----------------

    def submit(self, filename: str, dump_path: str, size: int, owned_file: bool):
        """Returns the new Job, or None when all slots are busy."""
        if not self._sem.acquire(blocking=False):
            return None
        job = Job(filename, size, dump_path, owned_file)
        try:
            self.store.create(job.to_record())
            with self._active_lock:
                self._active[job.id] = job
            threading.Thread(
                target=self._run, args=(job,),
                name=f"restore-{job.id[:8]}", daemon=True,
            ).start()
            return job
        except Exception:
            self._sem.release()
            raise

    def cancel(self, job_id: str) -> str:
        """Accept cancellation. Returns 'accepted' | 'already_final' | 'not_found'."""
        with self._active_lock:
            job = self._active.get(job_id)
        if job is None:
            return "already_final" if self.store.get(job_id) else "not_found"
        with job.lock:
            if job.state in FINAL_STATES:
                return "already_final"
            job.cancel_event.set()
            job.state = "cancelling"
            job.message = "取消已受理：正在停止恢复进程与数据库会话，并清理临时库…"
            self.store.update(job.to_record())
            temp_db = job.temp_db
        if temp_db:
            # Unwinds a blocking row-count query on the temp database, if any.
            try:
                pgadmin.terminate_backends(temp_db)
            except Exception:
                log.exception("terminate_backends failed for %s", temp_db)
        return "accepted"

    def slots_in_use(self) -> int:
        with self._active_lock:
            return len(self._active)

    # ---------------- worker ----------------

    def _run(self, job: Job) -> None:
        outcome, message, errors = "failed", "", []
        row_counts: list[dict] = []
        skipped: dict[str, int] = {}
        list_path = job.dump_path + ".toclist"
        try:
            with job.lock:
                job.state = "running"
                job.started_at = time.time()
                job.temp_db = "rc_" + uuid.uuid4().hex[:12]
                self.store.update(job.to_record())
            self._check_cancel(job)
            pgadmin.create_database(job.temp_db)

            toc_text = self._read_toc(job)
            toc = filter_toc(toc_text)
            skipped = toc.skipped
            with open(list_path, "w", encoding="utf-8") as f:
                f.write("\n".join(toc.lines) + "\n")

            self._check_cancel(job)
            rc, err_lines = self._run_pg_restore(job, list_path)
            self._check_cancel(job)
            if rc != 0:
                raise JobFailed(f"pg_restore 退出码 {rc}：恢复过程中出现错误", err_lines)

            row_counts = self._count_rows(job)
            outcome, message = "succeeded", "试恢复成功"
        except Cancelled:
            outcome = "cancelled"
        except JobFailed as e:
            outcome, message, errors = "failed", e.message, e.errors
        except Exception as e:  # any worker error must still clean up
            log.exception("job %s failed with unexpected error", job.id)
            outcome, message, errors = "failed", f"内部错误：{e}", [str(e)]
        finally:
            # 1) make sure the restore process is stopped and reaped;
            # 2) make sure no database session remains on the temp database;
            # 3) drop the temp database;
            # only then publish the final state and release the slot.
            self._cleanup(job)
            with job.lock:
                if job.cancel_event.is_set():
                    # Cancellation was accepted: never publish a late report.
                    job.state = "cancelled"
                    job.message = "已取消：恢复进程与数据库会话均已停止，临时库已清理"
                    job.errors, job.row_counts, job.skipped = [], [], {}
                else:
                    job.state = outcome
                    job.message = message
                    job.errors, job.row_counts, job.skipped = errors, row_counts, skipped
                job.finished_at = time.time()
                self.store.update(job.to_record())
                with self._active_lock:
                    self._active.pop(job.id, None)
            self._remove_file(job.dump_path if job.owned_file else None)
            self._remove_file(list_path)
            self._sem.release()

    # ---------------- stages ----------------

    @staticmethod
    def _check_cancel(job: Job) -> None:
        if job.cancel_event.is_set():
            raise Cancelled()

    def _read_toc(self, job: Job) -> str:
        try:
            r = subprocess.run(
                ["pg_restore", "-l", job.dump_path],
                capture_output=True, text=True, timeout=120,
            )
        except subprocess.TimeoutExpired:
            raise JobFailed("读取备份目录（TOC）超时")
        except FileNotFoundError:
            raise JobFailed("服务缺少 pg_restore 工具")
        if r.returncode != 0:
            raise JobFailed(
                "无法读取备份目录：文件不是有效的 pg_dump 自定义格式备份，或已损坏",
                _cap_lines(r.stderr.splitlines()),
            )
        return r.stdout

    def _run_pg_restore(self, job: Job, list_path: str) -> tuple[int, list[str]]:
        cmd = [
            "pg_restore", "-w", "--no-owner", "--no-privileges",
            "-h", config.PGHOST, "-p", str(config.PGPORT), "-U", config.PGUSER,
            "-L", list_path, "-d", job.temp_db, job.dump_path,
        ]
        log.info("job %s: %s", job.id, " ".join(cmd))
        env = dict(os.environ, PGPASSWORD=config.PGPASSWORD)
        try:
            proc = subprocess.Popen(
                cmd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE, text=True, env=env,
            )
        except FileNotFoundError:
            raise JobFailed("服务缺少 pg_restore 工具")
        job.proc = proc
        err_lines: list[str] = []

        def _drain() -> None:  # avoid pipe-buffer deadlock on chatty stderr
            for line in proc.stderr:
                err_lines.append(line.rstrip("\n"))

        drain = threading.Thread(target=_drain, daemon=True)
        drain.start()
        deadline = time.time() + self.job_timeout_s
        try:
            while proc.poll() is None:
                if job.cancel_event.is_set():
                    _stop_proc(proc)
                    raise Cancelled()
                if time.time() > deadline:
                    _stop_proc(proc)
                    raise JobFailed(f"恢复超时（>{self.job_timeout_s}s），进程已终止")
                time.sleep(0.2)
        finally:
            drain.join(timeout=5)
        return proc.returncode, _cap_lines(err_lines)

    def _count_rows(self, job: Job) -> list[dict]:
        from psycopg2 import sql  # local import keeps this module testable

        conn = pgadmin.connect(job.temp_db)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT n.nspname, c.relname FROM pg_class c "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relkind = 'r' "
                    "AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
                    "ORDER BY 1, 2"
                )
                tables = cur.fetchall()
                counts = []
                for nsp, rel in tables:
                    self._check_cancel(job)
                    cur.execute(
                        sql.SQL("SELECT count(*) FROM {}.{}").format(
                            sql.Identifier(nsp), sql.Identifier(rel)
                        )
                    )
                    counts.append(
                        {"schema": nsp, "table": rel, "rows": int(cur.fetchone()[0])}
                    )
                return counts
        except Cancelled:
            raise
        except Exception as e:
            # e.g. our session was terminated by an accepted cancellation
            if job.cancel_event.is_set():
                raise Cancelled()
            raise JobFailed(f"统计表行数失败：{e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

    # ---------------- cleanup ----------------

    def _cleanup(self, job: Job) -> None:
        proc, job.proc = job.proc, None
        if proc is not None and proc.poll() is None:
            _stop_proc(proc)
        if job.temp_db:
            try:
                pgadmin.terminate_backends(job.temp_db)
            except Exception:
                log.exception("terminate_backends failed for %s", job.temp_db)
            try:
                pgadmin.wait_no_sessions(job.temp_db, timeout_s=10)
            except Exception:
                log.exception("wait_no_sessions failed for %s", job.temp_db)
            try:
                pgadmin.drop_database(job.temp_db)
            except Exception:
                log.exception("drop_database failed for %s", job.temp_db)

    @staticmethod
    def _remove_file(path: str | None) -> None:
        if not path:
            return
        try:
            os.unlink(path)
        except OSError:
            pass


def _stop_proc(proc) -> None:
    try:
        proc.terminate()
    except Exception:
        return
    try:
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
        try:
            proc.wait(timeout=5)
        except Exception:
            pass


def _cap_lines(lines: list[str]) -> list[str]:
    if len(lines) <= MAX_ERROR_LINES:
        return lines
    half = MAX_ERROR_LINES // 2
    return (
        lines[:half]
        + [f"……（省略 {len(lines) - MAX_ERROR_LINES} 行）……"]
        + lines[-half:]
    )
