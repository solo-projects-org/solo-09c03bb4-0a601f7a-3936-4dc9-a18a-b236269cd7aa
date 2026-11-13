"""FastAPI web app: upload pg_dump custom-format backups and trial-restore
them into throwaway databases on the local PostgreSQL instance."""
from __future__ import annotations

import logging
import os
import re
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse

import config
import pgadmin
from jobs import JobManager
from store import JobStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("main")

store = JobStore(os.path.join(config.DATA_DIR, "jobs.db"))
manager = JobManager(store, slots=config.SLOTS, job_timeout_s=config.JOB_TIMEOUT_S)
UPLOADS_DIR = os.path.join(config.DATA_DIR, "uploads")
os.makedirs(UPLOADS_DIR, exist_ok=True)

STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

SAMPLE_DESCRIPTIONS = {
    "normal": "正常备份：3 张普通表（主键/外键/唯一约束/索引），约 10 万行",
    "slow": "慢速恢复：单表约 200 万行并建索引，用于演示并行与取消",
    "corrupt": "损坏备份：正常备份中段被清零，恢复将报错",
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 先清理上一轮的遗留恢复活动和临时库，再开放接收新批次。
    pgadmin.wait_for_pg()
    killed = pgadmin.kill_stray_pg_restore()
    if killed:
        log.warning("terminated stray pg_restore processes: %s", killed)
    removed = pgadmin.cleanup_temp_databases()
    if removed:
        log.warning("dropped leftover temp databases: %s", removed)
    interrupted = store.mark_interrupted()
    if interrupted:
        log.warning("marked %d interrupted job(s)", interrupted)
    for name in os.listdir(UPLOADS_DIR):
        try:
            os.unlink(os.path.join(UPLOADS_DIR, name))
        except OSError:
            pass
    log.info("startup cleanup finished; accepting new batches (slots=%d)", config.SLOTS)
    yield


app = FastAPI(title="PostgreSQL 备份试恢复验证", lifespan=lifespan)


def _slots() -> dict:
    return {"total": config.SLOTS, "in_use": manager.slots_in_use()}


def _silent_unlink(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/api/slots")
def slots():
    return _slots()


@app.get("/api/samples")
def list_samples():
    out = []
    if os.path.isdir(config.SAMPLES_DIR):
        for name in sorted(os.listdir(config.SAMPLES_DIR)):
            if not name.endswith(".dump"):
                continue
            key = name[: -len(".dump")]
            path = os.path.join(config.SAMPLES_DIR, name)
            out.append(
                {
                    "name": key,
                    "file": name,
                    "size": os.path.getsize(path),
                    "description": SAMPLE_DESCRIPTIONS.get(key, ""),
                }
            )
    return out


@app.post("/api/jobs", status_code=201)
async def upload_job(file: UploadFile = File(...)):
    filename = os.path.basename(file.filename or "") or "backup.dump"
    tmp_path = os.path.join(UPLOADS_DIR, uuid.uuid4().hex + ".dump")
    size = 0
    try:
        with open(tmp_path, "wb") as f:
            while chunk := await file.read(1 << 20):
                size += len(chunk)
                if size > config.MAX_UPLOAD_BYTES:
                    raise HTTPException(
                        413,
                        f"文件超过大小限制（{config.MAX_UPLOAD_BYTES // (1 << 20)} MiB）",
                    )
                f.write(chunk)
    except HTTPException:
        _silent_unlink(tmp_path)
        raise
    except Exception as e:
        _silent_unlink(tmp_path)
        raise HTTPException(500, f"保存上传文件失败：{e}")
    if size == 0:
        _silent_unlink(tmp_path)
        raise HTTPException(400, "上传文件为空")
    job = manager.submit(filename, tmp_path, size, owned_file=True)
    if job is None:
        _silent_unlink(tmp_path)
        raise HTTPException(
            409, f"当前已有 {config.SLOTS} 批试恢复在进行，请等待其中一批结束后再试"
        )
    return {"job": store.get(job.id), "slots": _slots()}


@app.post("/api/jobs/sample/{name}", status_code=201)
def sample_job(name: str):
    if not re.fullmatch(r"[a-z0-9_]+", name):
        raise HTTPException(400, "非法示例名")
    path = os.path.join(config.SAMPLES_DIR, f"{name}.dump")
    if not os.path.isfile(path):
        raise HTTPException(404, "示例备份不存在")
    size = os.path.getsize(path)
    job = manager.submit(f"[示例] {name}.dump", path, size, owned_file=False)
    if job is None:
        raise HTTPException(
            409, f"当前已有 {config.SLOTS} 批试恢复在进行，请等待其中一批结束后再试"
        )
    return {"job": store.get(job.id), "slots": _slots()}


@app.get("/api/jobs")
def list_jobs():
    return {"slots": _slots(), "jobs": store.list(100)}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    rec = store.get(job_id)
    if rec is None:
        raise HTTPException(404, "批次不存在")
    return rec


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str):
    result = manager.cancel(job_id)
    if result == "not_found":
        raise HTTPException(404, "批次不存在")
    if result == "already_final":
        raise HTTPException(409, "批次已结束，无法取消")
    return {"accepted": True}
