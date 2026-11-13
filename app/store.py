"""SQLite-backed job history so state survives service restarts."""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time

FINAL_STATES = ("succeeded", "failed", "cancelled", "interrupted")
ACTIVE_STATES = ("queued", "running", "cancelling")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    filename TEXT NOT NULL,
    size INTEGER NOT NULL,
    state TEXT NOT NULL,
    temp_db TEXT,
    created_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL,
    message TEXT NOT NULL DEFAULT '',
    errors TEXT NOT NULL DEFAULT '[]',
    row_counts TEXT NOT NULL DEFAULT '[]',
    skipped TEXT NOT NULL DEFAULT '{}'
)
"""

_COLS = (
    "id", "filename", "size", "state", "temp_db", "created_at", "started_at",
    "finished_at", "message", "errors", "row_counts", "skipped",
)


def _encode(rec: dict) -> dict:
    r = dict(rec)
    r["errors"] = json.dumps(rec.get("errors") or [], ensure_ascii=False)
    r["row_counts"] = json.dumps(rec.get("row_counts") or [], ensure_ascii=False)
    r["skipped"] = json.dumps(rec.get("skipped") or {}, ensure_ascii=False)
    return r


def _decode(row: sqlite3.Row) -> dict:
    d = dict(row)
    d["errors"] = json.loads(d["errors"])
    d["row_counts"] = json.loads(d["row_counts"])
    d["skipped"] = json.loads(d["skipped"])
    return d


class JobStore:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    def create(self, rec: dict) -> None:
        r = _encode(rec)
        with self._lock:
            self._conn.execute(
                f"INSERT INTO jobs ({','.join(_COLS)}) "
                f"VALUES ({','.join('?' * len(_COLS))})",
                [r[c] for c in _COLS],
            )
            self._conn.commit()

    def update(self, rec: dict) -> None:
        r = _encode(rec)
        sets = ",".join(f"{c}=?" for c in _COLS if c != "id")
        with self._lock:
            self._conn.execute(
                f"UPDATE jobs SET {sets} WHERE id=?",
                [r[c] for c in _COLS if c != "id"] + [r["id"]],
            )
            self._conn.commit()

    def get(self, job_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
        return _decode(row) if row else None

    def list(self, limit: int = 100) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs ORDER BY "
                "CASE WHEN state IN ('queued','running','cancelling') THEN 0 ELSE 1 END, "
                "created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_decode(r) for r in rows]

    def mark_interrupted(self) -> int:
        """Mark every non-final job as interrupted (used at service startup)."""
        now = time.time()
        placeholders = ",".join("?" * len(FINAL_STATES))
        with self._lock:
            cur = self._conn.execute(
                f"UPDATE jobs SET state='interrupted', finished_at=?, "
                f"message='服务重启：试恢复被中断，遗留临时库已清理' "
                f"WHERE state NOT IN ({placeholders})",
                (now, *FINAL_STATES),
            )
            self._conn.commit()
            return cur.rowcount
