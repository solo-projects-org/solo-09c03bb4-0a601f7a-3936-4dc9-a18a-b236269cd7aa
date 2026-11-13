"""Admin helpers against the local PostgreSQL instance.

All cleanup is strictly scoped to temporary databases named ``rc_<12 hex>``;
every other database is left untouched.
"""
from __future__ import annotations

import logging
import os
import re
import signal
import time
from contextlib import closing

import psycopg2
from psycopg2 import sql

import config

log = logging.getLogger("pgadmin")

TEMP_DB_RE = re.compile(r"^rc_[0-9a-f]{12}$")
_TEMP_DB_RE_BYTES = re.compile(rb"rc_[0-9a-f]{12}")


def connect(dbname: str | None = None):
    conn = psycopg2.connect(
        host=config.PGHOST,
        port=config.PGPORT,
        user=config.PGUSER,
        password=config.PGPASSWORD,
        dbname=dbname or config.PGMAINTDB,
        connect_timeout=10,
    )
    conn.autocommit = True
    return conn


def wait_for_pg(timeout_s: int = 90) -> None:
    deadline = time.time() + timeout_s
    while True:
        try:
            with closing(connect()):
                log.info("postgresql is reachable")
                return
        except Exception as e:
            if time.time() > deadline:
                raise
            log.info("waiting for postgresql: %s", e)
            time.sleep(2)


def create_database(name: str) -> None:
    with closing(connect()) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))


def terminate_backends(name: str) -> None:
    """Terminate every session connected to the given database."""
    with closing(connect()) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (name,),
        )


def wait_no_sessions(name: str, timeout_s: float = 10.0) -> bool:
    """Wait until no session remains on the given database."""
    deadline = time.time() + timeout_s
    with closing(connect()) as conn, conn.cursor() as cur:
        while True:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = %s", (name,)
            )
            if cur.fetchone()[0] == 0:
                return True
            if time.time() > deadline:
                return False
            time.sleep(0.2)


def drop_database(name: str) -> None:
    with closing(connect()) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name))
        )


def list_temp_databases() -> list[str]:
    with closing(connect()) as conn, conn.cursor() as cur:
        cur.execute("SELECT datname FROM pg_database")
        return [r[0] for r in cur.fetchall() if TEMP_DB_RE.match(r[0])]


def cleanup_temp_databases() -> list[str]:
    """Drop every leftover trial-restore temp database (and only those)."""
    removed = []
    for name in list_temp_databases():
        log.info("cleaning up leftover temp database %s", name)
        try:
            terminate_backends(name)
            wait_no_sessions(name)
            drop_database(name)
            removed.append(name)
        except Exception:
            log.exception("failed to drop leftover temp database %s", name)
    return removed


def kill_stray_pg_restore() -> list[int]:
    """Best-effort SIGTERM for pg_restore processes referencing our temp DBs.

    Only relevant when the app runs outside a container; a container restart
    kills its processes anyway.
    """
    killed = []
    me = os.getpid()
    try:
        pids = os.listdir("/proc")
    except OSError:
        return killed
    for pid_s in pids:
        if not pid_s.isdigit():
            continue
        pid = int(pid_s)
        if pid == me:
            continue
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read()
        except OSError:
            continue
        if b"pg_restore" in cmdline and _TEMP_DB_RE_BYTES.search(cmdline):
            try:
                os.kill(pid, signal.SIGTERM)
                killed.append(pid)
                log.warning("sent SIGTERM to stray pg_restore pid=%d", pid)
            except OSError:
                pass
    return killed
