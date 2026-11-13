#!/usr/bin/env python3
"""State-machine tests for the trial-restore job manager.

Runs without Docker/PostgreSQL: psycopg2 is stubbed, pgadmin and subprocess
are faked. Verifies the tricky guarantees:
  * at most N concurrent batches (extra submissions rejected);
  * a batch only reaches a final state after process/session/temp-db cleanup;
  * once cancellation is accepted, no result report is published;
  * the slot is released only after cleanup, so a new batch can start.
"""
import io
import os
import subprocess as real_subprocess
import sys
import tempfile
import threading
import time
import types
import unittest

# ---- stub psycopg2 (not installed in this environment) ----
psycopg2 = types.ModuleType("psycopg2")
sql_mod = types.ModuleType("psycopg2.sql")


class _Identifier:
    def __init__(self, *args):
        self.args = args


class _SQL:
    def __init__(self, text):
        self.text = text

    def format(self, *args, **kwargs):
        return self


sql_mod.Identifier = _Identifier
sql_mod.SQL = _SQL
psycopg2.sql = sql_mod
psycopg2.connect = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no pg in tests"))
sys.modules["psycopg2"] = psycopg2
sys.modules["psycopg2.sql"] = sql_mod

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import pgadmin  # noqa: E402
import jobs as jobs_mod  # noqa: E402
from store import JobStore  # noqa: E402

TOC_TEXT = """\
; Archive created at 2026-11-13 10:00:00 UTC
;
; Selected TOC Entries:
;
3103; 0 0 ENCODING - ENCODING
3104; 0 0 STDSTRINGS - STDSTRINGS
8; 2615 2200 SCHEMA - public postgres
3210; 0 0 EXTENSION - plpgsql
220; 1259 16391 TABLE public customers postgres
3300; 0 16391 TABLE DATA public customers postgres
3200; 2606 16410 CONSTRAINT public customers customers_pkey postgres
3100; 1259 16415 INDEX public idx_customers_email postgres
"""


# ---- fakes ----

class FakeProc:
    """mode: 'ok' exits 0 immediately, 'fail' exits 1, 'block' runs until stopped."""

    mode = "ok"
    instances = []

    def __init__(self, cmd, **kw):
        self.cmd = cmd
        self.stderr = io.StringIO(
            "pg_restore: error: unexpected data in file\n" if FakeProc.mode == "fail" else ""
        )
        if FakeProc.mode == "ok":
            self.returncode = 0
        elif FakeProc.mode == "fail":
            self.returncode = 1
        else:
            self.returncode = None
        FakeProc.instances.append(self)

    def poll(self):
        return self.returncode

    def terminate(self):
        if self.returncode is None:
            self.returncode = -15

    def kill(self):
        if self.returncode is None:
            self.returncode = -9

    def wait(self, timeout=None):
        return self.returncode


class FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, q, params=None):
        pass

    def fetchall(self):
        return [("public", "customers"), ("public", "orders")]

    def fetchone(self):
        return (123,)


class FakeConn:
    def cursor(self):
        return FakeCursor()

    def close(self):
        pass


class CleanupLog:
    """Records cleanup calls plus the job state observed at drop time."""

    def __init__(self, store):
        self.store = store
        self.calls = []
        self.lock = threading.Lock()

    def record(self, kind, name):
        with self.lock:
            state = None
            for rec in self.store.list():
                if rec["temp_db"] == name:
                    state = rec["state"]
            self.calls.append((kind, name, state))

    def states_at_drop(self):
        return [c[2] for c in self.calls if c[0] == "drop"]


class JobsTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.store = JobStore(os.path.join(self.tmp, "jobs.db"))
        self.mgr = jobs_mod.JobManager(self.store, slots=self.SLOTS, job_timeout_s=60)
        self.cleanup = CleanupLog(self.store)

        self._orig = {
            "create_database": pgadmin.create_database,
            "terminate_backends": pgadmin.terminate_backends,
            "wait_no_sessions": pgadmin.wait_no_sessions,
            "drop_database": pgadmin.drop_database,
            "connect": pgadmin.connect,
            "subprocess": jobs_mod.subprocess,
        }
        pgadmin.create_database = lambda name: self.cleanup.record("create", name)
        pgadmin.terminate_backends = lambda name: self.cleanup.record("terminate", name)
        pgadmin.wait_no_sessions = lambda name, timeout_s=10: True
        pgadmin.drop_database = lambda name: self.cleanup.record("drop", name)
        pgadmin.connect = lambda dbname=None: FakeConn()

        fake_subprocess = types.SimpleNamespace(
            run=lambda *a, **k: types.SimpleNamespace(
                returncode=0, stdout=TOC_TEXT, stderr=""
            ),
            Popen=FakeProc,
            DEVNULL=real_subprocess.DEVNULL,
            PIPE=real_subprocess.PIPE,
            TimeoutExpired=real_subprocess.TimeoutExpired,
        )
        jobs_mod.subprocess = fake_subprocess
        FakeProc.instances = []

    def tearDown(self):
        for k, v in self._orig.items():
            if k == "subprocess":
                setattr(jobs_mod, k, v)
            else:
                setattr(pgadmin, k, v)

    SLOTS = 2

    # ---- helpers ----

    def make_dump(self, name="backup.dump"):
        path = os.path.join(self.tmp, name)
        with open(path, "wb") as f:
            f.write(b"PGDMP fake")
        return path

    def wait_final(self, job_id, timeout=10):
        deadline = time.time() + timeout
        while time.time() < deadline:
            rec = self.store.get(job_id)
            if rec and rec["state"] in ("succeeded", "failed", "cancelled", "interrupted"):
                return rec
            time.sleep(0.05)
        self.fail(f"job {job_id} did not reach a final state in time")

    def submit(self, name="backup.dump"):
        path = self.make_dump(name)
        job = self.mgr.submit(name, path, os.path.getsize(path), owned_file=True)
        self.assertIsNotNone(job, "submit unexpectedly rejected")
        return job


class TestSuccessPath(JobsTestCase):
    def test_success(self):
        FakeProc.mode = "ok"
        job = self.submit()
        rec = self.wait_final(job.id)

        self.assertEqual(rec["state"], "succeeded")
        self.assertEqual(len(rec["row_counts"]), 2)
        self.assertEqual(rec["row_counts"][0]["rows"], 123)
        self.assertEqual(rec["skipped"], {"EXTENSION": 1})
        # temp db cleaned, with cleanup strictly before the final state
        self.assertEqual(self.cleanup.states_at_drop(), ["running"])
        kinds = [c[0] for c in self.cleanup.calls]
        self.assertEqual(kinds, ["create", "terminate", "drop"])
        # slot released, upload file removed
        self.assertEqual(self.mgr.slots_in_use(), 0)
        self.assertFalse(os.path.exists(job.dump_path))


class TestFailurePath(JobsTestCase):
    def test_restore_error(self):
        FakeProc.mode = "fail"
        job = self.submit()
        rec = self.wait_final(job.id)

        self.assertEqual(rec["state"], "failed")
        self.assertTrue(any("unexpected data" in e for e in rec["errors"]))
        self.assertEqual(rec["row_counts"], [])
        self.assertEqual(self.cleanup.states_at_drop(), ["running"])
        self.assertEqual(self.mgr.slots_in_use(), 0)


class TestCancel(JobsTestCase):
    def test_cancel_during_restore_no_late_report(self):
        FakeProc.mode = "block"
        job = self.submit()
        # wait until pg_restore is actually running
        deadline = time.time() + 5
        while not FakeProc.instances and time.time() < deadline:
            time.sleep(0.05)
        self.assertTrue(FakeProc.instances)

        self.assertEqual(self.mgr.cancel(job.id), "accepted")
        rec = self.wait_final(job.id)

        self.assertEqual(rec["state"], "cancelled")
        # no late report: results must never be published after cancellation
        self.assertEqual(rec["row_counts"], [])
        self.assertEqual(rec["errors"], [])
        self.assertEqual(rec["skipped"], {})
        # process was stopped, sessions terminated, temp db dropped first
        self.assertEqual(FakeProc.instances[0].returncode, -15)
        self.assertEqual(self.cleanup.states_at_drop(), ["cancelling"])
        self.assertEqual(self.mgr.slots_in_use(), 0)

    def test_cancel_after_finish_rejected(self):
        FakeProc.mode = "ok"
        job = self.submit()
        self.wait_final(job.id)
        self.assertEqual(self.mgr.cancel(job.id), "already_final")

    def test_cancel_unknown_job(self):
        self.assertEqual(self.mgr.cancel("0" * 32), "not_found")


class TestSlots(JobsTestCase):
    SLOTS = 1

    def test_slot_limit_and_release_after_cleanup(self):
        FakeProc.mode = "block"
        job1 = self.submit("a.dump")
        # second submission must be rejected while the slot is held
        self.assertIsNone(self.mgr.submit("b.dump", self.make_dump("b.dump"), 10, True))
        self.assertEqual(self.mgr.cancel(job1.id), "accepted")
        rec = self.wait_final(job1.id)
        self.assertEqual(rec["state"], "cancelled")
        # slot released only after cleanup finished
        self.assertEqual(self.cleanup.states_at_drop(), ["cancelling"])
        self.assertEqual(self.mgr.slots_in_use(), 0)
        job2 = self.submit("c.dump")
        self.assertEqual(self.mgr.cancel(job2.id), "accepted")
        self.wait_final(job2.id)


class TestStore(JobsTestCase):
    def test_mark_interrupted_only_touches_active_jobs(self):
        path = self.make_dump()
        rec = {
            "id": "a" * 32, "filename": "x.dump", "size": 1, "state": "running",
            "temp_db": "rc_0123456789ab", "created_at": time.time(),
            "started_at": time.time(), "finished_at": None, "message": "",
            "errors": [], "row_counts": [], "skipped": {},
        }
        self.store.create(rec)
        done = dict(rec, id="b" * 32, state="succeeded", temp_db=None)
        self.store.create(done)
        os.unlink(path)

        self.assertEqual(self.store.mark_interrupted(), 1)
        self.assertEqual(self.store.get("a" * 32)["state"], "interrupted")
        self.assertEqual(self.store.get("b" * 32)["state"], "succeeded")


if __name__ == "__main__":
    unittest.main(verbosity=2)
