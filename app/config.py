"""Runtime configuration (environment variables, all with sane defaults)."""
from __future__ import annotations

import os

PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "postgres")
PGMAINTDB = os.getenv("PGMAINTDB", "postgres")  # maintenance database for admin tasks

SLOTS = int(os.getenv("SLOTS", "2"))  # max concurrent trial-restore batches
DATA_DIR = os.getenv("DATA_DIR", "./data")
SAMPLES_DIR = os.getenv("SAMPLES_DIR", "./samples")
MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_MB", "1024")) * 1024 * 1024
JOB_TIMEOUT_S = int(os.getenv("JOB_TIMEOUT_S", "1800"))
