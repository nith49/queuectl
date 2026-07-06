"""SQLite-based persistent storage for jobs."""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from typing import List, Optional

from .models import Job, JobState

DEFAULT_DB_PATH = os.path.join(os.path.expanduser("~"), ".queuectl", "queuectl.db")


def _ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)


class JobStore:
    """Thread-safe SQLite job store with row-level locking via transactions."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        _ensure_dir(db_path)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")  # better concurrency
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    @contextmanager
    def _transaction(self):
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        conn = self._connect()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                priority INTEGER NOT NULL DEFAULT 0,
                timeout INTEGER,
                run_at TEXT,
                output TEXT,
                error TEXT,
                worker_id TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_jobs_state ON jobs(state)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS workers (
                id TEXT PRIMARY KEY,
                pid INTEGER NOT NULL,
                started_at TEXT NOT NULL
            )
        """)
        conn.commit()
        conn.close()

    def _row_to_job(self, row: sqlite3.Row) -> Job:
        return Job(
            id=row["id"],
            command=row["command"],
            state=JobState(row["state"]),
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            priority=row["priority"],
            timeout=row["timeout"],
            run_at=row["run_at"],
            output=row["output"],
            error=row["error"],
            worker_id=row["worker_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    # ---- Job CRUD ----

    def add_job(self, job: Job) -> Job:
        with self._transaction() as conn:
            conn.execute(
                """INSERT INTO jobs (id, command, state, attempts, max_retries,
                   priority, timeout, run_at, output, error, worker_id,
                   created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    job.id, job.command, job.state.value, job.attempts,
                    job.max_retries, job.priority, job.timeout, job.run_at,
                    job.output, job.error, job.worker_id,
                    job.created_at, job.updated_at,
                ),
            )
        return job

    def get_job(self, job_id: str) -> Optional[Job]:
        conn = self._connect()
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        conn.close()
        return self._row_to_job(row) if row else None

    def list_jobs(self, state: Optional[str] = None) -> List[Job]:
        conn = self._connect()
        if state:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE state = ? ORDER BY priority DESC, created_at ASC",
                (state,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM jobs ORDER BY priority DESC, created_at ASC"
            ).fetchall()
        conn.close()
        return [self._row_to_job(r) for r in rows]

    def update_job(self, job: Job):
        job.touch()
        with self._transaction() as conn:
            conn.execute(
                """UPDATE jobs SET command=?, state=?, attempts=?, max_retries=?,
                   priority=?, timeout=?, run_at=?, output=?, error=?,
                   worker_id=?, updated_at=?
                   WHERE id=?""",
                (
                    job.command, job.state.value, job.attempts, job.max_retries,
                    job.priority, job.timeout, job.run_at, job.output, job.error,
                    job.worker_id, job.updated_at, job.id,
                ),
            )

    def claim_next_job(self, worker_id: str) -> Optional[Job]:
        """Atomically claim the next pending job for a worker (prevents duplicates)."""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat()
        with self._transaction() as conn:
            row = conn.execute(
                """SELECT * FROM jobs
                   WHERE state = 'pending'
                     AND (run_at IS NULL OR run_at <= ?)
                   ORDER BY priority DESC, created_at ASC
                   LIMIT 1""",
                (now,),
            ).fetchone()
            if not row:
                return None
            conn.execute(
                """UPDATE jobs SET state='processing', worker_id=?, updated_at=?
                   WHERE id=? AND state='pending'""",
                (worker_id, now, row["id"]),
            )
        return self._row_to_job(row)

    def count_by_state(self) -> dict:
        conn = self._connect()
        rows = conn.execute(
            "SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state"
        ).fetchall()
        conn.close()
        return {r["state"]: r["cnt"] for r in rows}

    # ---- DLQ ----

    def list_dlq(self) -> List[Job]:
        return self.list_jobs(state="dead")

    def retry_dlq_job(self, job_id: str) -> Optional[Job]:
        job = self.get_job(job_id)
        if not job or job.state != JobState.DEAD:
            return None
        job.state = JobState.PENDING
        job.attempts = 0
        job.error = None
        job.worker_id = None
        self.update_job(job)
        return job

    def retry_all_dlq(self) -> int:
        dead_jobs = self.list_dlq()
        count = 0
        for job in dead_jobs:
            self.retry_dlq_job(job.id)
            count += 1
        return count

    # ---- Workers ----

    def register_worker(self, worker_id: str, pid: int):
        from datetime import datetime, timezone
        with self._transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO workers (id, pid, started_at) VALUES (?, ?, ?)",
                (worker_id, pid, datetime.now(timezone.utc).isoformat()),
            )

    def unregister_worker(self, worker_id: str):
        with self._transaction() as conn:
            conn.execute("DELETE FROM workers WHERE id = ?", (worker_id,))

    def list_workers(self) -> list:
        conn = self._connect()
        rows = conn.execute("SELECT * FROM workers").fetchall()
        conn.close()
        return [dict(r) for r in rows]

    def clear_workers(self):
        with self._transaction() as conn:
            conn.execute("DELETE FROM workers")

    def release_stale_jobs(self):
        """Reset processing jobs whose workers no longer exist."""
        with self._transaction() as conn:
            conn.execute(
                """UPDATE jobs SET state='pending', worker_id=NULL
                   WHERE state='processing'
                   AND worker_id NOT IN (SELECT id FROM workers)"""
            )
