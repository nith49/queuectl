"""Worker processes that execute jobs from the queue."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from multiprocessing import Process
from typing import Optional

from .config import load_config
from .db import JobStore
from .models import Job, JobState


class Worker:
    """A single worker that polls for and executes jobs."""

    def __init__(self, store: JobStore, worker_id: Optional[str] = None):
        self.store = store
        self.worker_id = worker_id or f"w-{uuid.uuid4().hex[:6]}"
        self._running = True

    def _handle_signal(self, signum, frame):
        print(f"\n[{self.worker_id}] Received shutdown signal, finishing current job...")
        self._running = False

    def _execute_command(self, job: Job, timeout: int) -> tuple[int, str, str]:
        """Execute a shell command and return (exit_code, stdout, stderr)."""
        try:
            result = subprocess.run(
                job.command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout if timeout > 0 else None,
            )
            return result.returncode, result.stdout, result.stderr
        except subprocess.TimeoutExpired:
            return -1, "", f"Job timed out after {timeout}s"
        except Exception as e:
            return -1, "", str(e)

    def _process_job(self, job: Job, config: dict):
        """Execute a single job, handling success/failure/retry/DLQ."""
        max_retries = job.max_retries or config["max-retries"]
        backoff_base = config["backoff-base"]
        timeout = job.timeout or config["job-timeout"]

        job.attempts += 1
        self.store.update_job(job)

        print(f"[{self.worker_id}] Executing job '{job.id}': {job.command} (attempt {job.attempts}/{max_retries})")

        exit_code, stdout, stderr = self._execute_command(job, timeout)

        if exit_code == 0:
            job.state = JobState.COMPLETED
            job.output = stdout.strip() if stdout else None
            job.error = None
            self.store.update_job(job)
            print(f"[{self.worker_id}] Job '{job.id}' completed successfully.")
        else:
            error_msg = stderr.strip() if stderr else f"Exit code {exit_code}"
            job.error = error_msg

            if job.attempts >= max_retries:
                job.state = JobState.DEAD
                self.store.update_job(job)
                print(f"[{self.worker_id}] Job '{job.id}' moved to DLQ after {job.attempts} attempts: {error_msg}")
            else:
                delay = backoff_base ** job.attempts
                job.state = JobState.FAILED
                # Schedule retry using run_at
                retry_time = datetime.now(timezone.utc).timestamp() + delay
                retry_dt = datetime.fromtimestamp(retry_time, tz=timezone.utc)
                job.run_at = retry_dt.isoformat()
                job.state = JobState.PENDING
                job.worker_id = None
                self.store.update_job(job)
                print(f"[{self.worker_id}] Job '{job.id}' failed, retrying in {delay}s: {error_msg}")

    def run(self):
        """Main worker loop."""
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)

        self.store.register_worker(self.worker_id, os.getpid())
        config = load_config()
        poll_interval = config.get("worker-poll-interval", 1)

        print(f"[{self.worker_id}] Worker started (pid={os.getpid()})")

        try:
            while self._running:
                job = self.store.claim_next_job(self.worker_id)
                if job:
                    self._process_job(job, config)
                else:
                    time.sleep(poll_interval)
        finally:
            self.store.unregister_worker(self.worker_id)
            print(f"[{self.worker_id}] Worker stopped.")


def _run_single_worker(db_path: str, worker_id: str):
    """Entry point for a worker subprocess."""
    store = JobStore(db_path)
    w = Worker(store, worker_id)
    w.run()


def start_workers(store: JobStore, count: int = 1):
    """Start multiple worker processes."""
    # First release any stale processing jobs
    store.release_stale_jobs()

    processes = []
    for i in range(count):
        wid = f"w-{uuid.uuid4().hex[:6]}"
        p = Process(target=_run_single_worker, args=(store.db_path, wid), daemon=False)
        p.start()
        processes.append(p)
        print(f"Started worker {wid} (pid={p.pid})")

    print(f"\n{count} worker(s) running. Press Ctrl+C to stop all.")

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\nStopping all workers...")
        for p in processes:
            if p.is_alive():
                os.kill(p.pid, signal.SIGTERM)
        for p in processes:
            p.join(timeout=10)
        print("All workers stopped.")


def stop_workers(store: JobStore):
    """Send SIGTERM to all registered workers."""
    workers = store.list_workers()
    if not workers:
        print("No active workers found.")
        return

    stopped = 0
    for w in workers:
        pid = w["pid"]
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"Sent stop signal to worker {w['id']} (pid={pid})")
            stopped += 1
        except ProcessLookupError:
            store.unregister_worker(w["id"])
            print(f"Worker {w['id']} (pid={pid}) already dead, cleaned up.")

    # Release any jobs held by stopped workers
    store.release_stale_jobs()
    print(f"Stopped {stopped} worker(s).")
