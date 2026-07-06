#!/usr/bin/env python3
"""Tests for QueueCTL — validates all core flows."""

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from queuectl.db import JobStore
from queuectl.models import Job, JobState
from queuectl.config import load_config, set_config, reset_config, DEFAULTS


class TestJobModel(unittest.TestCase):
    def test_create_from_json(self):
        data = {"id": "t1", "command": "echo hello"}
        job = Job.from_json(data)
        self.assertEqual(job.id, "t1")
        self.assertEqual(job.command, "echo hello")
        self.assertEqual(job.state, JobState.PENDING)
        self.assertEqual(job.attempts, 0)

    def test_auto_generate_id(self):
        job = Job.from_json({"command": "echo hi"})
        self.assertTrue(len(job.id) > 0)

    def test_to_dict(self):
        job = Job.from_json({"id": "t2", "command": "ls"})
        d = job.to_dict()
        self.assertEqual(d["state"], "pending")
        self.assertIn("created_at", d)


class TestJobStore(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        self.store = JobStore(self.db_path)

    def test_add_and_get(self):
        job = Job.from_json({"id": "j1", "command": "echo test"})
        self.store.add_job(job)
        fetched = self.store.get_job("j1")
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.command, "echo test")

    def test_list_by_state(self):
        self.store.add_job(Job.from_json({"id": "a", "command": "echo a"}))
        self.store.add_job(Job.from_json({"id": "b", "command": "echo b"}))
        jobs = self.store.list_jobs(state="pending")
        self.assertEqual(len(jobs), 2)
        jobs_done = self.store.list_jobs(state="completed")
        self.assertEqual(len(jobs_done), 0)

    def test_claim_prevents_duplicates(self):
        self.store.add_job(Job.from_json({"id": "c1", "command": "sleep 1"}))
        j1 = self.store.claim_next_job("w1")
        j2 = self.store.claim_next_job("w2")
        self.assertIsNotNone(j1)
        self.assertIsNone(j2)  # Already claimed

    def test_persistence_across_reconnect(self):
        self.store.add_job(Job.from_json({"id": "p1", "command": "echo persist"}))
        # Create a new store instance pointing to same DB
        store2 = JobStore(self.db_path)
        job = store2.get_job("p1")
        self.assertIsNotNone(job)
        self.assertEqual(job.command, "echo persist")

    def test_dlq_retry(self):
        job = Job.from_json({"id": "d1", "command": "false"})
        job.state = JobState.DEAD
        job.attempts = 3
        self.store.add_job(job)
        retried = self.store.retry_dlq_job("d1")
        self.assertIsNotNone(retried)
        self.assertEqual(retried.state, JobState.PENDING)
        self.assertEqual(retried.attempts, 0)

    def test_count_by_state(self):
        self.store.add_job(Job.from_json({"id": "s1", "command": "echo 1"}))
        self.store.add_job(Job.from_json({"id": "s2", "command": "echo 2"}))
        j = Job.from_json({"id": "s3", "command": "echo 3"})
        j.state = JobState.COMPLETED
        self.store.add_job(j)
        counts = self.store.count_by_state()
        self.assertEqual(counts.get("pending", 0), 2)
        self.assertEqual(counts.get("completed", 0), 1)


class TestConfig(unittest.TestCase):
    def test_defaults(self):
        config = load_config()
        self.assertEqual(config["max-retries"], DEFAULTS["max-retries"])

    def test_set_and_get(self):
        set_config("max-retries", "5")
        config = load_config()
        self.assertEqual(config["max-retries"], 5)
        # Reset
        reset_config()


class TestWorkerExecution(unittest.TestCase):
    """Integration tests that exercise actual job execution."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        self.store = JobStore(self.db_path)

    def test_successful_job(self):
        """Scenario 1: Basic job completes successfully."""
        from queuectl.worker import Worker
        self.store.add_job(Job.from_json({"id": "ok1", "command": "echo hello"}))
        w = Worker(self.store, "test-worker")
        job = self.store.claim_next_job(w.worker_id)
        self.assertIsNotNone(job)
        from queuectl.config import load_config
        w._process_job(job, load_config())
        result = self.store.get_job("ok1")
        self.assertEqual(result.state, JobState.COMPLETED)
        self.assertEqual(result.output, "hello")

    def test_failed_job_retries_to_dlq(self):
        """Scenario 2: Failed job retries with backoff and moves to DLQ."""
        from queuectl.worker import Worker
        job = Job.from_json({"id": "fail1", "command": "nonexistent_cmd_xyz", "max_retries": 2})
        self.store.add_job(job)

        w = Worker(self.store, "test-worker")
        from queuectl.config import load_config
        config = load_config()

        # Attempt 1 — should fail and re-queue
        j = self.store.claim_next_job(w.worker_id)
        w._process_job(j, config)
        j = self.store.get_job("fail1")
        self.assertEqual(j.state, JobState.PENDING)
        self.assertEqual(j.attempts, 1)

        # Attempt 2 — should move to DLQ
        j = self.store.claim_next_job(w.worker_id)
        if j is None:
            # run_at may be in the future; manually set it to now for testing
            j = self.store.get_job("fail1")
            j.run_at = None
            self.store.update_job(j)
            j = self.store.claim_next_job(w.worker_id)
        w._process_job(j, config)
        j = self.store.get_job("fail1")
        self.assertEqual(j.state, JobState.DEAD)

    def test_invalid_command_fails_gracefully(self):
        """Scenario 4: Invalid commands fail gracefully."""
        from queuectl.worker import Worker
        self.store.add_job(Job.from_json({
            "id": "inv1", "command": "/no/such/binary --flag", "max_retries": 1
        }))
        w = Worker(self.store, "test-worker")
        from queuectl.config import load_config
        j = self.store.claim_next_job(w.worker_id)
        w._process_job(j, load_config())
        result = self.store.get_job("inv1")
        self.assertIn(result.state, [JobState.PENDING, JobState.DEAD])
        self.assertIsNotNone(result.error)


class TestCLI(unittest.TestCase):
    """End-to-end CLI smoke tests."""

    def _run(self, *args):
        # Use the installed queuectl entry point if available, else fall back to -m
        import shutil
        queuectl_bin = shutil.which("queuectl")
        if queuectl_bin:
            cmd = [sys.executable, queuectl_bin] + list(args)
        else:
            cmd = [sys.executable, "-m", "queuectl.cli"] + list(args)

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        return result

    def test_enqueue_and_list(self):
        import uuid
        job_id = "cli-" + uuid.uuid4().hex[:6]
        r = self._run("enqueue", f'{{"id":"{job_id}","command":"echo cli_test"}}')
        self.assertEqual(r.returncode, 0, msg=f"stderr: {r.stderr}")
        self.assertIn(job_id, r.stdout)

        r = self._run("list")
        self.assertEqual(r.returncode, 0, msg=f"stderr: {r.stderr}")
        self.assertIn(job_id, r.stdout)

    def test_status(self):
        r = self._run("status")
        self.assertEqual(r.returncode, 0, msg=f"stderr: {r.stderr}")
        self.assertIn("QueueCTL Status", r.stdout + r.stderr)

    def test_config_operations(self):
        r = self._run("config", "list")
        self.assertEqual(r.returncode, 0)
        self.assertIn("max-retries", r.stdout)

    def test_dlq_list(self):
        r = self._run("dlq", "list")
        self.assertEqual(r.returncode, 0)

    def test_help(self):
        r = self._run("--help")
        self.assertEqual(r.returncode, 0)
        self.assertIn("queuectl", r.stdout.lower())


if __name__ == "__main__":
    unittest.main(verbosity=2)