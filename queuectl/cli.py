#!/usr/bin/env python3
"""QueueCTL - CLI-based background job queue system."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from . import __version__
from .config import get_config, load_config, reset_config, set_config
from .db import JobStore
from .models import Job, JobState
from .worker import start_workers, stop_workers


# Force UTF-8 stdout on Windows to avoid cp1252 encoding errors
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')


def get_store() -> JobStore:
    return JobStore()


# ----------------------------------------------
#  Enqueue
# ----------------------------------------------

def cmd_enqueue(args):
    store = get_store()
    try:
        data = json.loads(args.job_json)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON - {e}", file=sys.stderr)
        sys.exit(1)

    if "command" not in data:
        print("Error: Job JSON must include a 'command' field.", file=sys.stderr)
        sys.exit(1)

    # Apply global config defaults if not specified
    config = load_config()
    if "max_retries" not in data:
        data["max_retries"] = config["max-retries"]

    job = Job.from_json(data)
    store.add_job(job)
    print(f"Job enqueued: {job.id}")
    print(json.dumps(job.to_dict(), indent=2))


# ----------------------------------------------
#  Worker
# ----------------------------------------------

def cmd_worker_start(args):
    store = get_store()
    start_workers(store, count=args.count)


def cmd_worker_stop(args):
    store = get_store()
    stop_workers(store)


# ----------------------------------------------
#  Status
# ----------------------------------------------

def cmd_status(args):
    store = get_store()
    counts = store.count_by_state()
    workers = store.list_workers()

    total = sum(counts.values())
    print("+----------------------------------+")
    print("|       QueueCTL Status            |")
    print("+----------------------------------+")
    for state in ["pending", "processing", "completed", "failed", "dead"]:
        c = counts.get(state, 0)
        label = state.capitalize().ljust(12)
        print(f"|  {label} : {c:<18} |")
    print(f"+----------------------------------+")
    print(f"|  Total Jobs  : {total:<18}|")
    print(f"|  Workers     : {len(workers):<18}|")
    print("+----------------------------------+")

    if workers:
        print("\nActive Workers:")
        for w in workers:
            print(f"  * {w['id']} (pid={w['pid']}, since {w['started_at'][:19]})")


# ----------------------------------------------
#  List
# ----------------------------------------------

def cmd_list(args):
    store = get_store()
    jobs = store.list_jobs(state=args.state)

    if not jobs:
        state_msg = f" in state '{args.state}'" if args.state else ""
        print(f"No jobs found{state_msg}.")
        return

    fmt = "{:<12} {:<30} {:<12} {:<8} {:<20}"
    print(fmt.format("ID", "COMMAND", "STATE", "TRIES", "UPDATED"))
    print("-" * 85)
    for job in jobs:
        cmd_display = job.command[:28] + ".." if len(job.command) > 30 else job.command
        print(fmt.format(
            job.id,
            cmd_display,
            job.state.value,
            f"{job.attempts}/{job.max_retries}",
            job.updated_at[:19],
        ))


# ----------------------------------------------
#  DLQ
# ----------------------------------------------

def cmd_dlq_list(args):
    store = get_store()
    dead_jobs = store.list_dlq()

    if not dead_jobs:
        print("Dead Letter Queue is empty.")
        return

    fmt = "{:<12} {:<30} {:<8} {:<30} {:<20}"
    print(fmt.format("ID", "COMMAND", "TRIES", "ERROR", "UPDATED"))
    print("-" * 102)
    for job in dead_jobs:
        cmd_display = job.command[:28] + ".." if len(job.command) > 30 else job.command
        err_display = (job.error or "")[:28] + ".." if job.error and len(job.error) > 30 else (job.error or "")
        print(fmt.format(
            job.id,
            cmd_display,
            f"{job.attempts}/{job.max_retries}",
            err_display,
            job.updated_at[:19],
        ))


def cmd_dlq_retry(args):
    store = get_store()
    if args.job_id == "all":
        count = store.retry_all_dlq()
        print(f"Re-queued {count} job(s) from DLQ.")
    else:
        job = store.retry_dlq_job(args.job_id)
        if job:
            print(f"Job '{job.id}' moved from DLQ back to pending.")
        else:
            print(f"Error: Job '{args.job_id}' not found in DLQ.", file=sys.stderr)
            sys.exit(1)


# ----------------------------------------------
#  Config
# ----------------------------------------------

def cmd_config_get(args):
    try:
        val = get_config(args.key)
        print(f"{args.key} = {val}")
    except KeyError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_config_set(args):
    try:
        val = set_config(args.key, args.value)
        print(f"{args.key} = {val}")
    except KeyError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_config_list(args):
    config = load_config()
    print("Current Configuration:")
    for k, v in sorted(config.items()):
        print(f"  {k} = {v}")


def cmd_config_reset(args):
    reset_config()
    print("Configuration reset to defaults.")


# ----------------------------------------------
#  Parser
# ----------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="queuectl",
        description="QueueCTL - A CLI-based background job queue system",
    )
    parser.add_argument("--version", action="version", version=f"queuectl {__version__}")
    sub = parser.add_subparsers(dest="command", help="Available commands")

    # --- enqueue ---
    p_enqueue = sub.add_parser("enqueue", help="Add a new job to the queue")
    p_enqueue.add_argument(
        "job_json",
        help='Job definition as JSON, e.g. \'{"id":"job1","command":"echo hello"}\'',
    )
    p_enqueue.set_defaults(func=cmd_enqueue)

    # --- worker ---
    p_worker = sub.add_parser("worker", help="Manage workers")
    worker_sub = p_worker.add_subparsers(dest="worker_cmd")

    p_wstart = worker_sub.add_parser("start", help="Start worker processes")
    p_wstart.add_argument("--count", type=int, default=1, help="Number of workers (default: 1)")
    p_wstart.set_defaults(func=cmd_worker_start)

    p_wstop = worker_sub.add_parser("stop", help="Stop all running workers gracefully")
    p_wstop.set_defaults(func=cmd_worker_stop)

    # --- status ---
    p_status = sub.add_parser("status", help="Show queue status summary")
    p_status.set_defaults(func=cmd_status)

    # --- list ---
    p_list = sub.add_parser("list", help="List jobs")
    p_list.add_argument("--state", choices=["pending", "processing", "completed", "failed", "dead"],
                        help="Filter by state")
    p_list.set_defaults(func=cmd_list)

    # --- dlq ---
    p_dlq = sub.add_parser("dlq", help="Dead Letter Queue operations")
    dlq_sub = p_dlq.add_subparsers(dest="dlq_cmd")

    p_dlq_list = dlq_sub.add_parser("list", help="List all DLQ jobs")
    p_dlq_list.set_defaults(func=cmd_dlq_list)

    p_dlq_retry = dlq_sub.add_parser("retry", help="Retry a DLQ job (or 'all')")
    p_dlq_retry.add_argument("job_id", help="Job ID to retry, or 'all'")
    p_dlq_retry.set_defaults(func=cmd_dlq_retry)

    # --- config ---
    p_config = sub.add_parser("config", help="Manage configuration")
    config_sub = p_config.add_subparsers(dest="config_cmd")

    p_cget = config_sub.add_parser("get", help="Get a config value")
    p_cget.add_argument("key", help="Config key")
    p_cget.set_defaults(func=cmd_config_get)

    p_cset = config_sub.add_parser("set", help="Set a config value")
    p_cset.add_argument("key", help="Config key")
    p_cset.add_argument("value", help="Config value")
    p_cset.set_defaults(func=cmd_config_set)

    p_clist = config_sub.add_parser("list", help="Show all config values")
    p_clist.set_defaults(func=cmd_config_list)

    p_creset = config_sub.add_parser("reset", help="Reset config to defaults")
    p_creset.set_defaults(func=cmd_config_reset)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.parse_args([args.command, "--help"])


if __name__ == "__main__":
    main()