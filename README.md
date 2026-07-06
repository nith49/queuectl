# QueueCTL

A CLI-based background job queue system built in Python. It manages background jobs with worker processes, handles retries using exponential backoff, and maintains a Dead Letter Queue (DLQ) for permanently failed jobs.

## Features

- **Job Enqueueing** — Submit shell commands as background jobs via CLI
- **Multi-Worker Processing** — Run multiple workers in parallel with safe job claiming (no duplicates)
- **Exponential Backoff Retries** — Failed jobs are retried with configurable `delay = base ^ attempts` seconds
- **Dead Letter Queue** — Jobs that exhaust retries are moved to DLQ for inspection and manual retry
- **Persistent Storage** — SQLite with WAL mode ensures data survives restarts and handles concurrent access
- **Graceful Shutdown** — Workers finish their current job before exiting on SIGTERM/SIGINT
- **Configurable** — Retry count, backoff base, poll interval, and job timeout are all configurable via CLI

## Setup Instructions

### Prerequisites

- Python 3.9+
- No external dependencies — uses only the Python standard library

### Install

```bash
# Clone the repo
git clone https://github.com/nith49/queuectl.git
cd queuectl

# Install in editable mode (creates the `queuectl` command)
pip install -e .

# Or run directly without installing
python -m queuectl.cli --help
```

## Usage Examples

### Enqueue a Job

```bash
# Simple command
queuectl enqueue '{"id":"job1","command":"echo Hello World"}'

# With custom retry limit
queuectl enqueue '{"id":"job2","command":"sleep 2","max_retries":5}'

# Auto-generated ID
queuectl enqueue '{"command":"date"}'

# With priority (higher = processed first)
queuectl enqueue '{"id":"urgent","command":"echo urgent","priority":10}'

# With timeout (seconds)
queuectl enqueue '{"id":"slow","command":"sleep 60","timeout":5}'
```

**Output:**
```
Job enqueued: job1
{
  "id": "job1",
  "command": "echo Hello World",
  "state": "pending",
  "attempts": 0,
  "max_retries": 3,
  ...
}
```

### Start Workers

```bash
# Start 1 worker (foreground)
queuectl worker start

# Start 3 workers
queuectl worker start --count 3
```

**Output:**
```
Started worker w-a1b2c3 (pid=12345)
Started worker w-d4e5f6 (pid=12346)
Started worker w-g7h8i9 (pid=12347)

3 worker(s) running. Press Ctrl+C to stop all.
[w-a1b2c3] Worker started (pid=12345)
[w-a1b2c3] Executing job 'job1': echo Hello World (attempt 1/3)
[w-a1b2c3] Job 'job1' completed successfully.
```

### Stop Workers

```bash
queuectl worker stop
```

### Check Status

```bash
queuectl status
```

**Output:**
```
╔══════════════════════════════════╗
║       QueueCTL Status            ║
╠══════════════════════════════════╣
║  Pending      : 2                ║
║  Processing   : 1                ║
║  Completed    : 5                ║
║  Failed       : 0                ║
║  Dead         : 1                ║
╠══════════════════════════════════╣
║  Total Jobs   : 9                ║
║  Workers      : 3                ║
╚══════════════════════════════════╝
```

### List Jobs

```bash
# All jobs
queuectl list

# Filter by state
queuectl list --state pending
queuectl list --state dead
```

### DLQ Operations

```bash
# View dead-letter jobs
queuectl dlq list

# Retry a specific DLQ job
queuectl dlq retry job2

# Retry all DLQ jobs
queuectl dlq retry all
```

### Configuration

```bash
# View all config
queuectl config list

# Get a specific value
queuectl config get max-retries

# Set values
queuectl config set max-retries 5
queuectl config set backoff-base 3
queuectl config set job-timeout 30

# Reset to defaults
queuectl config reset
```

**Available config keys:**

| Key | Default | Description |
|-----|---------|-------------|
| `max-retries` | 3 | Max retry attempts before DLQ |
| `backoff-base` | 2 | Base for exponential backoff (`base ^ attempts` seconds) |
| `worker-poll-interval` | 1 | Seconds between polling for new jobs |
| `job-timeout` | 0 | Job execution timeout in seconds (0 = no timeout) |

## Architecture Overview

### Job Lifecycle

```
                    ┌──────────┐
    enqueue ──────► │ PENDING  │
                    └────┬─────┘
                         │  claimed by worker
                    ┌────▼─────┐
                    │PROCESSING│
                    └────┬─────┘
                   ┌─────┴──────┐
              exit=0        exit≠0
                   │            │
             ┌─────▼────┐  ┌───▼───┐
             │COMPLETED  │  │FAILED │
             └──────────┘  └───┬───┘
                               │  attempts < max_retries
                               │  (backoff delay applied)
                          ┌────▼─────┐
                          │ PENDING  │ (retry)
                          └──────────┘
                               │  attempts >= max_retries
                          ┌────▼─────┐
                          │  DEAD    │ (DLQ)
                          └──────────┘
```

### Data Persistence

- **SQLite** with WAL (Write-Ahead Logging) mode for concurrent read/write safety
- Database stored at `~/.queuectl/queuectl.db`
- Configuration stored at `~/.queuectl/config.json`
- `BEGIN IMMEDIATE` transactions prevent race conditions when multiple workers claim jobs

### Worker Logic

1. Workers poll the database for pending jobs at a configurable interval
2. Job claiming is atomic — `claim_next_job()` uses a single transaction to SELECT + UPDATE, preventing duplicate processing
3. On failure, the worker calculates backoff delay (`base ^ attempts`) and sets a `run_at` timestamp — the job won't be picked up until that time
4. Workers register their PID in the database; on `worker stop`, SIGTERM is sent to each registered PID
5. Graceful shutdown: workers catch SIGTERM/SIGINT and finish the current job before exiting

### Project Structure

```
queuectl/
├── queuectl/
│   ├── __init__.py     # Version
│   ├── cli.py          # Argparse-based CLI interface
│   ├── db.py           # SQLite storage layer (JobStore)
│   ├── models.py       # Job dataclass and JobState enum
│   ├── worker.py       # Worker process logic
│   └── config.py       # Configuration management
├── tests/
│   └── test_queuectl.py
├── setup.py
└── README.md
```

## Assumptions & Trade-offs

| Decision | Rationale |
|----------|-----------|
| **SQLite over JSON files** | Atomic transactions, indexing, and WAL mode handle concurrency far better than file-based locking |
| **Multiprocessing over threading** | True parallelism; each worker is a separate OS process that can fully utilise a CPU core |
| **Shell execution via `subprocess`** | Matches the spec (`echo hello`, `sleep 2`); commands run in a shell just like a user would type them |
| **Backoff via `run_at` field** | Rather than sleeping in the worker (blocking it), failed jobs are re-queued with a future timestamp so the worker can process other jobs meanwhile |
| **No external dependencies** | Entire project uses only the Python stdlib — no pip installs needed beyond the package itself |
| **Worker registration in DB** | Allows `worker stop` from any terminal session, and stale detection on restart |

## Testing Instructions

### Run the Full Test Suite

```bash
cd queuectl
python -m pytest tests/ -v

# Or without pytest:
python -m unittest tests.test_queuectl -v
```

### What the Tests Cover

| Scenario | Test |
|----------|------|
| Basic job completes successfully | `test_successful_job` |
| Failed job retries with backoff → DLQ | `test_failed_job_retries_to_dlq` |
| Multiple workers can't claim same job | `test_claim_prevents_duplicates` |
| Invalid commands fail gracefully | `test_invalid_command_fails_gracefully` |
| Job data survives restart (persistence) | `test_persistence_across_reconnect` |
| DLQ retry resets job to pending | `test_dlq_retry` |
| CLI end-to-end smoke tests | `TestCLI` class |

### Manual Quick Test

```bash
# 1. Enqueue jobs
queuectl enqueue '{"id":"ok","command":"echo success"}'
queuectl enqueue '{"id":"fail","command":"nonexistent_command","max_retries":2}'

# 2. Start a worker
queuectl worker start

# 3. Check results (after worker processes both)
queuectl status
queuectl list --state completed
queuectl dlq list

# 4. Retry DLQ job
queuectl dlq retry fail
```

## Bonus Features Implemented

- **Job timeout handling** — `timeout` field per job + global `job-timeout` config
- **Job priority** — `priority` field; higher-priority jobs are processed first
- **Scheduled/delayed jobs** — `run_at` field for future execution (also used internally for backoff)
- **Job output logging** — stdout/stderr captured in `output` and `error` fields

## Video Demo

https://github.com/user-attachments/assets/fea4236e-a15d-41ea-971f-61afeaf348eb



