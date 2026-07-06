"""Job model definition."""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class JobState(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD = "dead"


@dataclass
class Job:
    id: str
    command: str
    state: JobState = JobState.PENDING
    attempts: int = 0
    max_retries: int = 3
    priority: int = 0
    timeout: Optional[int] = None
    run_at: Optional[str] = None
    output: Optional[str] = None
    error: Optional[str] = None
    worker_id: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @classmethod
    def from_json(cls, data: str | dict) -> Job:
        """Create a Job from a JSON string or dict."""
        if isinstance(data, str):
            data = json.loads(data)
        # Generate an ID if not provided
        if "id" not in data or not data["id"]:
            data["id"] = str(uuid.uuid4())[:8]
        # Map state string to enum
        if "state" in data:
            data["state"] = JobState(data["state"])
        # Only keep known fields
        known = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in known}
        return cls(**filtered)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["state"] = self.state.value
        return d

    def touch(self):
        self.updated_at = datetime.now(timezone.utc).isoformat()
