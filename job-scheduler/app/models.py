from datetime import datetime, timezone
from enum import StrEnum
from hashlib import sha256
from typing import Any, Literal
from uuid import uuid4

from app.config import settings
from pydantic import BaseModel, Field, field_validator


class ScheduleType(StrEnum):
    IMMEDIATE = "IMMEDIATE"
    DATE = "DATE"
    CRON = "CRON"


class ExecutionStatus(StrEnum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    RETRYING = "RETRYING"
    FAILED = "FAILED"


class Schedule(BaseModel):
    type: ScheduleType
    expression: str | None = None

    @field_validator("expression")
    @classmethod
    def expression_required_for_scheduled_jobs(
        cls, value: str | None, info: Any
    ) -> str | None:
        schedule_type = info.data.get("type")
        if schedule_type in {ScheduleType.DATE, ScheduleType.CRON} and not value:
            raise ValueError("expression is required for DATE and CRON schedules")
        return value


class CreateJobRequest(BaseModel):
    user_id: str = Field(min_length=1)
    task_id: str = Field(min_length=1)
    schedule: Schedule
    parameters: dict[str, Any] = Field(default_factory=dict)


class JobRecord(BaseModel):
    job_id: str = Field(default_factory=lambda: str(uuid4()))
    user_id: str
    task_id: str
    schedule: Schedule
    parameters: dict[str, Any]
    created_at: int = Field(default_factory=lambda: epoch_seconds())


class ExecutionRecord(BaseModel):
    execution_id: str = Field(default_factory=lambda: str(uuid4()))
    job_id: str
    user_id: str
    scheduled_at: int
    status: ExecutionStatus = ExecutionStatus.PENDING
    attempt: int = 0
    created_at: int = Field(default_factory=lambda: epoch_seconds())
    updated_at: int = Field(default_factory=lambda: epoch_seconds())
    error: str | None = None

    @property
    def time_bucket(self) -> str:
        return str((self.scheduled_at // 3600) * 3600)

    @property
    def shard_id(self) -> int:
        return execution_shard_id(self.execution_id)

    @property
    def time_bucket_shard(self) -> str:
        return f"{self.time_bucket}#shard_{self.shard_id:02d}"

    @property
    def execution_time_key(self) -> str:
        return f"{self.scheduled_at:010d}#{self.execution_id}"

    @property
    def status_time_bucket_shard(self) -> str:
        return status_time_bucket_shard_key(self.status, self.time_bucket, self.shard_id)

    @property
    def user_status(self) -> str:
        return user_status_key(self.user_id, self.status)


class JobResponse(BaseModel):
    job: JobRecord
    first_execution: ExecutionRecord


class ExecutionListResponse(BaseModel):
    executions: list[ExecutionRecord]
    next_token: str | None = None


class QueuedExecution(BaseModel):
    execution_id: str
    job_id: str
    scheduled_at: int
    due_at: int | None = None


class TaskResult(BaseModel):
    status: Literal["ok"] = "ok"
    output: dict[str, Any] = Field(default_factory=dict)


def epoch_seconds(dt: datetime | None = None) -> int:
    value = dt or datetime.now(timezone.utc)
    return int(value.timestamp())


def execution_shard_id(execution_id: str) -> int:
    digest = sha256(execution_id.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) % settings.execution_shard_count


def status_time_bucket_shard_key(
    status: ExecutionStatus | str,
    time_bucket: str,
    shard_id: int,
) -> str:
    status_value = status.value if isinstance(status, ExecutionStatus) else status
    return f"{status_value}#{time_bucket}#shard_{shard_id:02d}"


def user_status_key(user_id: str, status: ExecutionStatus | str) -> str:
    status_value = status.value if isinstance(status, ExecutionStatus) else status
    return f"{user_id}#{status_value}"
