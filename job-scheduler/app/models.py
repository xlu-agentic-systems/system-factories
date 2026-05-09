from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

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
    def execution_time_key(self) -> str:
        return f"{self.scheduled_at:010d}#{self.execution_id}"


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
