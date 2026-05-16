from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field


class QueueState(StrEnum):
    ADMITTED = "admitted"
    QUEUED = "queued"
    NOT_FOUND = "not_found"


class QueueSettings(BaseModel):
    enabled: bool = False
    admission_ttl_seconds: int = Field(default=600, ge=1, le=86_400)
    default_admit_limit: int = Field(default=100, ge=1, le=10_000)


class QueueJoinResult(BaseModel):
    event_id: str
    session_id: str
    state: QueueState
    position: int | None = None
    queue_depth: int


class QueueStatus(BaseModel):
    event_id: str
    session_id: str
    state: QueueState
    position: int | None = None
    queue_depth: int


class AdmissionResult(BaseModel):
    event_id: str
    admitted_count: int
    admitted_session_ids: list[str]
    remaining_depth: int


class ReservationRequest(BaseModel):
    session_id: str = Field(min_length=1)
    seats: list[str] = Field(default_factory=list)


class ReservationResponse(BaseModel):
    event_id: str
    session_id: str
    accepted: bool
    seats: list[str]
