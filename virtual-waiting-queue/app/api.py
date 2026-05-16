from __future__ import annotations

import asyncio
import json

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse

from app.config import settings
from app.models import (
    AdmissionResult,
    QueueJoinResult,
    QueueSettings,
    QueueState,
    QueueStatus,
    ReservationRequest,
    ReservationResponse,
)
from app.service import WaitingQueueService
from app.store import RedisQueueStore

app = FastAPI(title="Virtual Waiting Queue")


def get_store() -> RedisQueueStore:
    return RedisQueueStore.from_settings()


def get_service(store: RedisQueueStore = Depends(get_store)) -> WaitingQueueService:
    return WaitingQueueService(store=store)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/admin/events/{event_id}/queue/settings", response_model=QueueSettings)
def get_queue_settings(
    event_id: str,
    service: WaitingQueueService = Depends(get_service),
) -> QueueSettings:
    return service.store.get_settings(event_id)


@app.put("/admin/events/{event_id}/queue/settings", response_model=QueueSettings)
def set_queue_settings(
    event_id: str,
    queue_settings: QueueSettings,
    service: WaitingQueueService = Depends(get_service),
) -> QueueSettings:
    return service.configure_event(event_id, queue_settings)


@app.post("/admin/events/{event_id}/queue/admit", response_model=AdmissionResult)
def admit_next(
    event_id: str,
    limit: int | None = Query(default=None, ge=1, le=10_000),
    service: WaitingQueueService = Depends(get_service),
) -> AdmissionResult:
    return service.admit_next(event_id, limit=limit)


@app.post("/events/{event_id}/queue/join", response_model=QueueJoinResult)
def join_queue(
    event_id: str,
    session_id: str = Query(min_length=1),
    service: WaitingQueueService = Depends(get_service),
) -> QueueJoinResult:
    return service.join(event_id, session_id)


@app.get("/events/{event_id}/queue/status", response_model=QueueStatus)
def queue_status(
    event_id: str,
    session_id: str = Query(min_length=1),
    service: WaitingQueueService = Depends(get_service),
) -> QueueStatus:
    return service.status(event_id, session_id)


@app.get("/events/{event_id}/queue/stream")
def queue_stream(
    event_id: str,
    session_id: str = Query(min_length=1),
    service: WaitingQueueService = Depends(get_service),
) -> StreamingResponse:
    service.join(event_id, session_id)

    async def events():
        last_payload: dict[str, object] | None = None
        heartbeat_ticks = max(1, int(settings.heartbeat_seconds / settings.queue_poll_seconds))
        tick = 0

        while True:
            status = service.status(event_id, session_id)
            payload = status.model_dump()
            if payload != last_payload:
                yield _sse("queue_update", payload)
                last_payload = payload

            if status.state == QueueState.ADMITTED:
                yield _sse("admitted", payload)
                break

            tick += 1
            if tick % heartbeat_ticks == 0:
                yield _sse("heartbeat", {"event_id": event_id, "session_id": session_id})

            await asyncio.sleep(settings.queue_poll_seconds)

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/events/{event_id}/reservations", response_model=ReservationResponse)
def reserve(
    event_id: str,
    request: ReservationRequest,
    service: WaitingQueueService = Depends(get_service),
) -> ReservationResponse:
    if not service.can_reserve(event_id, request.session_id):
        raise HTTPException(status_code=403, detail="session has not been admitted through queue")
    return ReservationResponse(
        event_id=event_id,
        session_id=request.session_id,
        accepted=True,
        seats=request.seats,
    )


def _sse(event: str, payload: dict[str, object]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n"
