from __future__ import annotations

from app.models import AdmissionResult, QueueJoinResult, QueueSettings, QueueState, QueueStatus
from app.store import QueueStore


class WaitingQueueService:
    def __init__(self, store: QueueStore) -> None:
        self.store = store

    def configure_event(self, event_id: str, settings: QueueSettings) -> QueueSettings:
        return self.store.set_settings(event_id, settings)

    def join(self, event_id: str, session_id: str) -> QueueJoinResult:
        queue_settings = self.store.get_settings(event_id)
        if self.store.is_admitted(event_id, session_id):
            return self._join_result(event_id, session_id, QueueState.ADMITTED)

        if not queue_settings.enabled:
            self.store.mark_admitted(
                event_id=event_id,
                session_id=session_id,
                ttl_seconds=queue_settings.admission_ttl_seconds,
            )
            return self._join_result(event_id, session_id, QueueState.ADMITTED)

        self.store.enqueue(event_id, session_id)
        return self._join_result(event_id, session_id, QueueState.QUEUED)

    def status(self, event_id: str, session_id: str) -> QueueStatus:
        if self.store.is_admitted(event_id, session_id):
            return QueueStatus(
                event_id=event_id,
                session_id=session_id,
                state=QueueState.ADMITTED,
                position=None,
                queue_depth=self.store.depth(event_id),
            )

        position = self.store.position(event_id, session_id)
        if position is None:
            return QueueStatus(
                event_id=event_id,
                session_id=session_id,
                state=QueueState.NOT_FOUND,
                position=None,
                queue_depth=self.store.depth(event_id),
            )

        return QueueStatus(
            event_id=event_id,
            session_id=session_id,
            state=QueueState.QUEUED,
            position=position,
            queue_depth=self.store.depth(event_id),
        )

    def admit_next(self, event_id: str, limit: int | None = None) -> AdmissionResult:
        queue_settings = self.store.get_settings(event_id)
        admit_limit = limit or queue_settings.default_admit_limit
        session_ids = self.store.dequeue(event_id, admit_limit)
        for session_id in session_ids:
            self.store.mark_admitted(
                event_id=event_id,
                session_id=session_id,
                ttl_seconds=queue_settings.admission_ttl_seconds,
            )

        return AdmissionResult(
            event_id=event_id,
            admitted_count=len(session_ids),
            admitted_session_ids=session_ids,
            remaining_depth=self.store.depth(event_id),
        )

    def can_reserve(self, event_id: str, session_id: str) -> bool:
        return self.store.is_admitted(event_id, session_id)

    def leave(self, event_id: str, session_id: str) -> None:
        self.store.remove(event_id, session_id)

    def _join_result(self, event_id: str, session_id: str, state: QueueState) -> QueueJoinResult:
        position = None
        if state == QueueState.QUEUED:
            position = self.store.position(event_id, session_id)
        return QueueJoinResult(
            event_id=event_id,
            session_id=session_id,
            state=state,
            position=position,
            queue_depth=self.store.depth(event_id),
        )
