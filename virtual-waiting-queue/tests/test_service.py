from app.models import QueueSettings, QueueState
from app.service import WaitingQueueService
from app.store import InMemoryQueueStore


def service() -> WaitingQueueService:
    return WaitingQueueService(InMemoryQueueStore())


def test_disabled_queue_admits_immediately() -> None:
    waiting_queue = service()
    waiting_queue.configure_event("event_1", QueueSettings(enabled=False, admission_ttl_seconds=60))

    result = waiting_queue.join("event_1", "session_1")

    assert result.state == QueueState.ADMITTED
    assert waiting_queue.can_reserve("event_1", "session_1")
    assert waiting_queue.store.depth("event_1") == 0


def test_enabled_queue_preserves_fifo_order() -> None:
    waiting_queue = service()
    waiting_queue.configure_event("event_1", QueueSettings(enabled=True, admission_ttl_seconds=60))
    store = waiting_queue.store

    store.enqueue("event_1", "session_a", joined_at=10)
    store.enqueue("event_1", "session_b", joined_at=20)
    store.enqueue("event_1", "session_c", joined_at=30)

    result = waiting_queue.admit_next("event_1", limit=2)

    assert result.admitted_session_ids == ["session_a", "session_b"]
    assert waiting_queue.can_reserve("event_1", "session_a")
    assert waiting_queue.can_reserve("event_1", "session_b")
    assert not waiting_queue.can_reserve("event_1", "session_c")
    assert waiting_queue.status("event_1", "session_c").position == 1


def test_booking_guard_rejects_until_session_is_admitted() -> None:
    waiting_queue = service()
    waiting_queue.configure_event("event_1", QueueSettings(enabled=True, admission_ttl_seconds=60))

    join = waiting_queue.join("event_1", "session_1")
    assert join.state == QueueState.QUEUED
    assert not waiting_queue.can_reserve("event_1", "session_1")

    waiting_queue.admit_next("event_1", limit=1)

    assert waiting_queue.status("event_1", "session_1").state == QueueState.ADMITTED
    assert waiting_queue.can_reserve("event_1", "session_1")


def test_duplicate_join_does_not_duplicate_queue_entry() -> None:
    waiting_queue = service()
    waiting_queue.configure_event("event_1", QueueSettings(enabled=True, admission_ttl_seconds=60))

    first = waiting_queue.join("event_1", "session_1")
    second = waiting_queue.join("event_1", "session_1")

    assert first.position == 1
    assert second.position == 1
    assert waiting_queue.store.depth("event_1") == 1
