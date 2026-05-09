from datetime import datetime, timedelta, timezone

from app.models import (
    CreateJobRequest,
    ExecutionRecord,
    ExecutionStatus,
    JobRecord,
    Schedule,
    ScheduleType,
)
from app.service import JobSchedulerService
from app.storage import DynamoJobStore, _time_buckets_between


class FakeStore:
    def __init__(self) -> None:
        self.jobs: dict[str, JobRecord] = {}
        self.executions: dict[str, ExecutionRecord] = {}
        self.pending_window: list[ExecutionRecord] = []

    def put_job(self, job: JobRecord) -> None:
        self.jobs[job.job_id] = job

    def put_execution(self, execution: ExecutionRecord) -> None:
        self.executions[execution.execution_id] = execution

    def get_job(self, job_id: str) -> JobRecord:
        return self.jobs[job_id]

    def due_executions(
        self, now: int, window_seconds: int, limit: int = 500
    ) -> list[ExecutionRecord]:
        return self.pending_window[:limit]


class FakeQueue:
    def __init__(self) -> None:
        self.items: list[object] = []

    def enqueue(self, item: object) -> None:
        self.items.append(item)


def test_create_immediate_job_persists_execution_and_enqueues() -> None:
    store = FakeStore()
    queue = FakeQueue()
    service = JobSchedulerService(store=store, queue=queue)  # type: ignore[arg-type]

    response = service.create_job(
        CreateJobRequest(
            user_id="user_123",
            task_id="print_message",
            schedule=Schedule(type=ScheduleType.IMMEDIATE),
            parameters={"message": "hello"},
        )
    )

    assert response.job.job_id in store.jobs
    assert response.first_execution.execution_id in store.executions
    assert response.first_execution.status == ExecutionStatus.PENDING
    assert len(queue.items) == 1


def test_create_future_job_outside_window_is_not_enqueued() -> None:
    store = FakeStore()
    queue = FakeQueue()
    service = JobSchedulerService(store=store, queue=queue)  # type: ignore[arg-type]
    future = datetime.now(timezone.utc) + timedelta(hours=2)

    service.create_job(
        CreateJobRequest(
            user_id="user_123",
            task_id="print_message",
            schedule=Schedule(type=ScheduleType.DATE, expression=future.isoformat()),
            parameters={},
        )
    )

    assert len(store.jobs) == 1
    assert len(store.executions) == 1
    assert queue.items == []


def test_scheduler_enqueues_pending_execution_window() -> None:
    store = FakeStore()
    queue = FakeQueue()
    service = JobSchedulerService(store=store, queue=queue)  # type: ignore[arg-type]
    execution = ExecutionRecord(
        job_id="job_123",
        user_id="user_123",
        scheduled_at=int(datetime.now(timezone.utc).timestamp()),
    )
    store.pending_window = [execution]

    count = service.enqueue_pending_window()

    assert count == 1
    assert len(queue.items) == 1


def test_completed_cron_job_creates_next_execution() -> None:
    store = FakeStore()
    queue = FakeQueue()
    service = JobSchedulerService(store=store, queue=queue)  # type: ignore[arg-type]
    job = JobRecord(
        job_id="job_123",
        user_id="user_123",
        task_id="print_message",
        schedule=Schedule(type=ScheduleType.CRON, expression="*/5 * * * *"),
        parameters={},
    )
    completed = ExecutionRecord(
        job_id=job.job_id,
        user_id=job.user_id,
        scheduled_at=1_700_000_000,
        status=ExecutionStatus.COMPLETED,
    )
    store.put_job(job)

    next_execution = service.create_next_recurring_execution(completed)

    assert next_execution is not None
    assert next_execution.execution_id in store.executions
    assert next_execution.scheduled_at > completed.scheduled_at
    assert next_execution.status == ExecutionStatus.PENDING


def test_execution_item_includes_shard_and_gsi_keys() -> None:
    execution = ExecutionRecord(
        execution_id="execution_123",
        job_id="job_123",
        user_id="user_123",
        scheduled_at=1_700_000_000,
    )

    item = DynamoJobStore._execution_to_item(execution)

    assert item["time_bucket"] == "1699999200"
    assert item["time_bucket_shard"].startswith("1699999200#shard_")
    assert item["status_time_bucket_shard"].startswith("PENDING#1699999200#shard_")
    assert item["user_status"] == "user_123#PENDING"


def test_time_bucket_range_crosses_hour_boundary() -> None:
    assert _time_buckets_between(1_700_003_590, 1_700_006_401) == [
        "1700002800",
        "1700006400",
    ]
