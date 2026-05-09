import asyncio
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.models import CreateJobRequest, Schedule, ScheduleType, epoch_seconds
from app.queue import RedisDelayQueue
from app.service import JobSchedulerService
from app.storage import DynamoJobStore
from app.worker import Worker


async def main() -> None:
    store = DynamoJobStore.from_settings()
    queue = RedisDelayQueue.from_settings()
    store.create_tables()
    queue.redis.delete(queue.key)
    queue.redis.delete(queue.processing_key)

    service = JobSchedulerService(store=store, queue=queue)
    user_id = f"smoke_{uuid4().hex}"
    immediate = service.create_job(
        CreateJobRequest(
            user_id=user_id,
            task_id="print_message",
            schedule=Schedule(type=ScheduleType.IMMEDIATE),
            parameters={"message": "high-level smoke"},
        )
    )
    future_time = datetime.now(timezone.utc) + timedelta(seconds=2)
    future = service.create_job(
        CreateJobRequest(
            user_id=user_id,
            task_id="print_message",
            schedule=Schedule(type=ScheduleType.DATE, expression=future_time.isoformat()),
            parameters={"message": "future smoke"},
        )
    )

    worker = Worker(store=store, queue=queue, service=service)
    items = queue.pop_due(now=epoch_seconds(), limit=10)
    for item in items:
        await worker.process(item)
    await asyncio.sleep(2.2)
    later_items = queue.pop_due(now=epoch_seconds(), limit=10)
    for item in later_items:
        await worker.process(item)

    executions = store.list_user_executions(user_id=user_id, limit=10)
    statuses = {execution.execution_id: execution.status for execution in executions}
    immediate_status = statuses.get(immediate.first_execution.execution_id)
    future_status = statuses.get(future.first_execution.execution_id)

    print(f"user_id={user_id}")
    print(f"immediate_job_id={immediate.job.job_id}")
    print(f"immediate_execution_id={immediate.first_execution.execution_id}")
    print(f"future_job_id={future.job.job_id}")
    print(f"future_execution_id={future.first_execution.execution_id}")
    print(f"claimed_now={len(items)}")
    print(f"claimed_later={len(later_items)}")
    print(f"immediate_status={immediate_status}")
    print(f"future_status={future_status}")

    if str(immediate_status) != "COMPLETED" or str(future_status) != "COMPLETED":
        raise SystemExit("smoke test failed: executions did not complete")


if __name__ == "__main__":
    asyncio.run(main())
