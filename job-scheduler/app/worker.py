import asyncio
import logging

from app.config import settings
from app.models import QueuedExecution, epoch_seconds
from app.queue import RedisDelayQueue
from app.registry import registry
from app.service import JobSchedulerService
from app.storage import ConflictError, DynamoJobStore, NotFoundError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def retry_delay_seconds(attempt: int) -> int:
    return min(5**attempt, 300)


class Worker:
    def __init__(
        self,
        store: DynamoJobStore,
        queue: RedisDelayQueue,
        service: JobSchedulerService,
    ) -> None:
        self.store = store
        self.queue = queue
        self.service = service

    async def run_forever(self) -> None:
        self.store.create_tables()
        while True:
            items = self.queue.pop_due(now=epoch_seconds(), limit=100)
            if not items:
                await asyncio.sleep(settings.worker_poll_seconds)
                continue

            await asyncio.gather(*(self.process(item) for item in items))

    async def process(self, item: QueuedExecution) -> None:
        try:
            execution = self.store.get_execution(item.execution_id)
            execution = self.store.mark_in_progress(execution)
        except (ConflictError, NotFoundError) as exc:
            logger.info("execution is no longer claimable", extra={"error": str(exc)})
            return

        try:
            job = self.store.get_job(execution.job_id)
            await registry.run(job.task_id, job.parameters)
        except Exception as exc:  # noqa: BLE001 - task code is the failure boundary.
            error = str(exc)
            if execution.attempt >= settings.max_attempts:
                self.store.mark_failed(execution, error)
                logger.exception("execution failed permanently")
                return

            retrying = self.store.mark_retrying(execution, error)
            delay = retry_delay_seconds(retrying.attempt)
            self.queue.enqueue(
                QueuedExecution(
                    execution_id=retrying.execution_id,
                    job_id=retrying.job_id,
                    scheduled_at=retrying.scheduled_at,
                    due_at=epoch_seconds() + delay,
                )
            )
            logger.exception("execution failed, scheduled retry")
            return

        completed = self.store.mark_completed(execution)
        self.service.create_next_recurring_execution(completed)
        logger.info(
            "execution completed",
            extra={"execution_id": completed.execution_id, "job_id": completed.job_id},
        )


async def main() -> None:
    store = DynamoJobStore.from_settings()
    queue = RedisDelayQueue.from_settings()
    service = JobSchedulerService(store=store, queue=queue)
    await Worker(store=store, queue=queue, service=service).run_forever()


if __name__ == "__main__":
    asyncio.run(main())
