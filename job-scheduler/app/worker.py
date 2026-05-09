import asyncio
import logging

from app.config import settings
from app.models import ExecutionStatus, QueuedExecution, epoch_seconds
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
            await self.recover_expired_leases()
            items = self.queue.pop_due(
                now=epoch_seconds(),
                limit=settings.worker_batch_size,
            )
            if not items:
                await asyncio.sleep(settings.worker_poll_seconds)
                continue

            await asyncio.gather(*(self.process(item) for item in items))

    async def recover_expired_leases(self) -> None:
        expired = self.queue.reclaim_expired(
            now=epoch_seconds(),
            limit=settings.worker_batch_size,
        )
        for item in expired:
            try:
                execution = self.store.get_execution(item.execution_id)
            except NotFoundError:
                continue

            if execution.status in {ExecutionStatus.COMPLETED, ExecutionStatus.FAILED}:
                continue

            if execution.attempt >= settings.max_attempts:
                try:
                    self.store.mark_failed(execution, "worker lease expired")
                except ConflictError:
                    pass
                continue

            if execution.status == ExecutionStatus.IN_PROGRESS:
                try:
                    self.store.mark_retrying(execution, "worker lease expired")
                except ConflictError:
                    continue

            self.queue.enqueue(item.model_copy(update={"due_at": epoch_seconds()}))

    async def process(self, item: QueuedExecution) -> None:
        try:
            execution = self.store.get_execution(item.execution_id)
            execution = self.store.mark_in_progress(execution)
        except (ConflictError, NotFoundError) as exc:
            logger.info("execution is no longer claimable", extra={"error": str(exc)})
            self.queue.ack(item)
            return

        heartbeat = asyncio.create_task(self._heartbeat(item))
        try:
            job = self.store.get_job(execution.job_id)
            await registry.run(job.task_id, job.parameters)
        except Exception as exc:  # noqa: BLE001 - task code is the failure boundary.
            error = str(exc)
            if execution.attempt >= settings.max_attempts:
                self.store.mark_failed(execution, error)
                heartbeat.cancel()
                self.queue.ack(item)
                logger.exception("execution failed permanently")
                return

            retrying = self.store.mark_retrying(execution, error)
            delay = retry_delay_seconds(retrying.attempt)
            heartbeat.cancel()
            self.queue.release(item, due_at=epoch_seconds() + delay)
            logger.exception("execution failed, scheduled retry")
            return

        completed = self.store.mark_completed(execution)
        heartbeat.cancel()
        self.queue.ack(item)
        self.service.create_next_recurring_execution(completed)
        logger.info(
            "execution completed",
            extra={"execution_id": completed.execution_id, "job_id": completed.job_id},
        )

    async def _heartbeat(self, item: QueuedExecution) -> None:
        interval = max(1, settings.queue_visibility_timeout_seconds // 2)
        while True:
            await asyncio.sleep(interval)
            self.queue.extend_lease(item, now=epoch_seconds())


async def main() -> None:
    store = DynamoJobStore.from_settings()
    queue = RedisDelayQueue.from_settings()
    service = JobSchedulerService(store=store, queue=queue)
    await Worker(store=store, queue=queue, service=service).run_forever()


if __name__ == "__main__":
    asyncio.run(main())
