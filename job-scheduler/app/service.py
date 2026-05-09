from app.config import settings
from app.models import (
    CreateJobRequest,
    ExecutionRecord,
    ExecutionStatus,
    JobRecord,
    JobResponse,
    QueuedExecution,
    ScheduleType,
    epoch_seconds,
)
from app.queue import RedisDelayQueue
from app.storage import DynamoJobStore
from app.time_utils import next_scheduled_at


class JobSchedulerService:
    def __init__(self, store: DynamoJobStore, queue: RedisDelayQueue) -> None:
        self.store = store
        self.queue = queue

    def create_job(self, request: CreateJobRequest) -> JobResponse:
        scheduled_at = next_scheduled_at(request.schedule)
        if scheduled_at is None:
            raise ValueError("schedule did not produce an execution time")

        job = JobRecord(
            user_id=request.user_id,
            task_id=request.task_id,
            schedule=request.schedule,
            parameters=request.parameters,
        )
        execution = ExecutionRecord(
            job_id=job.job_id,
            user_id=job.user_id,
            scheduled_at=scheduled_at,
        )

        self.store.put_job(job)
        self.store.put_execution(execution)
        self.enqueue_if_near_term(execution)

        return JobResponse(job=job, first_execution=execution)

    def enqueue_if_near_term(self, execution: ExecutionRecord) -> None:
        now = epoch_seconds()
        if execution.scheduled_at <= now + settings.scheduler_window_seconds:
            self.queue.enqueue(
                QueuedExecution(
                    execution_id=execution.execution_id,
                    job_id=execution.job_id,
                    scheduled_at=execution.scheduled_at,
                )
            )

    def enqueue_pending_window(self) -> int:
        now = epoch_seconds()
        executions = self.store.due_executions(
            now=now,
            window_seconds=settings.scheduler_window_seconds,
        )
        for execution in executions:
            self.queue.enqueue(
                QueuedExecution(
                    execution_id=execution.execution_id,
                    job_id=execution.job_id,
                    scheduled_at=execution.scheduled_at,
                )
            )
        return len(executions)

    def create_next_recurring_execution(self, completed: ExecutionRecord) -> ExecutionRecord | None:
        job = self.store.get_job(completed.job_id)
        if job.schedule.type != ScheduleType.CRON:
            return None

        next_time = next_scheduled_at(job.schedule, after=completed.scheduled_at)
        if next_time is None:
            return None

        execution = ExecutionRecord(
            job_id=job.job_id,
            user_id=job.user_id,
            scheduled_at=next_time,
            status=ExecutionStatus.PENDING,
        )
        self.store.put_execution(execution)
        self.enqueue_if_near_term(execution)
        return execution
