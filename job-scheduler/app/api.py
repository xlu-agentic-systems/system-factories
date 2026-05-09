from fastapi import Depends, FastAPI, HTTPException, Query

from app.models import CreateJobRequest, ExecutionListResponse, ExecutionStatus, JobResponse
from app.queue import RedisDelayQueue
from app.service import JobSchedulerService
from app.storage import DynamoJobStore, NotFoundError

app = FastAPI(title="Job Scheduler")


def get_store() -> DynamoJobStore:
    return DynamoJobStore.from_settings()


def get_queue() -> RedisDelayQueue:
    return RedisDelayQueue.from_settings()


def get_service(
    store: DynamoJobStore = Depends(get_store),
    queue: RedisDelayQueue = Depends(get_queue),
) -> JobSchedulerService:
    return JobSchedulerService(store=store, queue=queue)


@app.on_event("startup")
def create_tables() -> None:
    get_store().create_tables()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/jobs", response_model=JobResponse, status_code=201)
def create_job(
    request: CreateJobRequest,
    service: JobSchedulerService = Depends(get_service),
) -> JobResponse:
    try:
        return service.create_job(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/jobs", response_model=ExecutionListResponse)
@app.get("/jobs/executions", response_model=ExecutionListResponse)
def list_jobs(
    user_id: str = Query(min_length=1),
    status: ExecutionStatus | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    store: DynamoJobStore = Depends(get_store),
) -> ExecutionListResponse:
    try:
        executions = store.list_user_executions(
            user_id=user_id,
            status=status,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )
    except NotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return ExecutionListResponse(executions=executions)
