from __future__ import annotations

import os

from fastapi import BackgroundTasks, Body, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import RedirectResponse

from app.models import ClickInput
from app.service import ClickAggregationService
from app.storage import SQLiteClickStorage
from app.stream import StreamProcessor


DEFAULT_DB_PATH = "data/ads_clicks.sqlite3"
DEFAULT_HMAC_SECRET = "local-dev-secret"

app = FastAPI(title="ProjectL Ads Click Aggregation")


def get_storage() -> SQLiteClickStorage:
    db_path = os.getenv("ADS_CLICK_DB_PATH", DEFAULT_DB_PATH)
    return SQLiteClickStorage(db_path)


def get_processor(storage: SQLiteClickStorage = Depends(get_storage)) -> StreamProcessor:
    return StreamProcessor(storage=storage)


def get_service(
    storage: SQLiteClickStorage = Depends(get_storage),
    processor: StreamProcessor = Depends(get_processor),
) -> ClickAggregationService:
    return ClickAggregationService(
        storage=storage,
        processor=processor,
        hmac_secret=os.getenv("ADS_CLICK_HMAC_SECRET", DEFAULT_HMAC_SECRET),
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/sign")
def sign_impression(
    advertiser_id: str = Query(min_length=1),
    ad_id: str = Query(min_length=1),
    impression_id: str = Query(min_length=1),
    service: ClickAggregationService = Depends(get_service),
) -> dict[str, str]:
    return {
        "signature": service.sign_impression(
            advertiser_id=advertiser_id,
            ad_id=ad_id,
            impression_id=impression_id,
        )
    }


@app.get("/click")
def click_redirect(
    background_tasks: BackgroundTasks,
    request: Request,
    advertiser_id: str = Query(min_length=1),
    ad_id: str = Query(min_length=1),
    impression_id: str = Query(min_length=1),
    target_url: str = Query(min_length=1),
    signature: str = Query(min_length=1),
    user_id: str | None = None,
    occurred_at: int | None = None,
    service: ClickAggregationService = Depends(get_service),
    processor: StreamProcessor = Depends(get_processor),
) -> RedirectResponse:
    try:
        service.track_click(
            ClickInput(
                advertiser_id=advertiser_id,
                ad_id=ad_id,
                impression_id=impression_id,
                target_url=target_url,
                signature=signature,
                user_id=user_id,
                occurred_at=occurred_at,
            ),
            source_ip=request.client.host if request.client else None,
            user_agent=request.headers.get("user-agent"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    background_tasks.add_task(processor.run_once)
    return RedirectResponse(url=target_url, status_code=302)


@app.post("/click/log", status_code=202)
def ingest_log_line(
    background_tasks: BackgroundTasks,
    line: str = Body(..., media_type="text/plain"),
    service: ClickAggregationService = Depends(get_service),
    processor: StreamProcessor = Depends(get_processor),
) -> dict[str, object]:
    try:
        result = service.track_click_log_line(line)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    background_tasks.add_task(processor.run_once)
    return {"accepted": result.accepted, "duplicate": result.duplicate}


@app.get("/metrics")
def query_metrics(
    advertiser_id: str = Query(min_length=1),
    start_time: int = Query(ge=0),
    end_time: int = Query(ge=0),
    ad_id: list[str] | None = Query(default=None),
    granularity_seconds: int = Query(default=60, ge=60),
    service: ClickAggregationService = Depends(get_service),
) -> dict[str, object]:
    try:
        points = service.query_metrics(
            advertiser_id=advertiser_id,
            ad_ids=ad_id,
            start_time=start_time,
            end_time=end_time,
            granularity_seconds=granularity_seconds,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {
        "advertiser_id": advertiser_id,
        "granularity_seconds": granularity_seconds,
        "points": [point.__dict__ for point in points],
    }


@app.post("/stream/drain")
def drain_stream(
    batch_size: int = Query(default=1000, ge=1, le=10000),
    max_batches: int = Query(default=100, ge=1, le=10000),
    processor: StreamProcessor = Depends(get_processor),
) -> dict[str, int]:
    return {"processed": processor.drain(batch_size=batch_size, max_batches=max_batches)}


@app.post("/reconcile")
def reconcile(
    start_time: int = Query(ge=0),
    end_time: int = Query(ge=0),
    storage: SQLiteClickStorage = Depends(get_storage),
) -> dict[str, int]:
    if end_time <= start_time:
        raise HTTPException(status_code=400, detail="end_time must be greater than start_time")
    return {"rows_rebuilt": storage.rebuild_derived_metrics(start_time=start_time, end_time=end_time)}

