from __future__ import annotations

import time
import uuid
from urllib.parse import urlparse

from app.log_parser import parse_click_log_line
from app.models import ClickEvent, ClickInput, ClickResult, MetricPoint
from app.signing import sign_impression, verify_impression_signature
from app.storage import SQLiteClickStorage
from app.stream import StreamProcessor


class ClickAggregationService:
    def __init__(
        self,
        *,
        storage: SQLiteClickStorage,
        hmac_secret: str,
        processor: StreamProcessor | None = None,
    ) -> None:
        self.storage = storage
        self.hmac_secret = hmac_secret
        self.processor = processor

    def sign_impression(self, *, advertiser_id: str, ad_id: str, impression_id: str) -> str:
        return sign_impression(
            secret=self.hmac_secret,
            advertiser_id=advertiser_id,
            ad_id=ad_id,
            impression_id=impression_id,
        )

    def track_click(
        self,
        click: ClickInput,
        *,
        source_ip: str | None = None,
        user_agent: str | None = None,
        process_inline: bool = False,
    ) -> ClickResult:
        self._validate_target_url(click.target_url)
        if not verify_impression_signature(
            secret=self.hmac_secret,
            advertiser_id=click.advertiser_id,
            ad_id=click.ad_id,
            impression_id=click.impression_id,
            signature=click.signature,
        ):
            raise ValueError("invalid impression signature")

        now = int(time.time())
        event = ClickEvent(
            event_id=str(uuid.uuid4()),
            advertiser_id=click.advertiser_id,
            ad_id=click.ad_id,
            impression_id=click.impression_id,
            user_id=click.user_id,
            occurred_at=click.occurred_at or now,
            target_url=click.target_url,
            signature=click.signature,
            source_ip=source_ip,
            user_agent=user_agent,
            received_at=now,
        )
        accepted, _ = self.storage.record_click(event)
        if accepted and process_inline and self.processor is not None:
            self.processor.run_once()
        return ClickResult(
            accepted=accepted,
            duplicate=not accepted,
            event_id=event.event_id if accepted else None,
            target_url=click.target_url,
        )

    def track_click_log_line(self, line: str, *, process_inline: bool = False) -> ClickResult:
        return self.track_click(parse_click_log_line(line), process_inline=process_inline)

    def query_metrics(
        self,
        *,
        advertiser_id: str,
        start_time: int,
        end_time: int,
        ad_ids: list[str] | None = None,
        granularity_seconds: int = 60,
    ) -> list[MetricPoint]:
        if end_time <= start_time:
            raise ValueError("end_time must be greater than start_time")
        return self.storage.query_metrics(
            advertiser_id=advertiser_id,
            start_time=start_time,
            end_time=end_time,
            ad_ids=ad_ids,
            granularity_seconds=granularity_seconds,
        )

    @staticmethod
    def _validate_target_url(target_url: str) -> None:
        parsed = urlparse(target_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("target_url must be an absolute http or https URL")

