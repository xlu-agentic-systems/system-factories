from __future__ import annotations

from dataclasses import dataclass


MIN_GRANULARITY_SECONDS = 60


@dataclass(frozen=True)
class ClickInput:
    advertiser_id: str
    ad_id: str
    impression_id: str
    target_url: str
    signature: str
    occurred_at: int | None = None
    user_id: str | None = None


@dataclass(frozen=True)
class ClickEvent:
    event_id: str
    advertiser_id: str
    ad_id: str
    impression_id: str
    target_url: str
    signature: str
    occurred_at: int
    received_at: int
    user_id: str | None = None
    source_ip: str | None = None
    user_agent: str | None = None
    raw_event_id: int | None = None

    @property
    def minute_start(self) -> int:
        return self.occurred_at - (self.occurred_at % MIN_GRANULARITY_SECONDS)


@dataclass(frozen=True)
class ClickResult:
    accepted: bool
    duplicate: bool
    event_id: str | None
    target_url: str


@dataclass(frozen=True)
class MappedEvent:
    partition_key: str
    event: ClickEvent


@dataclass(frozen=True)
class PartialAggregate:
    partition_key: str
    advertiser_id: str
    ad_id: str
    minute_start: int
    click_count: int


@dataclass(frozen=True)
class MetricDelta:
    advertiser_id: str
    ad_id: str
    minute_start: int
    click_count: int


@dataclass(frozen=True)
class MetricPoint:
    advertiser_id: str
    ad_id: str
    bucket_start: int
    click_count: int

