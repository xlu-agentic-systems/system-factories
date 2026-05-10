from __future__ import annotations

import hashlib
from collections import defaultdict

from app.models import ClickEvent, MappedEvent, MetricDelta, PartialAggregate
from app.storage import SQLiteClickStorage


def stable_shard(value: str, shard_count: int) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % shard_count


class MapNode:
    """Shards events by ad_id, with optional extra virtual shards for hot ads."""

    def __init__(self, shard_count: int = 32, hot_ad_shards: dict[str, int] | None = None):
        if shard_count < 1:
            raise ValueError("shard_count must be >= 1")
        self.shard_count = shard_count
        self.hot_ad_shards = hot_ad_shards or {}

    def map(self, events: list[ClickEvent]) -> list[MappedEvent]:
        mapped: list[MappedEvent] = []
        for event in events:
            base_shard = stable_shard(event.ad_id, self.shard_count)
            hot_shards = max(1, self.hot_ad_shards.get(event.ad_id, 1))
            if hot_shards > 1:
                virtual = stable_shard(event.impression_id, hot_shards)
                partition_key = f"shard_{base_shard:03d}:{event.ad_id}:{virtual}"
            else:
                partition_key = f"shard_{base_shard:03d}:{event.ad_id}"
            mapped.append(MappedEvent(partition_key=partition_key, event=event))
        return mapped


class AggregateNode:
    """Aggregates mapped events by partition, ad, and event-time minute."""

    def aggregate(self, mapped_events: list[MappedEvent]) -> list[PartialAggregate]:
        counts: dict[tuple[str, str, str, int], int] = defaultdict(int)
        for mapped in mapped_events:
            event = mapped.event
            key = (
                mapped.partition_key,
                event.advertiser_id,
                event.ad_id,
                event.minute_start,
            )
            counts[key] += 1

        return [
            PartialAggregate(
                partition_key=partition_key,
                advertiser_id=advertiser_id,
                ad_id=ad_id,
                minute_start=minute_start,
                click_count=count,
            )
            for (partition_key, advertiser_id, ad_id, minute_start), count in counts.items()
        ]


class ReduceNode:
    """Combines partial aggregates into advertiser/ad/minute metric deltas."""

    def reduce(self, partials: list[PartialAggregate]) -> list[MetricDelta]:
        counts: dict[tuple[str, str, int], int] = defaultdict(int)
        for partial in partials:
            key = (partial.advertiser_id, partial.ad_id, partial.minute_start)
            counts[key] += partial.click_count

        return [
            MetricDelta(
                advertiser_id=advertiser_id,
                ad_id=ad_id,
                minute_start=minute_start,
                click_count=count,
            )
            for (advertiser_id, ad_id, minute_start), count in counts.items()
        ]


class MapAggregateReducePipeline:
    def __init__(
        self,
        map_node: MapNode | None = None,
        aggregate_node: AggregateNode | None = None,
        reduce_node: ReduceNode | None = None,
    ) -> None:
        self.map_node = map_node or MapNode()
        self.aggregate_node = aggregate_node or AggregateNode()
        self.reduce_node = reduce_node or ReduceNode()

    def process(self, events: list[ClickEvent]) -> list[MetricDelta]:
        mapped = self.map_node.map(events)
        partials = self.aggregate_node.aggregate(mapped)
        return self.reduce_node.reduce(partials)


class StreamProcessor:
    """Durable micro-batch stream processor backed by the raw event outbox."""

    def __init__(
        self,
        storage: SQLiteClickStorage,
        pipeline: MapAggregateReducePipeline | None = None,
        cursor_name: str = "map-aggregate-reduce-v1",
    ) -> None:
        self.storage = storage
        self.pipeline = pipeline or MapAggregateReducePipeline()
        self.cursor_name = cursor_name

    def run_once(self, batch_size: int = 1000) -> int:
        cursor = self.storage.get_cursor(self.cursor_name)
        events = self.storage.fetch_raw_after(cursor, limit=batch_size)
        if not events:
            return 0

        deltas = self.pipeline.process(events)
        last_raw_event_id = events[-1].raw_event_id
        if last_raw_event_id is None:
            raise ValueError("raw events must include raw_event_id")

        self.storage.commit_deltas_and_cursor(
            cursor_name=self.cursor_name,
            deltas=deltas,
            last_raw_event_id=last_raw_event_id,
        )
        return len(events)

    def drain(self, *, batch_size: int = 1000, max_batches: int = 1000) -> int:
        processed = 0
        for _ in range(max_batches):
            count = self.run_once(batch_size=batch_size)
            if count == 0:
                break
            processed += count
        return processed

