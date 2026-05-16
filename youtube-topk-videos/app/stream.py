from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Iterable, Sequence

from app.models import CountDelta, ViewEvent, WINDOWS
from app.time_windows import bucket_start


def stable_hash(value: str) -> int:
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=False)


class ShardedBatchAggregator:
    """Flink-style map and batch aggregate stage for view events."""

    def __init__(
        self,
        shard_count: int = 20,
        windows: Sequence[str] = WINDOWS,
    ) -> None:
        if shard_count <= 0:
            raise ValueError("shard_count must be positive")
        self.shard_count = shard_count
        self.windows = tuple(windows)

    def shard_for(self, event: ViewEvent) -> int:
        key = event.event_id or f"{event.kafka_partition}:{event.kafka_offset}:{event.video_id}"
        return stable_hash(key) % self.shard_count

    def aggregate(self, events: Iterable[ViewEvent]) -> list[CountDelta]:
        counts: Counter[tuple[str, int, str, int]] = Counter()
        for event in events:
            shard_id = self.shard_for(event)
            for window in self.windows:
                counts[(window, bucket_start(window, event.occurred_at), event.video_id, shard_id)] += 1

        return [
            CountDelta(
                window=window,
                bucket_start=start,
                video_id=video_id,
                shard_id=shard_id,
                view_count=count,
            )
            for (window, start, video_id, shard_id), count in counts.items()
        ]


class TopKStreamProcessor:
    def __init__(self, storage, aggregator: ShardedBatchAggregator | None = None) -> None:
        self.storage = storage
        self.aggregator = aggregator or ShardedBatchAggregator()

    def process_batch(self, events: Iterable[ViewEvent]) -> int:
        batch = list(events)
        if not batch:
            return 0
        self.storage.apply_deltas(self.aggregator.aggregate(batch))
        return len(batch)

    def process_stream(self, events: Iterable[ViewEvent], batch_size: int = 10_000) -> int:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")

        processed = 0
        batch: list[ViewEvent] = []
        for event in events:
            batch.append(event)
            if len(batch) >= batch_size:
                processed += self.process_batch(batch)
                batch.clear()

        if batch:
            processed += self.process_batch(batch)
        return processed

