import json

from redis import Redis

from app.config import settings
from app.models import QueuedExecution


class RedisDelayQueue:
    def __init__(
        self,
        redis: Redis,
        key: str = settings.redis_due_queue_key,
        processing_key: str = settings.redis_processing_queue_key,
    ) -> None:
        self.redis = redis
        self.key = key
        self.processing_key = processing_key

    @classmethod
    def from_settings(cls) -> "RedisDelayQueue":
        return cls(Redis.from_url(settings.redis_url, decode_responses=True))

    def enqueue(self, item: QueuedExecution) -> None:
        score = item.due_at or item.scheduled_at
        self.redis.zadd(
            self.key,
            {item.model_dump_json(): score},
            nx=True,
        )

    def pop_due(self, now: int, limit: int = 100) -> list[QueuedExecution]:
        raw_items = self.redis.zrangebyscore(self.key, min=0, max=now, start=0, num=limit)
        if not raw_items:
            return []

        claimed: list[QueuedExecution] = []
        for raw in raw_items:
            was_removed = self.redis.zrem(self.key, raw)
            if was_removed:
                self.redis.zadd(
                    self.processing_key,
                    {raw: now + settings.queue_visibility_timeout_seconds},
                )
                claimed.append(QueuedExecution(**json.loads(raw)))
        return claimed

    def ack(self, item: QueuedExecution) -> None:
        self.redis.zrem(self.processing_key, item.model_dump_json())

    def release(self, item: QueuedExecution, due_at: int) -> None:
        raw = item.model_dump_json()
        retry = item.model_copy(update={"due_at": due_at})
        pipe = self.redis.pipeline()
        pipe.zrem(self.processing_key, raw)
        pipe.zadd(self.key, {retry.model_dump_json(): due_at})
        pipe.execute()

    def extend_lease(self, item: QueuedExecution, now: int) -> None:
        self.redis.zadd(
            self.processing_key,
            {item.model_dump_json(): now + settings.queue_visibility_timeout_seconds},
            xx=True,
        )

    def reclaim_expired(self, now: int, limit: int = 100) -> list[QueuedExecution]:
        raw_items = self.redis.zrangebyscore(
            self.processing_key,
            min=0,
            max=now,
            start=0,
            num=limit,
        )
        if not raw_items:
            return []

        pipe = self.redis.pipeline()
        for raw in raw_items:
            pipe.zrem(self.processing_key, raw)
        removed = pipe.execute()

        reclaimed: list[QueuedExecution] = []
        for raw, was_removed in zip(raw_items, removed, strict=True):
            if was_removed:
                reclaimed.append(QueuedExecution(**json.loads(raw)))
        return reclaimed
