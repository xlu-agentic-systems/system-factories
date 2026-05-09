import json

from redis import Redis

from app.config import settings
from app.models import QueuedExecution


class RedisDelayQueue:
    def __init__(self, redis: Redis, key: str = settings.redis_due_queue_key) -> None:
        self.redis = redis
        self.key = key

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

        pipe = self.redis.pipeline()
        for raw in raw_items:
            pipe.zrem(self.key, raw)
        removed = pipe.execute()

        claimed: list[QueuedExecution] = []
        for raw, was_removed in zip(raw_items, removed, strict=True):
            if was_removed:
                claimed.append(QueuedExecution(**json.loads(raw)))
        return claimed
