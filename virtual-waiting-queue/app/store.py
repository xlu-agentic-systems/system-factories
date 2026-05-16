from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod

from redis import BlockingConnectionPool, Redis

from app.config import settings
from app.models import QueueSettings


class QueueStore(ABC):
    @abstractmethod
    def get_settings(self, event_id: str) -> QueueSettings:
        raise NotImplementedError

    @abstractmethod
    def set_settings(self, event_id: str, queue_settings: QueueSettings) -> QueueSettings:
        raise NotImplementedError

    @abstractmethod
    def enqueue(self, event_id: str, session_id: str, joined_at: float | None = None) -> bool:
        raise NotImplementedError

    @abstractmethod
    def dequeue(self, event_id: str, limit: int) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    def mark_admitted(self, event_id: str, session_id: str, ttl_seconds: int) -> None:
        raise NotImplementedError

    @abstractmethod
    def is_admitted(self, event_id: str, session_id: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def position(self, event_id: str, session_id: str) -> int | None:
        raise NotImplementedError

    @abstractmethod
    def depth(self, event_id: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def remove(self, event_id: str, session_id: str) -> None:
        raise NotImplementedError


class RedisQueueStore(QueueStore):
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    @classmethod
    def from_settings(cls) -> "RedisQueueStore":
        pool = BlockingConnectionPool.from_url(
            settings.redis_url,
            decode_responses=True,
            max_connections=settings.redis_max_connections,
            timeout=10,
        )
        return cls(Redis(connection_pool=pool))

    def get_settings(self, event_id: str) -> QueueSettings:
        raw = self.redis.hgetall(self._settings_key(event_id))
        if not raw:
            return QueueSettings(
                enabled=settings.default_queue_enabled,
                admission_ttl_seconds=settings.default_admission_ttl_seconds,
            )
        return QueueSettings(
            enabled=raw.get("enabled") == "1",
            admission_ttl_seconds=int(raw.get("admission_ttl_seconds", "600")),
            default_admit_limit=int(raw.get("default_admit_limit", "100")),
        )

    def set_settings(self, event_id: str, queue_settings: QueueSettings) -> QueueSettings:
        self.redis.hset(
            self._settings_key(event_id),
            mapping={
                "enabled": "1" if queue_settings.enabled else "0",
                "admission_ttl_seconds": str(queue_settings.admission_ttl_seconds),
                "default_admit_limit": str(queue_settings.default_admit_limit),
            },
        )
        return queue_settings

    def enqueue(self, event_id: str, session_id: str, joined_at: float | None = None) -> bool:
        score = joined_at if joined_at is not None else time.time()
        return bool(self.redis.zadd(self._queue_key(event_id), {session_id: score}, nx=True))

    def dequeue(self, event_id: str, limit: int) -> list[str]:
        raw = self.redis.zpopmin(self._queue_key(event_id), count=limit)
        return [session_id for session_id, _score in raw]

    def mark_admitted(self, event_id: str, session_id: str, ttl_seconds: int) -> None:
        pipe = self.redis.pipeline()
        pipe.set(self._admitted_key(event_id, session_id), "1", ex=ttl_seconds)
        pipe.sadd(self._admitted_index_key(event_id), session_id)
        pipe.expire(self._admitted_index_key(event_id), ttl_seconds)
        pipe.execute()

    def is_admitted(self, event_id: str, session_id: str) -> bool:
        return bool(self.redis.exists(self._admitted_key(event_id, session_id)))

    def position(self, event_id: str, session_id: str) -> int | None:
        rank = self.redis.zrank(self._queue_key(event_id), session_id)
        if rank is None:
            return None
        return int(rank) + 1

    def depth(self, event_id: str) -> int:
        return int(self.redis.zcard(self._queue_key(event_id)))

    def remove(self, event_id: str, session_id: str) -> None:
        self.redis.zrem(self._queue_key(event_id), session_id)

    @staticmethod
    def _queue_key(event_id: str) -> str:
        return f"waiting_queue:queue:{event_id}"

    @staticmethod
    def _settings_key(event_id: str) -> str:
        return f"waiting_queue:settings:{event_id}"

    @staticmethod
    def _admitted_key(event_id: str, session_id: str) -> str:
        return f"waiting_queue:admitted:{event_id}:{session_id}"

    @staticmethod
    def _admitted_index_key(event_id: str) -> str:
        return f"waiting_queue:admitted_index:{event_id}"


class InMemoryQueueStore(QueueStore):
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._settings: dict[str, QueueSettings] = {}
        self._queues: dict[str, dict[str, float]] = {}
        self._ordered: dict[str, list[tuple[float, str]]] = {}
        self._positions: dict[str, dict[str, int]] = {}
        self._admitted: dict[tuple[str, str], float] = {}

    def get_settings(self, event_id: str) -> QueueSettings:
        with self._lock:
            return self._settings.get(
                event_id,
                QueueSettings(
                    enabled=settings.default_queue_enabled,
                    admission_ttl_seconds=settings.default_admission_ttl_seconds,
                ),
            )

    def set_settings(self, event_id: str, queue_settings: QueueSettings) -> QueueSettings:
        with self._lock:
            self._settings[event_id] = queue_settings
            return queue_settings

    def enqueue(self, event_id: str, session_id: str, joined_at: float | None = None) -> bool:
        with self._lock:
            queue = self._queues.setdefault(event_id, {})
            if session_id in queue:
                return False
            score = joined_at if joined_at is not None else time.time()
            queue[session_id] = score

            ordered = self._ordered.setdefault(event_id, [])
            ordered.append((score, session_id))
            if len(ordered) > 1 and ordered[-2] > ordered[-1]:
                ordered.sort()
                self._rebuild_positions(event_id)
            else:
                self._positions.setdefault(event_id, {})[session_id] = len(ordered)
            return True

    def dequeue(self, event_id: str, limit: int) -> list[str]:
        with self._lock:
            queue = self._queues.setdefault(event_id, {})
            ordered = self._ordered.setdefault(event_id, [])
            claimed = ordered[:limit]
            del ordered[:limit]
            session_ids = [session_id for _score, session_id in claimed]
            for session_id in session_ids:
                queue.pop(session_id, None)
                self._positions.setdefault(event_id, {}).pop(session_id, None)
            if session_ids:
                self._rebuild_positions(event_id)
            return session_ids

    def mark_admitted(self, event_id: str, session_id: str, ttl_seconds: int) -> None:
        with self._lock:
            self._admitted[(event_id, session_id)] = time.time() + ttl_seconds

    def is_admitted(self, event_id: str, session_id: str) -> bool:
        with self._lock:
            expires_at = self._admitted.get((event_id, session_id))
            if expires_at is None:
                return False
            if expires_at < time.time():
                self._admitted.pop((event_id, session_id), None)
                return False
            return True

    def position(self, event_id: str, session_id: str) -> int | None:
        with self._lock:
            return self._positions.setdefault(event_id, {}).get(session_id)

    def depth(self, event_id: str) -> int:
        with self._lock:
            return len(self._queues.setdefault(event_id, {}))

    def remove(self, event_id: str, session_id: str) -> None:
        with self._lock:
            removed = self._queues.setdefault(event_id, {}).pop(session_id, None)
            if removed is None:
                return
            ordered = self._ordered.setdefault(event_id, [])
            self._ordered[event_id] = [
                item for item in ordered if item[1] != session_id
            ]
            self._positions.setdefault(event_id, {}).pop(session_id, None)
            self._rebuild_positions(event_id)

    def _rebuild_positions(self, event_id: str) -> None:
        self._positions[event_id] = {
            session_id: index
            for index, (_score, session_id) in enumerate(self._ordered.setdefault(event_id, []), start=1)
        }
