from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from app.db import connect


@dataclass(frozen=True)
class Lease:
    resource_id: str
    owner_id: str
    token: str
    expires_at: float | None


class LocalAppLockBackend:
    """A process-local lock table. It is intentionally not shared across processes."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._owners: dict[str, Lease] = {}

    def acquire(self, resource_id: str, owner_id: str, ttl_seconds: float) -> Lease | None:
        del ttl_seconds
        with self._lock:
            if resource_id in self._owners:
                return None
            lease = Lease(resource_id, owner_id, _token(owner_id), None)
            self._owners[resource_id] = lease
            return lease

    def release(self, lease: Lease) -> bool:
        with self._lock:
            current = self._owners.get(lease.resource_id)
            if current is None or current.token != lease.token:
                return False
            del self._owners[lease.resource_id]
            return True


class DbTtlLeaseBackend:
    """A durable lease table guarded by SQLite write locking."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        _init_db_lease_table(self.db_path)

    def acquire(self, resource_id: str, owner_id: str, ttl_seconds: float) -> Lease | None:
        now = time.time()
        expires_at = now + ttl_seconds
        token = _token(owner_id)
        conn = connect(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT owner_id, token, expires_at
                FROM db_leases
                WHERE resource_id = ?
                """,
                (resource_id,),
            ).fetchone()
            if row is not None and float(row["expires_at"]) > now:
                conn.execute("ROLLBACK")
                return None
            conn.execute(
                """
                INSERT INTO db_leases (resource_id, owner_id, token, expires_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(resource_id) DO UPDATE SET
                    owner_id = excluded.owner_id,
                    token = excluded.token,
                    expires_at = excluded.expires_at
                """,
                (resource_id, owner_id, token, expires_at),
            )
            conn.execute("COMMIT")
            return Lease(resource_id, owner_id, token, expires_at)
        except sqlite3.Error:
            _rollback_quietly(conn)
            raise
        finally:
            conn.close()

    def release(self, lease: Lease) -> bool:
        with connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                DELETE FROM db_leases
                WHERE resource_id = ?
                  AND token = ?
                """,
                (lease.resource_id, lease.token),
            )
        return cursor.rowcount == 1


class RedisTtlLeaseBackend:
    """Redis SET NX PX lease semantics, backed by a local store for this demo."""

    def __init__(self, store: "RedisTtlStore") -> None:
        self.store = store

    def acquire(self, resource_id: str, owner_id: str, ttl_seconds: float) -> Lease | None:
        token = _token(owner_id)
        key = _redis_lock_key(resource_id)
        ttl_ms = int(ttl_seconds * 1000)
        if not self.store.set_nx_px(key, token, ttl_ms):
            return None
        return Lease(resource_id, owner_id, token, time.time() + ttl_seconds)

    def release(self, lease: Lease) -> bool:
        key = _redis_lock_key(lease.resource_id)
        return self.store.delete_if_value(key, lease.token)


class RedisTtlStore:
    def set_nx_px(self, key: str, value: str, ttl_ms: int) -> bool:
        raise NotImplementedError

    def delete_if_value(self, key: str, value: str) -> bool:
        raise NotImplementedError


class InMemoryRedisTtlStore(RedisTtlStore):
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._values: dict[str, tuple[str, float]] = {}

    def set_nx_px(self, key: str, value: str, ttl_ms: int) -> bool:
        now = time.time()
        with self._lock:
            self._delete_if_expired(key, now)
            if key in self._values:
                return False
            self._values[key] = (value, now + ttl_ms / 1000)
            return True

    def delete_if_value(self, key: str, value: str) -> bool:
        now = time.time()
        with self._lock:
            self._delete_if_expired(key, now)
            current = self._values.get(key)
            if current is None or current[0] != value:
                return False
            del self._values[key]
            return True

    def _delete_if_expired(self, key: str, now: float) -> None:
        current = self._values.get(key)
        if current is not None and current[1] <= now:
            del self._values[key]


class SqliteRedisTtlStore(RedisTtlStore):
    """A process-safe local stand-in for Redis TTL commands used by tests and CLI."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        _init_redis_ttl_table(self.db_path)

    def set_nx_px(self, key: str, value: str, ttl_ms: int) -> bool:
        now = time.time()
        expires_at = now + ttl_ms / 1000
        conn = connect(self.db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT value, expires_at
                FROM redis_ttl_keys
                WHERE key = ?
                """,
                (key,),
            ).fetchone()
            if row is not None and float(row["expires_at"]) > now:
                conn.execute("ROLLBACK")
                return False
            conn.execute(
                """
                INSERT INTO redis_ttl_keys (key, value, expires_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    expires_at = excluded.expires_at
                """,
                (key, value, expires_at),
            )
            conn.execute("COMMIT")
            return True
        except sqlite3.Error:
            _rollback_quietly(conn)
            raise
        finally:
            conn.close()

    def delete_if_value(self, key: str, value: str) -> bool:
        with connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                DELETE FROM redis_ttl_keys
                WHERE key = ?
                  AND value = ?
                """,
                (key, value),
            )
        return cursor.rowcount == 1


class RedisServerTtlStore(RedisTtlStore):
    def __init__(self, redis_url: str) -> None:
        try:
            import redis
        except ImportError as exc:
            raise RuntimeError("install the redis package to use --redis-url") from exc

        self.client = redis.Redis.from_url(redis_url, decode_responses=True)

    def set_nx_px(self, key: str, value: str, ttl_ms: int) -> bool:
        return bool(self.client.set(key, value, nx=True, px=max(1, ttl_ms)))

    def delete_if_value(self, key: str, value: str) -> bool:
        script = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        end
        return 0
        """
        return bool(self.client.eval(script, 1, key, value))


def _init_db_lease_table(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS db_leases (
                resource_id TEXT PRIMARY KEY,
                owner_id TEXT NOT NULL,
                token TEXT NOT NULL,
                expires_at REAL NOT NULL
            );
            """
        )


def _init_redis_ttl_table(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS redis_ttl_keys (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                expires_at REAL NOT NULL
            );
            """
        )


def _redis_lock_key(resource_id: str) -> str:
    return f"lease:{resource_id}"


def _token(owner_id: str) -> str:
    return f"{owner_id}:{uuid.uuid4().hex}"


def _rollback_quietly(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass
