from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import psycopg


@dataclass(frozen=True)
class WriteResult:
    ok: bool
    post_id: str
    user_id: str
    elapsed_ms: float


SCHEMA_SQL = """
DROP TABLE IF EXISTS likes;
DROP TABLE IF EXISTS post_counters;

CREATE TABLE post_counters (
    post_id TEXT PRIMARY KEY,
    like_count BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE likes (
    user_id TEXT NOT NULL,
    post_id TEXT NOT NULL REFERENCES post_counters(post_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, post_id)
);
"""


class PostgresLikeStore:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._local = threading.local()

    def reset(self, post_ids: list[str]) -> None:
        import psycopg

        with psycopg.connect(self.dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)
                cur.executemany(
                    "INSERT INTO post_counters (post_id, like_count) VALUES (%s, 0)",
                    [(post_id,) for post_id in post_ids],
                )
                cur.execute("ANALYZE post_counters")
                cur.execute("ANALYZE likes")

    def like(self, post_id: str, user_id: str) -> WriteResult:
        started = time.perf_counter()
        conn = self._connection()
        return self.like_with_connection(conn, post_id, user_id, started)

    def like_with_connection(
        self,
        conn: "psycopg.Connection",
        post_id: str,
        user_id: str,
        started: float | None = None,
    ) -> WriteResult:
        started = time.perf_counter() if started is None else started
        with conn.transaction():
            inserted = conn.execute(
                """
                INSERT INTO likes (user_id, post_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (user_id, post_id),
            ).rowcount
            if inserted:
                conn.execute(
                    """
                    UPDATE post_counters
                    SET like_count = like_count + 1
                    WHERE post_id = %s
                    """,
                    (post_id,),
                )
        return WriteResult(
            ok=bool(inserted),
            post_id=post_id,
            user_id=user_id,
            elapsed_ms=(time.perf_counter() - started) * 1000,
        )

    def counts(self) -> dict[str, int]:
        import psycopg

        with psycopg.connect(self.dsn) as conn:
            rows = conn.execute("SELECT post_id, like_count FROM post_counters").fetchall()
        return {post_id: count for post_id, count in rows}

    def close_thread_connection(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def _connection(self) -> "psycopg.Connection":
        import psycopg

        conn = getattr(self._local, "conn", None)
        if conn is None or conn.closed:
            conn = psycopg.connect(self.dsn, autocommit=False)
            self._local.conn = conn
        return conn
