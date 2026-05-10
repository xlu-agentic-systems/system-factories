from __future__ import annotations

import sqlite3
import time
from collections.abc import Iterable
from pathlib import Path

from app.models import ClickEvent, MetricDelta, MetricPoint


class SQLiteClickStorage:
    """Durable raw click store, stream cursor store, and derived metric store."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        if self.db_path.parent:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS seen_impressions (
                    impression_id TEXT PRIMARY KEY,
                    advertiser_id TEXT NOT NULL,
                    ad_id TEXT NOT NULL,
                    first_event_id TEXT NOT NULL,
                    first_seen_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS raw_click_events (
                    raw_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    advertiser_id TEXT NOT NULL,
                    ad_id TEXT NOT NULL,
                    impression_id TEXT NOT NULL,
                    user_id TEXT,
                    occurred_at INTEGER NOT NULL,
                    minute_start INTEGER NOT NULL,
                    target_url TEXT NOT NULL,
                    signature TEXT NOT NULL,
                    source_ip TEXT,
                    user_agent TEXT,
                    received_at INTEGER NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_raw_click_events_time
                    ON raw_click_events(occurred_at);
                CREATE INDEX IF NOT EXISTS idx_raw_click_events_ad_minute
                    ON raw_click_events(advertiser_id, ad_id, minute_start);

                CREATE TABLE IF NOT EXISTS stream_cursors (
                    name TEXT PRIMARY KEY,
                    last_raw_event_id INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS derived_click_metrics (
                    advertiser_id TEXT NOT NULL,
                    ad_id TEXT NOT NULL,
                    minute_start INTEGER NOT NULL,
                    click_count INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    PRIMARY KEY (advertiser_id, ad_id, minute_start)
                );

                CREATE INDEX IF NOT EXISTS idx_derived_click_metrics_adv_time
                    ON derived_click_metrics(advertiser_id, minute_start);
                """
            )

    def record_click(self, event: ClickEvent) -> tuple[bool, int | None]:
        """Persist one click if its impression has not already been counted."""
        now = int(time.time())
        with self.connect() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO seen_impressions (
                        impression_id, advertiser_id, ad_id, first_event_id, first_seen_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        event.impression_id,
                        event.advertiser_id,
                        event.ad_id,
                        event.event_id,
                        now,
                    ),
                )
            except sqlite3.IntegrityError:
                return False, None

            cursor = conn.execute(
                """
                INSERT INTO raw_click_events (
                    event_id, advertiser_id, ad_id, impression_id, user_id,
                    occurred_at, minute_start, target_url, signature, source_ip,
                    user_agent, received_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.event_id,
                    event.advertiser_id,
                    event.ad_id,
                    event.impression_id,
                    event.user_id,
                    event.occurred_at,
                    event.minute_start,
                    event.target_url,
                    event.signature,
                    event.source_ip,
                    event.user_agent,
                    event.received_at,
                ),
            )
            return True, int(cursor.lastrowid)

    def get_cursor(self, name: str) -> int:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT last_raw_event_id FROM stream_cursors WHERE name = ?",
                (name,),
            ).fetchone()
        return int(row["last_raw_event_id"]) if row else 0

    def fetch_raw_after(self, last_raw_event_id: int, limit: int) -> list[ClickEvent]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM raw_click_events
                WHERE raw_event_id > ?
                ORDER BY raw_event_id ASC
                LIMIT ?
                """,
                (last_raw_event_id, limit),
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def commit_deltas_and_cursor(
        self,
        *,
        cursor_name: str,
        deltas: Iterable[MetricDelta],
        last_raw_event_id: int,
    ) -> None:
        now = int(time.time())
        with self.connect() as conn:
            for delta in deltas:
                conn.execute(
                    """
                    INSERT INTO derived_click_metrics (
                        advertiser_id, ad_id, minute_start, click_count, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(advertiser_id, ad_id, minute_start) DO UPDATE SET
                        click_count = click_count + excluded.click_count,
                        updated_at = excluded.updated_at
                    """,
                    (
                        delta.advertiser_id,
                        delta.ad_id,
                        delta.minute_start,
                        delta.click_count,
                        now,
                    ),
                )
            conn.execute(
                """
                INSERT INTO stream_cursors (name, last_raw_event_id)
                VALUES (?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    last_raw_event_id = excluded.last_raw_event_id
                """,
                (cursor_name, last_raw_event_id),
            )

    def query_metrics(
        self,
        *,
        advertiser_id: str,
        start_time: int,
        end_time: int,
        ad_ids: list[str] | None = None,
        granularity_seconds: int = 60,
    ) -> list[MetricPoint]:
        if granularity_seconds < 60 or granularity_seconds % 60 != 0:
            raise ValueError("granularity_seconds must be a multiple of 60")

        start_minute = start_time - (start_time % 60)
        end_remainder = end_time % 60
        end_minute = end_time if end_remainder == 0 else end_time + (60 - end_remainder)
        params: list[object] = [granularity_seconds, advertiser_id, start_minute, end_minute]
        ad_filter = ""
        if ad_ids:
            placeholders = ", ".join("?" for _ in ad_ids)
            ad_filter = f" AND ad_id IN ({placeholders})"
            params.extend(ad_ids)

        with self.connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    advertiser_id,
                    ad_id,
                    minute_start - (minute_start % ?) AS bucket_start,
                    SUM(click_count) AS click_count
                FROM derived_click_metrics
                WHERE advertiser_id = ?
                  AND minute_start >= ?
                  AND minute_start < ?
                  {ad_filter}
                GROUP BY advertiser_id, ad_id, bucket_start
                ORDER BY bucket_start ASC, ad_id ASC
                """,
                params,
            ).fetchall()

        return [
            MetricPoint(
                advertiser_id=row["advertiser_id"],
                ad_id=row["ad_id"],
                bucket_start=int(row["bucket_start"]),
                click_count=int(row["click_count"]),
            )
            for row in rows
        ]

    def rebuild_derived_metrics(self, *, start_time: int, end_time: int) -> int:
        """Reconciliation job: rebuild derived minute metrics from raw events."""
        start_minute = start_time - (start_time % 60)
        end_minute = end_time - (end_time % 60)
        now = int(time.time())
        with self.connect() as conn:
            conn.execute(
                """
                DELETE FROM derived_click_metrics
                WHERE minute_start >= ? AND minute_start < ?
                """,
                (start_minute, end_minute),
            )
            cursor = conn.execute(
                """
                INSERT INTO derived_click_metrics (
                    advertiser_id, ad_id, minute_start, click_count, updated_at
                )
                SELECT advertiser_id, ad_id, minute_start, COUNT(*), ?
                FROM raw_click_events
                WHERE minute_start >= ? AND minute_start < ?
                GROUP BY advertiser_id, ad_id, minute_start
                """,
                (now, start_minute, end_minute),
            )
            return int(cursor.rowcount)

    def raw_count(self) -> int:
        with self.connect() as conn:
            row = conn.execute("SELECT COUNT(*) AS count FROM raw_click_events").fetchone()
        return int(row["count"])

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> ClickEvent:
        return ClickEvent(
            raw_event_id=int(row["raw_event_id"]),
            event_id=row["event_id"],
            advertiser_id=row["advertiser_id"],
            ad_id=row["ad_id"],
            impression_id=row["impression_id"],
            user_id=row["user_id"],
            occurred_at=int(row["occurred_at"]),
            target_url=row["target_url"],
            signature=row["signature"],
            source_ip=row["source_ip"],
            user_agent=row["user_agent"],
            received_at=int(row["received_at"]),
        )
