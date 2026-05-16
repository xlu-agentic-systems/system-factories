from __future__ import annotations

import sqlite3
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

from app.models import (
    CountDelta,
    TopKEntry,
    WINDOWS,
    WINDOW_1DAY,
    WINDOW_1HOUR,
    WINDOW_1MONTH,
    WINDOW_ALL_TIME,
)
from app.time_windows import bucket_end, bucket_start


AGGREGATE_TABLES = {
    WINDOW_1HOUR: "aggregate_1hour",
    WINDOW_1DAY: "aggregate_1day",
    WINDOW_1MONTH: "aggregate_1month",
    WINDOW_ALL_TIME: "aggregate_all_time",
}


class SQLiteTopKStorage:
    """Local stand-in for sharded counters plus materialized top-k tables."""

    def __init__(self, db_path: str | Path, k_limit: int = 1000) -> None:
        if k_limit <= 0 or k_limit > 1000:
            raise ValueError("k_limit must be between 1 and 1000")
        self.db_path = Path(db_path)
        self.k_limit = k_limit
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode = WAL;

                CREATE TABLE IF NOT EXISTS view_count_shards (
                    window TEXT NOT NULL,
                    bucket_start INTEGER NOT NULL,
                    video_id TEXT NOT NULL,
                    shard_id INTEGER NOT NULL,
                    view_count INTEGER NOT NULL,
                    PRIMARY KEY (window, bucket_start, video_id, shard_id)
                );

                CREATE TABLE IF NOT EXISTS aggregate_1hour (
                    bucket_start INTEGER NOT NULL,
                    video_id TEXT NOT NULL,
                    view_count INTEGER NOT NULL,
                    PRIMARY KEY (bucket_start, video_id)
                );

                CREATE TABLE IF NOT EXISTS aggregate_1day (
                    bucket_start INTEGER NOT NULL,
                    video_id TEXT NOT NULL,
                    view_count INTEGER NOT NULL,
                    PRIMARY KEY (bucket_start, video_id)
                );

                CREATE TABLE IF NOT EXISTS aggregate_1month (
                    bucket_start INTEGER NOT NULL,
                    video_id TEXT NOT NULL,
                    view_count INTEGER NOT NULL,
                    PRIMARY KEY (bucket_start, video_id)
                );

                CREATE TABLE IF NOT EXISTS aggregate_all_time (
                    bucket_start INTEGER NOT NULL,
                    video_id TEXT NOT NULL,
                    view_count INTEGER NOT NULL,
                    PRIMARY KEY (bucket_start, video_id)
                );

                CREATE TABLE IF NOT EXISTS topk_snapshots (
                    window TEXT NOT NULL,
                    bucket_start INTEGER NOT NULL,
                    rank INTEGER NOT NULL,
                    video_id TEXT NOT NULL,
                    view_count INTEGER NOT NULL,
                    PRIMARY KEY (window, bucket_start, rank)
                );

                CREATE TABLE IF NOT EXISTS native_hourly_counts (
                    hour_start INTEGER NOT NULL,
                    video_id TEXT NOT NULL,
                    view_count INTEGER NOT NULL,
                    PRIMARY KEY (hour_start, video_id)
                );

                CREATE INDEX IF NOT EXISTS idx_aggregate_1hour_topk
                    ON aggregate_1hour (bucket_start, view_count DESC, video_id);
                CREATE INDEX IF NOT EXISTS idx_aggregate_1day_topk
                    ON aggregate_1day (bucket_start, view_count DESC, video_id);
                CREATE INDEX IF NOT EXISTS idx_aggregate_1month_topk
                    ON aggregate_1month (bucket_start, view_count DESC, video_id);
                CREATE INDEX IF NOT EXISTS idx_aggregate_all_time_topk
                    ON aggregate_all_time (bucket_start, view_count DESC, video_id);
                CREATE INDEX IF NOT EXISTS idx_topk_lookup
                    ON topk_snapshots (window, bucket_start, rank);
                CREATE INDEX IF NOT EXISTS idx_native_hourly_range
                    ON native_hourly_counts (hour_start, video_id);
                CREATE INDEX IF NOT EXISTS idx_native_hourly_topk
                    ON native_hourly_counts (hour_start, view_count DESC, video_id);
                """
            )

    def apply_deltas(self, deltas: Iterable[CountDelta]) -> int:
        merged: defaultdict[tuple[str, int, str, int], int] = defaultdict(int)
        for delta in deltas:
            if delta.window not in WINDOWS:
                raise ValueError(f"unsupported window: {delta.window}")
            if delta.view_count <= 0:
                raise ValueError("view_count deltas must be positive")
            merged[(delta.window, delta.bucket_start, delta.video_id, delta.shard_id)] += delta.view_count

        if not merged:
            return 0

        affected: set[tuple[str, int]] = set()
        with self.connect() as conn:
            conn.execute("BEGIN")
            for (window, start, video_id, shard_id), count in merged.items():
                aggregate_table = self._aggregate_table(window)
                conn.execute(
                    """
                    INSERT INTO view_count_shards
                        (window, bucket_start, video_id, shard_id, view_count)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(window, bucket_start, video_id, shard_id)
                    DO UPDATE SET view_count = view_count + excluded.view_count
                    """,
                    (window, start, video_id, shard_id, count),
                )
                conn.execute(
                    f"""
                    INSERT INTO {aggregate_table}
                        (bucket_start, video_id, view_count)
                    VALUES (?, ?, ?)
                    ON CONFLICT(bucket_start, video_id)
                    DO UPDATE SET view_count = view_count + excluded.view_count
                    """,
                    (start, video_id, count),
                )
                affected.add((window, start))

            for window, start in affected:
                self._refresh_topk(conn, window, start)
            conn.commit()

        return len(merged)

    def apply_native_hourly_counts(self, rows: Iterable[tuple[int, str, int]]) -> int:
        merged: defaultdict[tuple[int, str], int] = defaultdict(int)
        for hour_start, video_id, view_count in rows:
            if view_count <= 0:
                raise ValueError("view_count rows must be positive")
            merged[(hour_start, video_id)] += view_count

        if not merged:
            return 0

        with self.connect() as conn:
            conn.executemany(
                """
                INSERT INTO native_hourly_counts
                    (hour_start, video_id, view_count)
                VALUES (?, ?, ?)
                ON CONFLICT(hour_start, video_id)
                DO UPDATE SET view_count = view_count + excluded.view_count
                """,
                [(hour_start, video_id, count) for (hour_start, video_id), count in merged.items()],
            )
        return len(merged)

    def apply_precomputed_hourly_counts(
        self,
        rows: Iterable[tuple[int, str, int]],
        shard_count: int = 20,
        refresh_snapshots: bool = True,
        write_shards: bool = True,
    ) -> set[tuple[str, int]]:
        if shard_count <= 0:
            raise ValueError("shard_count must be positive")

        affected: set[tuple[str, int]] = set()
        shard_rows: list[tuple[str, int, str, int, int]] = []
        aggregate_rows: dict[str, list[tuple[int, str, int]]] = {
            table: [] for table in AGGREGATE_TABLES.values()
        }
        for hour_start, video_id, view_count in rows:
            if view_count <= 0:
                raise ValueError("view_count rows must be positive")
            if write_shards:
                shard_base = view_count // shard_count
                shard_remainder = view_count % shard_count
                for shard_id in range(shard_count):
                    shard_count_value = shard_base + (1 if shard_id < shard_remainder else 0)
                    if shard_count_value:
                        shard_rows.append(
                            (WINDOW_1HOUR, hour_start, video_id, shard_id, shard_count_value)
                        )

            bucket_counts = (
                (WINDOW_1HOUR, hour_start),
                (WINDOW_1DAY, bucket_start(WINDOW_1DAY, hour_start)),
                (WINDOW_1MONTH, bucket_start(WINDOW_1MONTH, hour_start)),
                (WINDOW_ALL_TIME, 0),
            )
            for window, aggregate_start in bucket_counts:
                aggregate_table = self._aggregate_table(window)
                aggregate_rows[aggregate_table].append((aggregate_start, video_id, view_count))
                affected.add((window, aggregate_start))

        if not shard_rows and not any(aggregate_rows.values()):
            return affected

        with self.connect() as conn:
            conn.execute("BEGIN")
            conn.executemany(
                """
                INSERT INTO view_count_shards
                    (window, bucket_start, video_id, shard_id, view_count)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(window, bucket_start, video_id, shard_id)
                DO UPDATE SET view_count = view_count + excluded.view_count
                """,
                shard_rows,
            )
            for aggregate_table, table_rows in aggregate_rows.items():
                if not table_rows:
                    continue
                conn.executemany(
                    f"""
                    INSERT INTO {aggregate_table}
                        (bucket_start, video_id, view_count)
                    VALUES (?, ?, ?)
                    ON CONFLICT(bucket_start, video_id)
                    DO UPDATE SET view_count = view_count + excluded.view_count
                    """,
                    table_rows,
                )

            if refresh_snapshots:
                for window, start in affected:
                    self._refresh_topk(conn, window, start)
            conn.commit()
        return affected

    def refresh_topk_snapshots(self, buckets: Iterable[tuple[str, int]]) -> int:
        unique_buckets = set(buckets)
        if not unique_buckets:
            return 0
        with self.connect() as conn:
            conn.execute("BEGIN")
            for window, start in unique_buckets:
                self._refresh_topk(conn, window, start)
            conn.commit()
        return len(unique_buckets)

    def _aggregate_table(self, window: str) -> str:
        try:
            return AGGREGATE_TABLES[window]
        except KeyError as exc:
            raise ValueError(f"unsupported window: {window}") from exc

    def _refresh_topk(self, conn: sqlite3.Connection, window: str, bucket_start: int) -> None:
        aggregate_table = self._aggregate_table(window)
        conn.execute(
            "DELETE FROM topk_snapshots WHERE window = ? AND bucket_start = ?",
            (window, bucket_start),
        )
        rows = conn.execute(
            """
            SELECT video_id, view_count
            FROM {aggregate_table}
            WHERE bucket_start = ?
            ORDER BY view_count DESC, video_id ASC
            LIMIT ?
            """.format(aggregate_table=aggregate_table),
            (bucket_start, self.k_limit),
        ).fetchall()
        conn.executemany(
            """
            INSERT INTO topk_snapshots
                (window, bucket_start, rank, video_id, view_count)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (window, bucket_start, rank, row["video_id"], row["view_count"])
                for rank, row in enumerate(rows, start=1)
            ],
        )

    def topk(self, window: str, bucket_start: int, k: int) -> list[TopKEntry]:
        if k <= 0 or k > self.k_limit:
            raise ValueError(f"k must be between 1 and configured k_limit={self.k_limit}")
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT rank, video_id, view_count
                FROM topk_snapshots
                WHERE window = ? AND bucket_start = ? AND rank <= ?
                ORDER BY rank ASC
                """,
                (window, bucket_start, k),
            ).fetchall()
        return [
            TopKEntry(
                rank=row["rank"],
                video_id=row["video_id"],
                view_count=row["view_count"],
                window=window,
                bucket_start=bucket_start,
            )
            for row in rows
        ]

    def native_topk(self, window: str, start: int, k: int) -> list[TopKEntry]:
        if k <= 0 or k > self.k_limit:
            raise ValueError(f"k must be between 1 and configured k_limit={self.k_limit}")
        if window not in WINDOWS:
            raise ValueError(f"unsupported window: {window}")

        end = bucket_end(window, start)
        with self.connect() as conn:
            if window == WINDOW_1HOUR:
                rows = conn.execute(
                    """
                    SELECT video_id, view_count
                    FROM native_hourly_counts
                    WHERE hour_start = ?
                    ORDER BY view_count DESC, video_id ASC
                    LIMIT ?
                    """,
                    (start, k),
                ).fetchall()
            elif end is None:
                rows = conn.execute(
                    """
                    SELECT video_id, SUM(view_count) AS view_count
                    FROM native_hourly_counts
                    GROUP BY video_id
                    ORDER BY view_count DESC, video_id ASC
                    LIMIT ?
                    """,
                    (k,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT video_id, SUM(view_count) AS view_count
                    FROM native_hourly_counts
                    WHERE hour_start >= ? AND hour_start < ?
                    GROUP BY video_id
                    ORDER BY view_count DESC, video_id ASC
                    LIMIT ?
                    """,
                    (start, end, k),
                ).fetchall()

        return [
            TopKEntry(
                rank=rank,
                video_id=row["video_id"],
                view_count=int(row["view_count"]),
                window=window,
                bucket_start=start,
            )
            for rank, row in enumerate(rows, start=1)
        ]

    def video_total(self, window: str, bucket_start: int, video_id: str) -> int:
        aggregate_table = self._aggregate_table(window)
        with self.connect() as conn:
            row = conn.execute(
                f"""
                SELECT view_count
                FROM {aggregate_table}
                WHERE bucket_start = ? AND video_id = ?
                """,
                (bucket_start, video_id),
            ).fetchone()
        return 0 if row is None else int(row["view_count"])

    def shard_counts(self, window: str, bucket_start: int, video_id: str) -> dict[int, int]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT shard_id, view_count
                FROM view_count_shards
                WHERE window = ? AND bucket_start = ? AND video_id = ?
                ORDER BY shard_id ASC
                """,
                (window, bucket_start, video_id),
            ).fetchall()
        return {int(row["shard_id"]): int(row["view_count"]) for row in rows}

    def row_count(self, table: str) -> int:
        allowed_tables = {
            "view_count_shards",
            "native_hourly_counts",
            "topk_snapshots",
            *AGGREGATE_TABLES.values(),
        }
        if table not in allowed_tables:
            raise ValueError("unsupported table")
        with self.connect() as conn:
            row = conn.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()
        return int(row["count"])

    def aggregate_tables(self) -> dict[str, str]:
        return dict(AGGREGATE_TABLES)
