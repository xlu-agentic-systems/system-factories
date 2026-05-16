from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from time import perf_counter

from app.encoder import encoder_for


TABLES = {
    "base62": "url_mappings_base62",
    "base36": "url_mappings_base36",
}


@dataclass(frozen=True)
class BatchInsertResult:
    input_urls: int
    inserted: int
    collisions: int
    retry_successes: int
    failures: int
    elapsed_seconds: float


class PostgresUrlStore:
    def __init__(self, dsn: str) -> None:
        try:
            import psycopg
        except ImportError as exc:
            raise RuntimeError("PostgreSQL support requires psycopg. Install requirements.txt") from exc
        self.psycopg = psycopg
        self.dsn = dsn

    def connect(self):
        return self.psycopg.connect(self.dsn)

    def init_schema(self, reset: bool = False) -> None:
        with self.connect() as conn:
            if reset:
                conn.execute("DROP TABLE IF EXISTS collision_failures")
                for table in TABLES.values():
                    conn.execute(f"DROP TABLE IF EXISTS {table}")
            for table in TABLES.values():
                conn.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        short_url VARCHAR(8) PRIMARY KEY,
                        long_url TEXT NOT NULL UNIQUE
                    )
                    """
                )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS collision_failures (
                    method TEXT NOT NULL,
                    long_url TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    last_short_url VARCHAR(8) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """
            )

    def count_rows(self, method: str) -> int:
        table = self._table(method)
        with self.connect() as conn:
            return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def insert_urls_with_retries(
        self,
        urls: Iterable[str],
        method: str,
        batch_size: int = 50_000,
        max_retries: int = 3,
    ) -> BatchInsertResult:
        from app.generator import batched

        table = self._table(method)
        encoder = encoder_for(method)
        started = perf_counter()
        input_urls = 0
        inserted_total = 0
        collisions = 0
        retry_successes = 0
        failures = 0

        with self.connect() as conn:
            conn.execute(
                """
                CREATE TEMP TABLE IF NOT EXISTS stage_url_mappings (
                    short_url VARCHAR(8) NOT NULL,
                    long_url TEXT NOT NULL
                ) ON COMMIT PRESERVE ROWS
                """
            )
            for batch in batched(urls, batch_size):
                input_urls += len(batch)
                pending = list(batch)
                for attempt in range(max_retries + 1):
                    encoded_rows = [
                        self._encoded_row(encoder, url, attempt)
                        for url in pending
                    ]
                    conn.execute("TRUNCATE stage_url_mappings")
                    with conn.cursor() as cur:
                        with cur.copy(
                            "COPY stage_url_mappings (short_url, long_url) FROM STDIN"
                        ) as copy:
                            for row in encoded_rows:
                                copy.write_row(row)

                    inserted_count = int(conn.execute(
                        f"""
                        WITH inserted AS (
                        INSERT INTO {table} (short_url, long_url)
                        SELECT short_url, long_url
                        FROM stage_url_mappings
                        ON CONFLICT DO NOTHING
                            RETURNING 1
                        )
                        SELECT COUNT(*) FROM inserted
                        """
                    ).fetchone()[0])
                    inserted_total += inserted_count
                    if attempt:
                        retry_successes += inserted_count

                    collided_rows = conn.execute(
                        f"""
                        SELECT s.long_url
                        FROM stage_url_mappings s
                        JOIN {table} m ON m.short_url = s.short_url
                        WHERE m.long_url <> s.long_url
                        """
                    ).fetchall()
                    collisions += len(collided_rows)
                    pending = [row[0] for row in collided_rows]
                    if not pending:
                        break
                else:
                    failures += len(pending)
                    failure_rows = [
                        (
                            method,
                            encoder(url, max_retries).canonical_url,
                            max_retries + 1,
                            encoder(url, max_retries).short_url,
                        )
                        for url in pending
                    ]
                    conn.executemany(
                        """
                        INSERT INTO collision_failures
                            (method, long_url, attempts, last_short_url)
                        VALUES (%s, %s, %s, %s)
                        """,
                        failure_rows,
                    )
                conn.commit()

        inserted_after = self.count_rows(method)
        return BatchInsertResult(
            input_urls=input_urls,
            inserted=inserted_total,
            collisions=collisions,
            retry_successes=retry_successes,
            failures=failures,
            elapsed_seconds=perf_counter() - started,
        )

    def _table(self, method: str) -> str:
        try:
            return TABLES[method]
        except KeyError as exc:
            raise ValueError(f"unsupported method: {method}") from exc

    @staticmethod
    def _encoded_row(encoder, url: str, attempt: int) -> tuple[str, str]:
        result = encoder(url, attempt)
        return result.short_url, result.canonical_url
