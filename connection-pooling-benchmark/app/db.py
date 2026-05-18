from __future__ import annotations

import asyncpg


CREATE_SQL = """
CREATE TABLE IF NOT EXISTS benchmark_items (
    id INTEGER PRIMARY KEY,
    payload TEXT NOT NULL
);
"""


async def setup_database(dsn: str, rows: int = 10_000) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(CREATE_SQL)
        await conn.executemany(
            """
            INSERT INTO benchmark_items (id, payload)
            VALUES ($1, $2)
            ON CONFLICT (id) DO UPDATE SET payload = excluded.payload
            """,
            [(i, f"payload-{i}") for i in range(1, rows + 1)],
        )
    finally:
        await conn.close()

