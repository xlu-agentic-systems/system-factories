from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

AVAILABLE = "AVAILABLE"
OFFERED = "OFFERED"
BUSY = "BUSY"


def connect(db_path: str | Path, timeout: float = 10.0) -> sqlite3.Connection:
    conn = sqlite3.connect(
        str(db_path),
        timeout=timeout,
        isolation_level=None,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 10000")
    return conn


def init_db(db_path: str | Path) -> None:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with connect(path) as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS drivers (
                driver_id TEXT PRIMARY KEY,
                status TEXT NOT NULL CHECK (status IN ('AVAILABLE', 'OFFERED', 'BUSY')),
                current_offer_id TEXT,
                version INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            """
        )


def reset_driver(
    db_path: str | Path,
    driver_id: str = "driver-1",
    status: str = AVAILABLE,
) -> None:
    init_db(db_path)
    with connect(db_path) as conn:
        conn.execute("DELETE FROM drivers")
        conn.execute(
            """
            INSERT INTO drivers (driver_id, status, current_offer_id, version)
            VALUES (?, ?, NULL, 0)
            """,
            (driver_id, status),
        )


def get_driver(db_path: str | Path, driver_id: str = "driver-1") -> dict[str, Any]:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT driver_id, status, current_offer_id, version, updated_at
            FROM drivers
            WHERE driver_id = ?
            """,
            (driver_id,),
        ).fetchone()
    if row is None:
        raise LookupError(f"driver not found: {driver_id}")
    return dict(row)

