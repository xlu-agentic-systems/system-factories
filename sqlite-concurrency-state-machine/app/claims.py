from __future__ import annotations

import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from app.db import AVAILABLE, OFFERED, connect


@dataclass(frozen=True)
class ClaimResult:
    strategy: str
    worker_id: str
    offer_id: str
    success: bool
    reason: str
    observed_status: str | None
    observed_version: int | None
    elapsed_ms: float


def claim_unsafe_check_then_update(
    db_path: str | Path,
    driver_id: str,
    worker_id: str,
    delay_seconds: float = 0.02,
) -> ClaimResult:
    started = time.perf_counter()
    offer_id = _offer_id(worker_id)

    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT status, version FROM drivers WHERE driver_id = ?",
            (driver_id,),
        ).fetchone()
        if row is None:
            return _result("unsafe", worker_id, offer_id, False, "missing-driver", None, None, started)

        observed_status = row["status"]
        observed_version = row["version"]
        if observed_status != AVAILABLE:
            return _result(
                "unsafe",
                worker_id,
                offer_id,
                False,
                "already-claimed",
                observed_status,
                observed_version,
                started,
            )

        time.sleep(delay_seconds)
        conn.execute(
            """
            UPDATE drivers
            SET status = ?,
                current_offer_id = ?,
                version = version + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE driver_id = ?
            """,
            (OFFERED, offer_id, driver_id),
        )

    return _result("unsafe", worker_id, offer_id, True, "updated-after-stale-read", observed_status, observed_version, started)


def claim_in_transaction(
    db_path: str | Path,
    driver_id: str,
    worker_id: str,
    delay_seconds: float = 0.02,
) -> ClaimResult:
    started = time.perf_counter()
    offer_id = _offer_id(worker_id)
    conn = connect(db_path)

    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT status, version FROM drivers WHERE driver_id = ?",
            (driver_id,),
        ).fetchone()
        if row is None:
            conn.execute("ROLLBACK")
            return _result("transaction", worker_id, offer_id, False, "missing-driver", None, None, started)

        observed_status = row["status"]
        observed_version = row["version"]
        if observed_status != AVAILABLE:
            conn.execute("ROLLBACK")
            return _result(
                "transaction",
                worker_id,
                offer_id,
                False,
                "already-claimed",
                observed_status,
                observed_version,
                started,
            )

        time.sleep(delay_seconds)
        conn.execute(
            """
            UPDATE drivers
            SET status = ?,
                current_offer_id = ?,
                version = version + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE driver_id = ?
            """,
            (OFFERED, offer_id, driver_id),
        )
        conn.execute("COMMIT")
        return _result("transaction", worker_id, offer_id, True, "committed", observed_status, observed_version, started)
    except sqlite3.Error:
        _rollback_quietly(conn)
        raise
    finally:
        conn.close()


def claim_with_atomic_update(
    db_path: str | Path,
    driver_id: str,
    worker_id: str,
    delay_seconds: float = 0.02,
) -> ClaimResult:
    started = time.perf_counter()
    offer_id = _offer_id(worker_id)

    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT status, version FROM drivers WHERE driver_id = ?",
            (driver_id,),
        ).fetchone()
        if row is None:
            return _result("atomic", worker_id, offer_id, False, "missing-driver", None, None, started)

        observed_status = row["status"]
        observed_version = row["version"]
        time.sleep(delay_seconds)
        cursor = conn.execute(
            """
            UPDATE drivers
            SET status = ?,
                current_offer_id = ?,
                version = version + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE driver_id = ?
              AND status = ?
              AND version = ?
            """,
            (OFFERED, offer_id, driver_id, AVAILABLE, observed_version),
        )

    if cursor.rowcount == 1:
        return _result("atomic", worker_id, offer_id, True, "affected-one-row", observed_status, observed_version, started)
    return _result("atomic", worker_id, offer_id, False, "affected-zero-rows", observed_status, observed_version, started)


def _offer_id(worker_id: str) -> str:
    return f"offer-{worker_id}-{uuid.uuid4().hex[:8]}"


def _result(
    strategy: str,
    worker_id: str,
    offer_id: str,
    success: bool,
    reason: str,
    observed_status: str | None,
    observed_version: int | None,
    started: float,
) -> ClaimResult:
    return ClaimResult(
        strategy=strategy,
        worker_id=worker_id,
        offer_id=offer_id,
        success=success,
        reason=reason,
        observed_status=observed_status,
        observed_version=observed_version,
        elapsed_ms=(time.perf_counter() - started) * 1000,
    )


def _rollback_quietly(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass

