from __future__ import annotations

import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

from app.claims import (
    ClaimResult,
    claim_in_transaction,
    claim_unsafe_check_then_update,
    claim_with_atomic_update,
)
from app.db import get_driver, reset_driver

ClaimFn = Callable[[str | Path, str, str, float], ClaimResult]

STRATEGIES: dict[str, ClaimFn] = {
    "unsafe": claim_unsafe_check_then_update,
    "transaction": claim_in_transaction,
    "atomic": claim_with_atomic_update,
}


@dataclass(frozen=True)
class RaceResult:
    strategy: str
    mode: str
    workers: int
    successes: int
    failures: int
    elapsed_ms: float
    final_driver: dict[str, object]
    results: list[ClaimResult]


def run_race(
    db_path: str | Path,
    strategy: str,
    workers: int = 32,
    mode: str = "threads",
    driver_id: str = "driver-1",
    delay_seconds: float = 0.02,
) -> RaceResult:
    if strategy not in STRATEGIES:
        raise ValueError(f"unknown strategy {strategy!r}; expected one of {sorted(STRATEGIES)}")
    if mode not in {"threads", "processes"}:
        raise ValueError("mode must be 'threads' or 'processes'")

    reset_driver(db_path, driver_id=driver_id)
    started = time.perf_counter()
    start_at = time.monotonic() + 0.2
    args = [
        (strategy, str(db_path), driver_id, f"{mode}-{index:03d}", delay_seconds, start_at)
        for index in range(workers)
    ]
    if mode == "threads":
        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(_run_one_claim, args))
    else:
        results = _run_process_claims(args)

    successes = sum(1 for result in results if result.success)
    final_driver = get_driver(db_path, driver_id=driver_id)
    return RaceResult(
        strategy=strategy,
        mode=mode,
        workers=workers,
        successes=successes,
        failures=workers - successes,
        elapsed_ms=(time.perf_counter() - started) * 1000,
        final_driver=final_driver,
        results=results,
    )


def summarize(results: Iterable[RaceResult]) -> str:
    lines = [
        "strategy      mode        workers  successes  failures  final_status  version  elapsed_ms",
        "------------  ----------  -------  ---------  --------  ------------  -------  ----------",
    ]
    for result in results:
        lines.append(
            f"{result.strategy:<12}  "
            f"{result.mode:<10}  "
            f"{result.workers:>7}  "
            f"{result.successes:>9}  "
            f"{result.failures:>8}  "
            f"{str(result.final_driver['status']):<12}  "
            f"{int(result.final_driver['version']):>7}  "
            f"{result.elapsed_ms:>10.1f}"
        )
    return "\n".join(lines)


def _run_one_claim(args: tuple[str, str, str, str, float, float]) -> ClaimResult:
    strategy, db_path, driver_id, worker_id, delay_seconds, start_at = args
    while time.monotonic() < start_at:
        time.sleep(0.001)
    return STRATEGIES[strategy](db_path, driver_id, worker_id, delay_seconds)


def _run_process_claims(args: list[tuple[str, str, str, str, float, float]]) -> list[ClaimResult]:
    project_root = Path(__file__).resolve().parents[1]
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "app.worker_claim",
                "--strategy",
                strategy,
                "--db",
                db_path,
                "--driver-id",
                driver_id,
                "--worker-id",
                worker_id,
                "--delay",
                str(delay_seconds),
                "--start-at",
                str(start_at),
            ],
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for strategy, db_path, driver_id, worker_id, delay_seconds, start_at in args
    ]

    results: list[ClaimResult] = []
    for process in processes:
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            raise RuntimeError(f"process worker failed with exit {process.returncode}: {stderr.strip()}")
        payload = json.loads(stdout)
        results.append(ClaimResult(**payload))
    return results

