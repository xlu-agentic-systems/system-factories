from __future__ import annotations

import json
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable

from app.lease_backends import (
    DbTtlLeaseBackend,
    InMemoryRedisTtlStore,
    Lease,
    LocalAppLockBackend,
    RedisServerTtlStore,
    RedisTtlLeaseBackend,
    SqliteRedisTtlStore,
)

LEASE_STRATEGIES = {"app-lock", "db-ttl", "redis-ttl"}


@dataclass(frozen=True)
class LeaseAttempt:
    strategy: str
    mode: str
    worker_id: str
    acquired: bool
    token: str | None
    elapsed_ms: float
    reason: str


@dataclass(frozen=True)
class LeaseContentionResult:
    strategy: str
    mode: str
    workers: int
    winners: int
    duplicate_winners: bool
    elapsed_ms: float
    attempts: list[LeaseAttempt]


@dataclass(frozen=True)
class CrashRecoveryResult:
    strategy: str
    ttl_seconds: float
    first_acquired: bool
    before_ttl_acquired: bool
    after_ttl_acquired: bool
    recovered_after_ttl: bool
    restart_loses_state: bool


def run_lease_contention(
    base_path: str | Path,
    strategy: str,
    workers: int = 32,
    mode: str = "threads",
    ttl_seconds: float = 15.0,
    resource_id: str | None = None,
    redis_url: str | None = None,
) -> LeaseContentionResult:
    _validate(strategy, mode)
    base = Path(base_path)
    resource = resource_id or f"driver-{time.time_ns()}"
    start_at = time.monotonic() + 0.2
    started = time.perf_counter()

    if mode == "threads":
        backend = _make_thread_backend(base, strategy, redis_url)
        args = [
            (backend, strategy, mode, resource, f"{mode}-{index:03d}", ttl_seconds, start_at)
            for index in range(workers)
        ]
        with ThreadPoolExecutor(max_workers=workers) as executor:
            attempts = list(executor.map(_attempt_thread_lease, args))
    else:
        _prepare_process_backend(base, strategy, redis_url)
        args = [
            (str(base), strategy, mode, resource, f"{mode}-{index:03d}", ttl_seconds, start_at, redis_url)
            for index in range(workers)
        ]
        attempts = _attempt_process_leases(args)

    winners = sum(1 for attempt in attempts if attempt.acquired)
    return LeaseContentionResult(
        strategy=strategy,
        mode=mode,
        workers=workers,
        winners=winners,
        duplicate_winners=winners > 1,
        elapsed_ms=(time.perf_counter() - started) * 1000,
        attempts=attempts,
    )


def run_crash_recovery(
    base_path: str | Path,
    strategy: str,
    ttl_seconds: float = 15.0,
    resource_id: str | None = None,
    redis_url: str | None = None,
) -> CrashRecoveryResult:
    if strategy not in LEASE_STRATEGIES:
        raise ValueError(f"unknown strategy {strategy!r}; expected one of {sorted(LEASE_STRATEGIES)}")

    resource = resource_id or f"driver-{time.time_ns()}"
    backend = _make_thread_backend(Path(base_path), strategy, redis_url)
    first = backend.acquire(resource, "owner-crashes", ttl_seconds)
    before_ttl = backend.acquire(resource, "owner-before-ttl", ttl_seconds)
    time.sleep(ttl_seconds + min(0.05, max(ttl_seconds / 4, 0.001)))
    after_ttl = backend.acquire(resource, "owner-after-ttl", ttl_seconds)

    restart_loses_state = False
    if strategy == "app-lock":
        restarted_backend = LocalAppLockBackend()
        restart_loses_state = restarted_backend.acquire(resource, "owner-after-process-restart", ttl_seconds) is not None

    return CrashRecoveryResult(
        strategy=strategy,
        ttl_seconds=ttl_seconds,
        first_acquired=first is not None,
        before_ttl_acquired=before_ttl is not None,
        after_ttl_acquired=after_ttl is not None,
        recovered_after_ttl=after_ttl is not None,
        restart_loses_state=restart_loses_state,
    )


def summarize_contention(results: Iterable[LeaseContentionResult]) -> str:
    lines = [
        "strategy    mode        workers  winners  duplicate_winners  elapsed_ms",
        "----------  ----------  -------  -------  -----------------  ----------",
    ]
    for result in results:
        lines.append(
            f"{result.strategy:<10}  "
            f"{result.mode:<10}  "
            f"{result.workers:>7}  "
            f"{result.winners:>7}  "
            f"{str(result.duplicate_winners):<17}  "
            f"{result.elapsed_ms:>10.1f}"
        )
    return "\n".join(lines)


def summarize_crash(results: Iterable[CrashRecoveryResult]) -> str:
    lines = [
        "strategy    first  before_ttl  after_ttl  recovered  restart_loses_state",
        "----------  -----  ----------  ---------  ---------  -------------------",
    ]
    for result in results:
        lines.append(
            f"{result.strategy:<10}  "
            f"{str(result.first_acquired):<5}  "
            f"{str(result.before_ttl_acquired):<10}  "
            f"{str(result.after_ttl_acquired):<9}  "
            f"{str(result.recovered_after_ttl):<9}  "
            f"{str(result.restart_loses_state):<19}"
        )
    return "\n".join(lines)


def attempt_once(
    base_path: str | Path,
    strategy: str,
    mode: str,
    resource_id: str,
    worker_id: str,
    ttl_seconds: float,
    redis_url: str | None = None,
) -> LeaseAttempt:
    backend = _make_process_backend(Path(base_path), strategy, redis_url)
    started = time.perf_counter()
    lease = backend.acquire(resource_id, worker_id, ttl_seconds)
    return LeaseAttempt(
        strategy=strategy,
        mode=mode,
        worker_id=worker_id,
        acquired=lease is not None,
        token=lease.token if lease is not None else None,
        elapsed_ms=(time.perf_counter() - started) * 1000,
        reason="acquired" if lease is not None else "busy",
    )


def _attempt_thread_lease(args: tuple[object, str, str, str, str, float, float]) -> LeaseAttempt:
    backend, strategy, mode, resource_id, worker_id, ttl_seconds, start_at = args
    while time.monotonic() < start_at:
        time.sleep(0.001)
    started = time.perf_counter()
    lease = backend.acquire(resource_id, worker_id, ttl_seconds)
    return LeaseAttempt(
        strategy=strategy,
        mode=mode,
        worker_id=worker_id,
        acquired=lease is not None,
        token=_lease_token(lease),
        elapsed_ms=(time.perf_counter() - started) * 1000,
        reason="acquired" if lease is not None else "busy",
    )


def _attempt_process_leases(args: list[tuple[str, str, str, str, str, float, float, str | None]]) -> list[LeaseAttempt]:
    project_root = Path(__file__).resolve().parents[1]
    processes = []
    for base_path, strategy, mode, resource_id, worker_id, ttl_seconds, start_at, redis_url in args:
        command = [
            sys.executable,
            "-m",
            "app.worker_lease",
            "--base-path",
            base_path,
            "--strategy",
            strategy,
            "--mode",
            mode,
            "--resource-id",
            resource_id,
            "--worker-id",
            worker_id,
            "--ttl",
            str(ttl_seconds),
            "--start-at",
            str(start_at),
        ]
        if redis_url is not None:
            command.extend(["--redis-url", redis_url])
        processes.append(
            subprocess.Popen(
                command,
                cwd=project_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        )

    attempts: list[LeaseAttempt] = []
    for process in processes:
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            raise RuntimeError(f"lease worker failed with exit {process.returncode}: {stderr.strip()}")
        attempts.append(LeaseAttempt(**json.loads(stdout)))
    return attempts


def _make_thread_backend(base_path: Path, strategy: str, redis_url: str | None = None):
    if strategy == "app-lock":
        return LocalAppLockBackend()
    if strategy == "db-ttl":
        return DbTtlLeaseBackend(base_path / "db-ttl-leases.sqlite3")
    if strategy == "redis-ttl":
        if redis_url is not None:
            return RedisTtlLeaseBackend(RedisServerTtlStore(redis_url))
        return RedisTtlLeaseBackend(InMemoryRedisTtlStore())
    raise ValueError(f"unknown strategy {strategy!r}")


def _make_process_backend(base_path: Path, strategy: str, redis_url: str | None = None):
    if strategy == "app-lock":
        return LocalAppLockBackend()
    if strategy == "db-ttl":
        return DbTtlLeaseBackend(base_path / "db-ttl-leases.sqlite3")
    if strategy == "redis-ttl":
        if redis_url is not None:
            return RedisTtlLeaseBackend(RedisServerTtlStore(redis_url))
        return RedisTtlLeaseBackend(SqliteRedisTtlStore(base_path / "redis-ttl-standin.sqlite3"))
    raise ValueError(f"unknown strategy {strategy!r}")


def _prepare_process_backend(base_path: Path, strategy: str, redis_url: str | None = None) -> None:
    if strategy == "app-lock" or redis_url is not None:
        return
    _make_process_backend(base_path, strategy, redis_url)


def _validate(strategy: str, mode: str) -> None:
    if strategy not in LEASE_STRATEGIES:
        raise ValueError(f"unknown strategy {strategy!r}; expected one of {sorted(LEASE_STRATEGIES)}")
    if mode not in {"threads", "processes"}:
        raise ValueError("mode must be 'threads' or 'processes'")


def _lease_token(lease: Lease | None) -> str | None:
    return lease.token if lease is not None else None


def to_json(result: LeaseAttempt) -> str:
    return json.dumps(asdict(result))
