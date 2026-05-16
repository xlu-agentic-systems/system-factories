from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.models import QueueSettings
from app.service import WaitingQueueService
from app.store import InMemoryQueueStore, RedisQueueStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Virtual waiting queue load test")
    parser.add_argument("--users", type=int, default=50_000)
    parser.add_argument("--join-concurrency", type=int, default=128)
    parser.add_argument("--admit-batch-size", type=int, default=1_000)
    parser.add_argument("--backend", choices=["memory", "redis"], default="memory")
    parser.add_argument("--event-id", default=None)
    return parser.parse_args()


def rate(count: int, seconds: float) -> float:
    return count / seconds if seconds > 0 else 0.0


def build_service(backend: str) -> WaitingQueueService:
    if backend == "redis":
        return WaitingQueueService(RedisQueueStore.from_settings())
    return WaitingQueueService(InMemoryQueueStore())


def run_joins(service: WaitingQueueService, event_id: str, users: int, concurrency: int) -> float:
    start = time.perf_counter()

    def join_one(index: int) -> None:
        service.join(event_id, f"session_{index:08d}")

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(join_one, index) for index in range(users)]
        for completed, future in enumerate(as_completed(futures), start=1):
            future.result()
            if completed % 10_000 == 0:
                elapsed = time.perf_counter() - start
                print(
                    f"joined={completed} elapsed={elapsed:.2f}s rate={rate(completed, elapsed):.2f}/sec",
                    flush=True,
                )

    return time.perf_counter() - start


def run_admissions(service: WaitingQueueService, event_id: str, batch_size: int) -> tuple[int, float]:
    admitted = 0
    start = time.perf_counter()
    while True:
        result = service.admit_next(event_id, limit=batch_size)
        admitted += result.admitted_count
        if result.admitted_count == 0:
            break
        if admitted % 10_000 == 0:
            elapsed = time.perf_counter() - start
            print(
                f"admitted={admitted} elapsed={elapsed:.2f}s rate={rate(admitted, elapsed):.2f}/sec",
                flush=True,
            )
    return admitted, time.perf_counter() - start


def run_reservation_checks(service: WaitingQueueService, event_id: str, users: int) -> tuple[int, float]:
    start = time.perf_counter()
    accepted = 0
    for index in range(users):
        if service.can_reserve(event_id, f"session_{index:08d}"):
            accepted += 1
    return accepted, time.perf_counter() - start


def main() -> None:
    args = parse_args()
    event_id = args.event_id or f"load_{uuid4().hex}"
    service = build_service(args.backend)
    service.configure_event(
        event_id,
        QueueSettings(enabled=True, admission_ttl_seconds=3600, default_admit_limit=args.admit_batch_size),
    )

    print(
        f"load_test backend={args.backend} event_id={event_id} users={args.users} "
        f"join_concurrency={args.join_concurrency} admit_batch_size={args.admit_batch_size}",
        flush=True,
    )

    join_seconds = run_joins(service, event_id, args.users, args.join_concurrency)
    print(
        f"join_done users={args.users} seconds={join_seconds:.2f} "
        f"rate={rate(args.users, join_seconds):.2f}/sec queue_depth={service.store.depth(event_id)}",
        flush=True,
    )

    admitted, admit_seconds = run_admissions(service, event_id, args.admit_batch_size)
    print(
        f"admit_done users={admitted} seconds={admit_seconds:.2f} "
        f"rate={rate(admitted, admit_seconds):.2f}/sec queue_depth={service.store.depth(event_id)}",
        flush=True,
    )

    accepted, check_seconds = run_reservation_checks(service, event_id, args.users)
    print(
        f"reservation_check_done accepted={accepted} seconds={check_seconds:.2f} "
        f"rate={rate(args.users, check_seconds):.2f}/sec",
        flush=True,
    )

    if admitted != args.users or accepted != args.users:
        raise SystemExit(
            f"load test failed: admitted={admitted}/{args.users} accepted={accepted}/{args.users}"
        )


if __name__ == "__main__":
    main()
