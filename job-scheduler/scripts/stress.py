import argparse
import asyncio
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.models import CreateJobRequest, Schedule, ScheduleType, epoch_seconds
from app.queue import RedisDelayQueue
from app.service import JobSchedulerService
from app.storage import DynamoJobStore
from app.storage import ConflictError, NotFoundError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local job scheduler stress test")
    parser.add_argument("--jobs", type=int, default=10_000)
    parser.add_argument("--create-concurrency", type=int, default=128)
    parser.add_argument("--process-concurrency", type=int, default=128)
    parser.add_argument("--pop-batch-size", type=int, default=500)
    parser.add_argument("--skip-process", action="store_true")
    return parser.parse_args()


def rate(count: int, seconds: float) -> float:
    return count / seconds if seconds > 0 else 0.0


async def run_blocking_many(
    count: int,
    concurrency: int,
    fn,
) -> float:
    loop = asyncio.get_running_loop()
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        tasks = [loop.run_in_executor(executor, fn, index) for index in range(count)]
        for completed, future in enumerate(asyncio.as_completed(tasks), start=1):
            await future
            if completed % 1000 == 0:
                elapsed = time.perf_counter() - start
                print(
                    f"completed={completed} elapsed={elapsed:.2f}s "
                    f"rate={rate(completed, elapsed):.2f}/sec",
                    flush=True,
                )
    return time.perf_counter() - start


async def process_due_queue(
    store: DynamoJobStore,
    queue: RedisDelayQueue,
    concurrency: int,
    batch_size: int,
) -> tuple[int, float]:
    loop = asyncio.get_running_loop()
    processed = 0
    start = time.perf_counter()

    def process_item(item) -> None:
        try:
            execution = store.get_execution(item.execution_id)
            execution = store.mark_in_progress(execution)
            store.mark_completed(execution)
        except (ConflictError, NotFoundError):
            pass
        finally:
            queue.ack(item)

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        while True:
            items = queue.pop_due(now=epoch_seconds(), limit=batch_size)
            if not items:
                break

            tasks = [loop.run_in_executor(executor, process_item, item) for item in items]
            for future in asyncio.as_completed(tasks):
                await future
                processed += 1
                if processed % 1000 == 0:
                    elapsed = time.perf_counter() - start
                    print(
                        f"processed={processed} elapsed={elapsed:.2f}s "
                        f"rate={rate(processed, elapsed):.2f}/sec",
                        flush=True,
                    )

    return processed, time.perf_counter() - start


async def main() -> None:
    args = parse_args()
    store = DynamoJobStore.from_settings()
    queue = RedisDelayQueue.from_settings()
    store.create_tables()
    queue.redis.delete(queue.key)
    queue.redis.delete(queue.processing_key)

    service = JobSchedulerService(store=store, queue=queue)
    user_id = f"stress_{uuid4().hex}"

    def create_one(index: int) -> None:
        service.create_job(
            CreateJobRequest(
                user_id=user_id,
                task_id="noop",
                schedule=Schedule(type=ScheduleType.IMMEDIATE),
                parameters={"index": index},
            )
        )

    print(
        f"creating jobs={args.jobs} concurrency={args.create_concurrency}",
        flush=True,
    )
    create_seconds = await run_blocking_many(
        count=args.jobs,
        concurrency=args.create_concurrency,
        fn=create_one,
    )
    due_count = queue.redis.zcard(queue.key)
    print(
        f"create_done jobs={args.jobs} seconds={create_seconds:.2f} "
        f"rate={rate(args.jobs, create_seconds):.2f}/sec due_queue={due_count}",
        flush=True,
    )

    if args.skip_process:
        return

    print(
        f"processing due queue concurrency={args.process_concurrency} "
        f"batch_size={args.pop_batch_size}",
        flush=True,
    )
    processed, process_seconds = await process_due_queue(
        store=store,
        queue=queue,
        concurrency=args.process_concurrency,
        batch_size=args.pop_batch_size,
    )
    processing_count = queue.redis.zcard(queue.processing_key)
    remaining_due = queue.redis.zcard(queue.key)
    print(
        f"process_done jobs={processed} seconds={process_seconds:.2f} "
        f"rate={rate(processed, process_seconds):.2f}/sec "
        f"remaining_due={remaining_due} processing={processing_count}",
        flush=True,
    )


if __name__ == "__main__":
    asyncio.run(main())
