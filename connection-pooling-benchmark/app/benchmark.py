from __future__ import annotations

import asyncio
from dataclasses import dataclass
from time import perf_counter

import asyncpg

from app.metrics import LatencySummary, summarize_latencies


QUERY = "SELECT payload FROM benchmark_items WHERE id = $1"


@dataclass(frozen=True)
class BenchmarkConfig:
    mode: str
    dsn: str
    qps: int
    duration_seconds: float
    pool_size: int
    max_in_flight: int
    row_count: int


@dataclass(frozen=True)
class BenchmarkResult:
    mode: str
    target_qps: int
    scheduled: int
    completed: int
    errors: int
    elapsed_seconds: float
    achieved_qps: float
    latency: LatencySummary


class QueryRunner:
    async def start(self) -> None:
        pass

    async def run_one(self, item_id: int) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        pass


class NewConnectionRunner(QueryRunner):
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    async def run_one(self, item_id: int) -> None:
        conn = await asyncpg.connect(self.dsn)
        try:
            await conn.fetchval(QUERY, item_id)
        finally:
            await conn.close()


class PoolRunner(QueryRunner):
    def __init__(self, dsn: str, pool_size: int) -> None:
        self.dsn = dsn
        self.pool_size = pool_size
        self.pool: asyncpg.Pool | None = None

    async def start(self) -> None:
        self.pool = await asyncpg.create_pool(
            self.dsn,
            min_size=self.pool_size,
            max_size=self.pool_size,
        )

    async def run_one(self, item_id: int) -> None:
        if self.pool is None:
            raise RuntimeError("pool has not been started")
        async with self.pool.acquire() as conn:
            await conn.fetchval(QUERY, item_id)

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()


def make_runner(mode: str, dsn: str, pool_size: int) -> QueryRunner:
    if mode in {"direct_new", "pgbouncer_new"}:
        return NewConnectionRunner(dsn)
    if mode in {"direct_pool", "pgbouncer_pool"}:
        return PoolRunner(dsn, pool_size)
    raise ValueError(f"unsupported benchmark mode: {mode}")


async def run_benchmark(config: BenchmarkConfig) -> BenchmarkResult:
    runner = make_runner(config.mode, config.dsn, config.pool_size)
    await runner.start()
    semaphore = asyncio.Semaphore(config.max_in_flight)
    latencies_ms: list[float] = []
    errors = 0
    scheduled = 0
    completed = 0
    tasks: set[asyncio.Task] = set()
    started = perf_counter()
    next_tick = started
    interval = 1.0 / config.qps

    async def one_request(sequence: int) -> None:
        nonlocal completed, errors
        item_id = (sequence % config.row_count) + 1
        async with semaphore:
            before = perf_counter()
            try:
                await runner.run_one(item_id)
                latencies_ms.append((perf_counter() - before) * 1000)
                completed += 1
            except Exception:
                errors += 1

    try:
        while perf_counter() - started < config.duration_seconds:
            now = perf_counter()
            if now < next_tick:
                await asyncio.sleep(min(next_tick - now, 0.001))
                continue
            task = asyncio.create_task(one_request(scheduled))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            scheduled += 1
            next_tick += interval

        if tasks:
            await asyncio.gather(*tasks)
    finally:
        await runner.close()

    elapsed = perf_counter() - started
    return BenchmarkResult(
        mode=config.mode,
        target_qps=config.qps,
        scheduled=scheduled,
        completed=completed,
        errors=errors,
        elapsed_seconds=elapsed,
        achieved_qps=completed / elapsed if elapsed else 0.0,
        latency=summarize_latencies(latencies_ms),
    )

