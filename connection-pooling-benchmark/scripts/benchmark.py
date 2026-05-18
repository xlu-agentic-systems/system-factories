from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.benchmark import BenchmarkConfig, run_benchmark
from app.db import setup_database


DEFAULT_DIRECT_DSN = "postgresql://bench:bench@127.0.0.1:25432/bench"
DEFAULT_PGBOUNCER_DSN = "postgresql://bench:bench@127.0.0.1:26432/bench"


async def main_async() -> None:
    parser = argparse.ArgumentParser(description="Benchmark PostgreSQL connection pooling.")
    parser.add_argument("--direct-dsn", default=os.getenv("DIRECT_DATABASE_URL", DEFAULT_DIRECT_DSN))
    parser.add_argument("--pgbouncer-dsn", default=os.getenv("PGBOUNCER_DATABASE_URL", DEFAULT_PGBOUNCER_DSN))
    parser.add_argument(
        "--modes",
        nargs="+",
        choices=("direct_new", "direct_pool", "pgbouncer_new", "pgbouncer_pool"),
        default=["direct_new", "direct_pool", "pgbouncer_new", "pgbouncer_pool"],
    )
    parser.add_argument("--qps", nargs="+", type=int, default=[100, 500, 1000, 2500])
    parser.add_argument("--duration", type=float, default=3.0)
    parser.add_argument("--pool-size", type=int, default=32)
    parser.add_argument("--max-in-flight", type=int, default=5000)
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--setup", action="store_true")
    args = parser.parse_args()

    if args.setup:
        await setup_database(args.direct_dsn, rows=args.rows)

    print("mode,target_qps,scheduled,completed,errors,elapsed_seconds,achieved_qps,p50_ms,p95_ms,p99_ms,max_ms")
    for qps in args.qps:
        for mode in args.modes:
            dsn = args.pgbouncer_dsn if mode.startswith("pgbouncer") else args.direct_dsn
            result = await run_benchmark(
                BenchmarkConfig(
                    mode=mode,
                    dsn=dsn,
                    qps=qps,
                    duration_seconds=args.duration,
                    pool_size=args.pool_size,
                    max_in_flight=args.max_in_flight,
                    row_count=args.rows,
                )
            )
            latency = result.latency
            print(
                f"{result.mode},{result.target_qps},{result.scheduled},{result.completed},"
                f"{result.errors},{result.elapsed_seconds:.3f},{result.achieved_qps:.1f},"
                f"{latency.p50_ms:.3f},{latency.p95_ms:.3f},{latency.p99_ms:.3f},{latency.max_ms:.3f}",
                flush=True,
            )


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()

