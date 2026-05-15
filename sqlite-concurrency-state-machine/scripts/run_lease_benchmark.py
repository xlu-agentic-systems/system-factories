from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.lease_benchmark import (  # noqa: E402
    LEASE_STRATEGIES,
    run_crash_recovery,
    run_lease_contention,
    summarize_contention,
    summarize_crash,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark lease strategies under contention and crash recovery.")
    parser.add_argument("--base-path", default="data/lease-benchmark", help="directory for local benchmark state")
    parser.add_argument("--workers", type=int, default=32)
    parser.add_argument("--ttl", type=float, default=15.0, help="lease TTL in seconds")
    parser.add_argument("--mode", choices=["threads", "processes", "both"], default="both")
    parser.add_argument("--strategy", choices=sorted(LEASE_STRATEGIES | {"all"}), default="all")
    parser.add_argument("--redis-url", help="use a real Redis server for the redis-ttl strategy")
    args = parser.parse_args()

    strategies = sorted(LEASE_STRATEGIES) if args.strategy == "all" else [args.strategy]
    modes = ["threads", "processes"] if args.mode == "both" else [args.mode]
    base = Path(args.base_path)

    contention_results = [
        run_lease_contention(
            base_path=base / f"{strategy}-{mode}",
            strategy=strategy,
            workers=args.workers,
            mode=mode,
            ttl_seconds=args.ttl,
            redis_url=args.redis_url,
        )
        for mode in modes
        for strategy in strategies
    ]
    crash_results = [
        run_crash_recovery(
            base_path=base / f"{strategy}-crash",
            strategy=strategy,
            ttl_seconds=args.ttl,
            redis_url=args.redis_url,
        )
        for strategy in strategies
    ]

    print("Contention")
    print(summarize_contention(contention_results))
    print()
    print("Crash Recovery")
    print(summarize_crash(crash_results))


if __name__ == "__main__":
    main()
