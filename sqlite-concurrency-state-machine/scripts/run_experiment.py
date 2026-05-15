from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.experiment import run_race, summarize


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SQLite state transition concurrency experiments.")
    parser.add_argument("--db", default="data/concurrency-demo.sqlite3", help="SQLite database path")
    parser.add_argument("--workers", type=int, default=32, help="concurrent workers per run")
    parser.add_argument("--delay", type=float, default=0.02, help="seconds of simulated business work per worker")
    parser.add_argument("--mode", choices=["threads", "processes", "both"], default="both")
    parser.add_argument(
        "--strategy",
        choices=["unsafe", "transaction", "atomic", "all"],
        default="all",
    )
    args = parser.parse_args()

    strategies = ["unsafe", "transaction", "atomic"] if args.strategy == "all" else [args.strategy]
    modes = ["threads", "processes"] if args.mode == "both" else [args.mode]
    results = []

    for mode in modes:
        for strategy in strategies:
            results.append(
                run_race(
                    db_path=Path(args.db),
                    strategy=strategy,
                    workers=args.workers,
                    mode=mode,
                    delay_seconds=args.delay,
                )
            )

    print(summarize(results))


if __name__ == "__main__":
    main()

