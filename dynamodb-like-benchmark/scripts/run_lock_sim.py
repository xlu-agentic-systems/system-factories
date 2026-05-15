from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.lock_simulator import format_lock_results, run_lock_simulation


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate same-row counter locking versus distributed row locking.")
    parser.add_argument("--operations", type=int, default=10_000)
    parser.add_argument("--posts", type=int, default=1_000)
    parser.add_argument("--workers", type=int, default=128)
    parser.add_argument("--update-ms", type=float, default=1.0, help="simulated work while holding the row/item lock")
    parser.add_argument("--mode", choices=["hot", "distributed", "zipf", "all"], default="all")
    parser.add_argument("--output", default="results/lock-sim.json")
    args = parser.parse_args()

    modes = ["hot", "distributed"] if args.mode == "all" else [args.mode]
    results = [
        run_lock_simulation(
            mode=mode,
            operations=args.operations,
            posts=args.posts,
            workers=args.workers,
            update_ms=args.update_ms,
        )
        for mode in modes
    ]
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps([asdict(result) for result in results], indent=2), encoding="utf-8")
    print(format_lock_results(results))


if __name__ == "__main__":
    main()

