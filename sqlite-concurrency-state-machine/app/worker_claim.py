from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict

from app.experiment import STRATEGIES


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one claim attempt for the process-mode experiment.")
    parser.add_argument("--strategy", required=True, choices=sorted(STRATEGIES))
    parser.add_argument("--db", required=True)
    parser.add_argument("--driver-id", required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--delay", type=float, required=True)
    parser.add_argument("--start-at", type=float, required=True)
    args = parser.parse_args()

    while time.monotonic() < args.start_at:
        time.sleep(0.001)

    result = STRATEGIES[args.strategy](args.db, args.driver_id, args.worker_id, args.delay)
    print(json.dumps(asdict(result)))


if __name__ == "__main__":
    main()

