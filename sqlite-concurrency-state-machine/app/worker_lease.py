from __future__ import annotations

import argparse
import time

from app.lease_benchmark import LEASE_STRATEGIES, attempt_once, to_json


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one lease attempt for process-mode benchmarks.")
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--strategy", required=True, choices=sorted(LEASE_STRATEGIES))
    parser.add_argument("--mode", required=True)
    parser.add_argument("--resource-id", required=True)
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--ttl", type=float, required=True)
    parser.add_argument("--start-at", type=float, required=True)
    parser.add_argument("--redis-url")
    args = parser.parse_args()

    while time.monotonic() < args.start_at:
        time.sleep(0.001)

    result = attempt_once(
        base_path=args.base_path,
        strategy=args.strategy,
        mode=args.mode,
        resource_id=args.resource_id,
        worker_id=args.worker_id,
        ttl_seconds=args.ttl,
        redis_url=args.redis_url,
    )
    print(to_json(result))


if __name__ == "__main__":
    main()
