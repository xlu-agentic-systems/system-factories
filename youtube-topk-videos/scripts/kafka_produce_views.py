from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.kafka_io import ensure_topic, produce_view_events
from app.simulator import generate_zipfian_views, project_scale


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce synthetic YouTube view events to Kafka.")
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--topic", default="video-views")
    parser.add_argument("--partitions", type=int, default=64)
    parser.add_argument("--events", type=int, default=1_000_000)
    parser.add_argument("--videos", type=int, default=1_000_000)
    parser.add_argument("--daily-views", type=int, default=70_000_000_000)
    parser.add_argument("--start-time", type=int, default=1_700_000_000)
    parser.add_argument("--simulated-duration-seconds", type=int, default=86_400)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--seed", type=int, default=7)
    args = parser.parse_args()

    ensure_topic(args.bootstrap_servers, args.topic, partitions=args.partitions)
    projection = project_scale(daily_views=args.daily_views, batch_size=args.batch_size)
    stream = generate_zipfian_views(
        total_events=args.events,
        distinct_videos=args.videos,
        start_time=args.start_time,
        duration_seconds=args.simulated_duration_seconds,
        seed=args.seed,
    )

    started = time.perf_counter()
    produced = produce_view_events(args.bootstrap_servers, args.topic, stream)
    elapsed = time.perf_counter() - started
    represented_seconds = produced / projection.events_per_second

    print(f"topic={args.topic}")
    print(f"produced_events={produced:,}")
    print(f"producer_rate={produced / max(elapsed, 0.001):,.0f} events/sec")
    print(f"target_70b_avg_rate={projection.events_per_second:,.0f} events/sec")
    print(f"sample_represents_70b_scale_seconds={represented_seconds:,.2f}")
    print(f"target_micro_batches_per_day={projection.micro_batches_per_day:,}")


if __name__ == "__main__":
    main()

