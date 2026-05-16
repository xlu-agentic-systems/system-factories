from __future__ import annotations

import argparse
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.models import WINDOW_1DAY, WINDOW_1HOUR, WINDOW_1MONTH, WINDOW_ALL_TIME
from app.query import TopKQueryService
from app.simulator import generate_zipfian_views, project_scale
from app.storage import SQLiteTopKStorage
from app.stream import TopKStreamProcessor
from app.time_windows import bucket_start


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate sharded top-k video aggregation.")
    parser.add_argument("--db", default="data/youtube_topk.sqlite3")
    parser.add_argument("--events", type=int, default=200_000)
    parser.add_argument("--videos", type=int, default=20_000)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--start-time", type=int, default=1_700_000_000)
    args = parser.parse_args()

    storage = SQLiteTopKStorage(args.db, k_limit=max(args.k, 1000))
    processor = TopKStreamProcessor(storage)
    stream = generate_zipfian_views(
        total_events=args.events,
        distinct_videos=args.videos,
        start_time=args.start_time,
        duration_seconds=3600,
    )
    processed = processor.process_stream(stream, batch_size=args.batch_size)
    projection = project_scale(batch_size=args.batch_size)
    query = TopKQueryService(storage)

    print(f"processed_events={processed}")
    print(f"projected_70b_tps={projection.events_per_second:,.0f}")
    print(f"projected_micro_batches_per_day={projection.micro_batches_per_day:,}")
    for window in (WINDOW_1HOUR, WINDOW_1DAY, WINDOW_1MONTH, WINDOW_ALL_TIME):
        start = bucket_start(window, args.start_time)
        print(f"\n{window} bucket={start}")
        for entry in query.topk_at(window, args.start_time, args.k):
            print(f"{entry.rank:>3} {entry.video_id:<16} {entry.view_count}")


if __name__ == "__main__":
    main()

