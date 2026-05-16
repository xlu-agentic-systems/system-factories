from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.kafka_io import consume_view_events
from app.models import WINDOW_1DAY, WINDOW_1HOUR, WINDOW_1MONTH, WINDOW_ALL_TIME
from app.query import TopKQueryService
from app.storage import SQLiteTopKStorage
from app.stream import TopKStreamProcessor
from app.time_windows import bucket_start


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume Kafka view events and update top-k tables.")
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--topic", default="video-views")
    parser.add_argument("--group-id", default="youtube-topk-local")
    parser.add_argument("--db", default="data/youtube_topk.sqlite3")
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--batch-size", type=int, default=50_000)
    parser.add_argument("--idle-polls-before-stop", type=int, default=5)
    parser.add_argument("--k", type=int, default=10)
    parser.add_argument("--query-time", type=int, default=1_700_000_000)
    args = parser.parse_args()

    storage = SQLiteTopKStorage(args.db)
    processor = TopKStreamProcessor(storage)

    started = time.perf_counter()
    processed = 0
    for batch in consume_view_events(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        max_messages=args.max_messages,
        max_poll_records=args.batch_size,
        idle_polls_before_stop=args.idle_polls_before_stop,
    ):
        processed += processor.process_batch(batch)

    elapsed = time.perf_counter() - started
    query = TopKQueryService(storage)
    print(f"processed_events={processed:,}")
    print(f"consumer_pipeline_rate={processed / max(elapsed, 0.001):,.0f} events/sec")
    for window in (WINDOW_1HOUR, WINDOW_1DAY, WINDOW_1MONTH, WINDOW_ALL_TIME):
        print(f"\n{window} bucket={bucket_start(window, args.query_time)}")
        for entry in query.topk_at(window, args.query_time, args.k):
            print(f"{entry.rank:>3} {entry.video_id:<16} {entry.view_count}")


if __name__ == "__main__":
    main()

