from __future__ import annotations

import argparse
import os
import time

from app.storage import SQLiteClickStorage
from app.stream import StreamProcessor


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Map/Aggregate/Reduce stream worker.")
    parser.add_argument("--db", default=os.getenv("ADS_CLICK_DB_PATH", "data/ads_clicks.sqlite3"))
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--interval", type=float, default=0.5)
    args = parser.parse_args()

    processor = StreamProcessor(SQLiteClickStorage(args.db))
    while True:
        processed = processor.run_once(batch_size=args.batch_size)
        if processed == 0:
            time.sleep(args.interval)


if __name__ == "__main__":
    main()

