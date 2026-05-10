from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.models import ClickInput
from app.service import ClickAggregationService
from app.storage import SQLiteClickStorage
from app.stream import StreamProcessor


def main() -> None:
    with tempfile.TemporaryDirectory() as directory:
        storage = SQLiteClickStorage(f"{directory}/clicks.sqlite3")
        processor = StreamProcessor(storage)
        service = ClickAggregationService(
            storage=storage,
            processor=processor,
            hmac_secret="smoke-secret",
        )

        signature = service.sign_impression(
            advertiser_id="adv_1",
            ad_id="ad_1",
            impression_id="imp_1",
        )
        service.track_click(
            ClickInput(
                advertiser_id="adv_1",
                ad_id="ad_1",
                impression_id="imp_1",
                target_url="https://advertiser.example/landing",
                signature=signature,
                occurred_at=1_700_000_000,
            )
        )
        processor.drain()
        points = service.query_metrics(
            advertiser_id="adv_1",
            ad_ids=["ad_1"],
            start_time=1_699_999_980,
            end_time=1_700_000_080,
        )
        print(points)


if __name__ == "__main__":
    main()
