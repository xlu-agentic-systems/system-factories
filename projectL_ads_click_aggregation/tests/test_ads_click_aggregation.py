from __future__ import annotations

import pytest

from app.log_parser import parse_click_log_line
from app.models import ClickEvent, ClickInput
from app.service import ClickAggregationService
from app.storage import SQLiteClickStorage
from app.stream import AggregateNode, MapAggregateReducePipeline, MapNode, ReduceNode, StreamProcessor


def make_service(tmp_path):
    storage = SQLiteClickStorage(tmp_path / "clicks.sqlite3")
    processor = StreamProcessor(storage)
    return ClickAggregationService(
        storage=storage,
        processor=processor,
        hmac_secret="test-secret",
    ), storage, processor


def test_clicks_are_stored_raw_and_aggregated_by_minute(tmp_path) -> None:
    service, storage, processor = make_service(tmp_path)

    sig_1 = service.sign_impression(
        advertiser_id="adv_1", ad_id="ad_1", impression_id="imp_1"
    )
    sig_2 = service.sign_impression(
        advertiser_id="adv_1", ad_id="ad_1", impression_id="imp_2"
    )
    for impression_id, signature, occurred_at in [
        ("imp_1", sig_1, 1_700_000_001),
        ("imp_2", sig_2, 1_700_000_012),
    ]:
        service.track_click(
            ClickInput(
                advertiser_id="adv_1",
                ad_id="ad_1",
                impression_id=impression_id,
                target_url="https://advertiser.example/a",
                signature=signature,
                occurred_at=occurred_at,
            )
        )

    assert storage.raw_count() == 2
    assert processor.drain(batch_size=10) == 2

    points = service.query_metrics(
        advertiser_id="adv_1",
        ad_ids=["ad_1"],
        start_time=1_700_000_000,
        end_time=1_700_000_060,
    )

    assert len(points) == 1
    assert points[0].click_count == 2
    assert points[0].bucket_start == 1_699_999_980


def test_impression_id_is_idempotent(tmp_path) -> None:
    service, storage, processor = make_service(tmp_path)
    signature = service.sign_impression(
        advertiser_id="adv_1", ad_id="ad_1", impression_id="imp_duplicate"
    )
    click = ClickInput(
        advertiser_id="adv_1",
        ad_id="ad_1",
        impression_id="imp_duplicate",
        target_url="https://advertiser.example/a",
        signature=signature,
        occurred_at=1_700_000_000,
    )

    first = service.track_click(click)
    second = service.track_click(click)
    processor.drain()

    assert first.accepted is True
    assert second.duplicate is True
    assert storage.raw_count() == 1
    points = service.query_metrics(
        advertiser_id="adv_1",
        ad_ids=["ad_1"],
        start_time=1_699_999_980,
        end_time=1_700_000_060,
    )
    assert points[0].click_count == 1


def test_invalid_signature_is_rejected(tmp_path) -> None:
    service, storage, _ = make_service(tmp_path)

    with pytest.raises(ValueError, match="invalid impression signature"):
        service.track_click(
            ClickInput(
                advertiser_id="adv_1",
                ad_id="ad_1",
                impression_id="imp_1",
                target_url="https://advertiser.example/a",
                signature="bad",
            )
        )

    assert storage.raw_count() == 0


def test_log_line_parser_accepts_key_value_input(tmp_path) -> None:
    service, _, processor = make_service(tmp_path)
    signature = service.sign_impression(
        advertiser_id="adv_1", ad_id="ad_1", impression_id="imp_1"
    )
    line = (
        "ts=2023-11-14T22:13:20Z advertiser_id=adv_1 ad_id=ad_1 "
        f"impression_id=imp_1 target_url=https://advertiser.example/a signature={signature}"
    )

    parsed = parse_click_log_line(line)
    service.track_click(parsed)
    processor.drain()

    points = service.query_metrics(
        advertiser_id="adv_1",
        start_time=1_699_999_980,
        end_time=1_700_000_060,
    )
    assert points[0].click_count == 1


def test_hot_ad_virtual_shards_reduce_to_one_metric_delta() -> None:
    events = [
        ClickEvent(
            raw_event_id=i + 1,
            event_id=f"event_{i}",
            advertiser_id="adv_1",
            ad_id="hot_ad",
            impression_id=f"imp_{i}",
            target_url="https://advertiser.example/a",
            signature="sig",
            occurred_at=1_700_000_000 + i,
            received_at=1_700_000_000 + i,
        )
        for i in range(20)
    ]
    pipeline = MapAggregateReducePipeline(
        map_node=MapNode(shard_count=4, hot_ad_shards={"hot_ad": 8}),
        aggregate_node=AggregateNode(),
        reduce_node=ReduceNode(),
    )

    mapped = pipeline.map_node.map(events)
    assert len({event.partition_key for event in mapped}) > 1

    deltas = pipeline.process(events)
    assert len(deltas) == 1
    assert deltas[0].ad_id == "hot_ad"
    assert deltas[0].click_count == 20


def test_reconciliation_rebuilds_derived_metrics_from_raw(tmp_path) -> None:
    service, storage, _ = make_service(tmp_path)
    for i in range(3):
        signature = service.sign_impression(
            advertiser_id="adv_1", ad_id="ad_1", impression_id=f"imp_{i}"
        )
        service.track_click(
            ClickInput(
                advertiser_id="adv_1",
                ad_id="ad_1",
                impression_id=f"imp_{i}",
                target_url="https://advertiser.example/a",
                signature=signature,
                occurred_at=1_700_000_000 + i,
            )
        )

    rows = storage.rebuild_derived_metrics(
        start_time=1_699_999_980,
        end_time=1_700_000_060,
    )
    points = service.query_metrics(
        advertiser_id="adv_1",
        start_time=1_699_999_980,
        end_time=1_700_000_060,
    )

    assert rows == 1
    assert points[0].click_count == 3
