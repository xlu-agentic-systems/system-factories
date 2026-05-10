# ProjectL Ads Click Aggregation

Local implementation of an ads click event aggregation system with a Map/Aggregate/Reduce streaming architecture. It supports click redirects, durable raw click storage, derived minute-level metrics, idempotent click tracking, and log-line ingestion.

## What It Implements

- `GET /click` records a click and returns a `302` redirect to the advertiser URL.
- `POST /click/log` ingests click data from a single log line.
- `GET /metrics` returns advertiser click metrics at a minimum granularity of 1 minute.
- Raw click events are durably stored in SQLite as an append-only source of truth.
- Derived click metrics are stored separately in SQLite for low-latency queries.
- A streaming worker processes raw events through explicit Map, Aggregate, and Reduce nodes.
- Impression IDs are HMAC-signed and deduplicated so duplicate clicks are not counted.
- A reconciliation endpoint can rebuild derived metrics from raw events.

## Architecture

Click ingestion is intentionally separated from analytics processing:

1. The click endpoint validates the signed impression ID and target URL.
2. The service deduplicates by `impression_id` and appends accepted clicks to `raw_click_events`.
3. The Map node shards raw events by `ad_id`. Hot ads can be split into extra virtual shards.
4. The Aggregate node counts clicks by partition, ad, advertiser, and event-time minute.
5. The Reduce node combines partial counts into final advertiser/ad/minute deltas.
6. The stream processor upserts deltas into `derived_click_metrics` and advances its cursor in the same transaction.
7. Metrics queries read only the derived table, so advertiser queries stay fast.

This models Kafka/Kinesis-style stream retention with a local SQLite outbox. If the worker stops, raw events remain stored and are processed when it resumes.

## Log Line Format

The log parser accepts JSON or key-value text.

```text
ts=2023-11-14T22:13:20Z advertiser_id=adv_1 ad_id=ad_1 impression_id=imp_1 target_url=https://advertiser.example/a signature=<hmac>
```

Required fields:

- `advertiser_id`
- `ad_id`
- `impression_id`
- `target_url`
- `signature`

Aliases such as `advertiser`, `adId`, `timestamp`, `url`, and `sig` are also accepted.

## Local Run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.api:app --host 127.0.0.1 --port 8080 --reload
```

Run the stream worker in another shell:

```bash
python -m app.worker --db data/ads_clicks.sqlite3
```

For local demos, you can also let the API schedule a small background processing pass after each click or call:

```bash
curl -X POST 'http://127.0.0.1:8080/stream/drain'
```

## Example Flow

Create an impression signature:

```bash
curl 'http://127.0.0.1:8080/sign?advertiser_id=adv_1&ad_id=ad_1&impression_id=imp_1'
```

Record a click and redirect:

```bash
curl -i 'http://127.0.0.1:8080/click?advertiser_id=adv_1&ad_id=ad_1&impression_id=imp_1&target_url=https%3A%2F%2Fadvertiser.example%2Fa&signature=<hmac>'
```

Query metrics:

```bash
curl 'http://127.0.0.1:8080/metrics?advertiser_id=adv_1&ad_id=ad_1&start_time=1699999980&end_time=1700000060&granularity_seconds=60'
```

Run tests:

```bash
pytest -q
python scripts/smoke.py
```

## Scale Notes

The local implementation uses SQLite so it can run without external infrastructure. In production, the same boundaries map cleanly to larger systems:

- Raw storage and stream retention: Kafka or Kinesis plus object storage archive.
- Map node: stream partitioning by `ad_id`, with virtual shards for hot ads.
- Aggregate node: per-partition minute counters.
- Reduce node: final merge into advertiser/ad/minute aggregates.
- Derived storage: ClickHouse, BigQuery, Snowflake, Redshift, or another OLAP store.
- Reconciliation: periodic batch rebuild from raw archived events.

