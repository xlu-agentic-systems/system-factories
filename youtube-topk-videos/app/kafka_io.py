from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from typing import Any

from app.models import ViewEvent


def event_to_json(event: ViewEvent) -> bytes:
    payload = {
        "event_id": event.event_id,
        "video_id": event.video_id,
        "occurred_at": event.occurred_at,
        "kafka_partition": event.kafka_partition,
        "kafka_offset": event.kafka_offset,
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def event_from_json(payload: bytes | str, partition: int | None = None, offset: int | None = None) -> ViewEvent:
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    data: dict[str, Any] = json.loads(payload)
    return ViewEvent(
        event_id=str(data["event_id"]),
        video_id=str(data["video_id"]),
        occurred_at=int(data["occurred_at"]),
        kafka_partition=int(partition if partition is not None else data.get("kafka_partition", 0)),
        kafka_offset=offset if offset is not None else data.get("kafka_offset"),
    )


def require_kafka():
    try:
        from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError
    except ImportError as exc:
        raise RuntimeError(
            "Kafka support requires kafka-python. Install with: pip install -r requirements.txt"
        ) from exc
    return KafkaAdminClient, KafkaConsumer, KafkaProducer, NewTopic, TopicAlreadyExistsError


def ensure_topic(
    bootstrap_servers: str,
    topic: str,
    partitions: int = 64,
    replication_factor: int = 1,
) -> None:
    KafkaAdminClient, _, _, NewTopic, TopicAlreadyExistsError = require_kafka()
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="youtube-topk-admin")
    try:
        admin.create_topics(
            [NewTopic(name=topic, num_partitions=partitions, replication_factor=replication_factor)]
        )
    except TopicAlreadyExistsError:
        return
    finally:
        admin.close()


def produce_view_events(
    bootstrap_servers: str,
    topic: str,
    events: Iterable[ViewEvent],
    linger_ms: int = 50,
    batch_size: int = 131_072,
) -> int:
    _, _, KafkaProducer, _, _ = require_kafka()
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=event_to_json,
        linger_ms=linger_ms,
        batch_size=batch_size,
        acks="all",
        compression_type="gzip",
    )
    count = 0
    try:
        for event in events:
            producer.send(topic, key=event.video_id, value=event)
            count += 1
        producer.flush()
    finally:
        producer.close()
    return count


def consume_view_events(
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    max_messages: int | None = None,
    max_poll_records: int = 10_000,
    poll_timeout_ms: int = 1000,
    idle_polls_before_stop: int | None = None,
) -> Iterator[list[ViewEvent]]:
    _, KafkaConsumer, _, _, _ = require_kafka()
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=max_poll_records,
    )
    consumed = 0
    idle_polls = 0
    try:
        while max_messages is None or consumed < max_messages:
            records = consumer.poll(timeout_ms=poll_timeout_ms, max_records=max_poll_records)
            if not records:
                idle_polls += 1
                if idle_polls_before_stop is not None and idle_polls >= idle_polls_before_stop:
                    break
                continue

            idle_polls = 0
            batch: list[ViewEvent] = []
            for partition, messages in records.items():
                for message in messages:
                    if max_messages is not None and consumed >= max_messages:
                        break
                    batch.append(event_from_json(message.value, partition.partition, message.offset))
                    consumed += 1
                if max_messages is not None and consumed >= max_messages:
                    break

            if batch:
                yield batch
                consumer.commit()
    finally:
        consumer.close()

