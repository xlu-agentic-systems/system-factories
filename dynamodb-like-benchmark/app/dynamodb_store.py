from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import boto3
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class WriteResult:
    ok: bool
    action: str
    post_id: str
    user_id: str
    reason: str
    elapsed_ms: float


class LikeStore:
    def __init__(
        self,
        endpoint_url: str = "http://127.0.0.1:58000",
        region_name: str = "us-east-1",
    ) -> None:
        self.resource = boto3.resource(
            "dynamodb",
            endpoint_url=endpoint_url,
            region_name=region_name,
            aws_access_key_id="local",
            aws_secret_access_key="local",
        )
        self.client = self.resource.meta.client
        self.likes = self.resource.Table("Likes")
        self.counters = self.resource.Table("PostCounters")

    def reset(self, post_ids: list[str]) -> None:
        self._delete_table_if_exists("Likes")
        self._delete_table_if_exists("PostCounters")
        self._create_likes_table()
        self._create_counters_table()
        with self.counters.batch_writer() as batch:
            for post_id in post_ids:
                batch.put_item(Item={"post_id": post_id, "like_count": Decimal(0)})

    def like(self, post_id: str, user_id: str) -> WriteResult:
        started = time.perf_counter()
        try:
            self.likes.put_item(
                Item={"user_id": user_id, "post_id": post_id, "created_at": Decimal(str(time.time()))},
                ConditionExpression="attribute_not_exists(user_id) AND attribute_not_exists(post_id)",
            )
            self.counters.update_item(
                Key={"post_id": post_id},
                UpdateExpression="ADD like_count :one",
                ExpressionAttributeValues={":one": Decimal(1)},
            )
            return _result(True, "like", post_id, user_id, "liked", started)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return _result(False, "like", post_id, user_id, "already-liked", started)
            raise

    def unlike(self, post_id: str, user_id: str) -> WriteResult:
        started = time.perf_counter()
        try:
            self.likes.delete_item(
                Key={"user_id": user_id, "post_id": post_id},
                ConditionExpression="attribute_exists(user_id) AND attribute_exists(post_id)",
            )
            self.counters.update_item(
                Key={"post_id": post_id},
                UpdateExpression="ADD like_count :minus_one",
                ExpressionAttributeValues={":minus_one": Decimal(-1)},
            )
            return _result(True, "unlike", post_id, user_id, "unliked", started)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return _result(False, "unlike", post_id, user_id, "not-liked", started)
            raise

    def counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        response = self.counters.scan()
        for item in response.get("Items", []):
            counts[item["post_id"]] = int(item["like_count"])
        while "LastEvaluatedKey" in response:
            response = self.counters.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
            for item in response.get("Items", []):
                counts[item["post_id"]] = int(item["like_count"])
        return counts

    def _delete_table_if_exists(self, table_name: str) -> None:
        existing = self.client.list_tables()["TableNames"]
        if table_name not in existing:
            return
        self.client.delete_table(TableName=table_name)
        waiter = self.client.get_waiter("table_not_exists")
        waiter.wait(TableName=table_name)

    def _create_likes_table(self) -> None:
        self.client.create_table(
            TableName="Likes",
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[
                {"AttributeName": "user_id", "AttributeType": "S"},
                {"AttributeName": "post_id", "AttributeType": "S"},
            ],
            KeySchema=[
                {"AttributeName": "user_id", "KeyType": "HASH"},
                {"AttributeName": "post_id", "KeyType": "RANGE"},
            ],
        )
        self.client.get_waiter("table_exists").wait(TableName="Likes")

    def _create_counters_table(self) -> None:
        self.client.create_table(
            TableName="PostCounters",
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": "post_id", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "post_id", "KeyType": "HASH"}],
        )
        self.client.get_waiter("table_exists").wait(TableName="PostCounters")


def _result(
    ok: bool,
    action: str,
    post_id: str,
    user_id: str,
    reason: str,
    started: float,
) -> WriteResult:
    return WriteResult(
        ok=ok,
        action=action,
        post_id=post_id,
        user_id=user_id,
        reason=reason,
        elapsed_ms=(time.perf_counter() - started) * 1000,
    )

