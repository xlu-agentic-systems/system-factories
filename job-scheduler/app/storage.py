from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from app.config import settings
from app.models import (
    ExecutionRecord,
    ExecutionStatus,
    JobRecord,
    Schedule,
    epoch_seconds,
)


class ConflictError(RuntimeError):
    pass


class NotFoundError(RuntimeError):
    pass


class DynamoJobStore:
    user_execution_index = "user_execution_time_index"
    status_index = "status_execution_time_index"

    def __init__(self, dynamodb: Any) -> None:
        self.dynamodb = dynamodb
        self.jobs = dynamodb.Table(settings.dynamodb_jobs_table)
        self.executions = dynamodb.Table(settings.dynamodb_executions_table)

    @classmethod
    def from_settings(cls) -> "DynamoJobStore":
        dynamodb = boto3.resource(
            "dynamodb",
            region_name=settings.aws_region,
            endpoint_url=settings.dynamodb_endpoint_url,
            aws_access_key_id="local",
            aws_secret_access_key="local",
        )
        return cls(dynamodb)

    def create_tables(self) -> None:
        existing = {table.name for table in self.dynamodb.tables.all()}

        if settings.dynamodb_jobs_table not in existing:
            self.dynamodb.create_table(
                TableName=settings.dynamodb_jobs_table,
                KeySchema=[{"AttributeName": "job_id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "job_id", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            ).wait_until_exists()

        if settings.dynamodb_executions_table not in existing:
            self.dynamodb.create_table(
                TableName=settings.dynamodb_executions_table,
                KeySchema=[
                    {"AttributeName": "time_bucket", "KeyType": "HASH"},
                    {"AttributeName": "execution_time_key", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "time_bucket", "AttributeType": "S"},
                    {"AttributeName": "execution_time_key", "AttributeType": "S"},
                    {"AttributeName": "execution_id", "AttributeType": "S"},
                    {"AttributeName": "user_id", "AttributeType": "S"},
                    {"AttributeName": "status", "AttributeType": "S"},
                ],
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": self.user_execution_index,
                        "KeySchema": [
                            {"AttributeName": "user_id", "KeyType": "HASH"},
                            {"AttributeName": "execution_time_key", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    },
                    {
                        "IndexName": self.status_index,
                        "KeySchema": [
                            {"AttributeName": "status", "KeyType": "HASH"},
                            {"AttributeName": "execution_time_key", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    },
                    {
                        "IndexName": "execution_id_index",
                        "KeySchema": [{"AttributeName": "execution_id", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                    },
                ],
                BillingMode="PAY_PER_REQUEST",
            ).wait_until_exists()

    def put_job(self, job: JobRecord) -> None:
        item = job.model_dump(mode="json")
        self.jobs.put_item(Item=item, ConditionExpression="attribute_not_exists(job_id)")

    def put_execution(self, execution: ExecutionRecord) -> None:
        item = self._execution_to_item(execution)
        self.executions.put_item(
            Item=item,
            ConditionExpression=(
                "attribute_not_exists(time_bucket) "
                "AND attribute_not_exists(execution_time_key)"
            ),
        )

    def get_job(self, job_id: str) -> JobRecord:
        response = self.jobs.get_item(Key={"job_id": job_id})
        item = response.get("Item")
        if not item:
            raise NotFoundError(f"job not found: {job_id}")
        return JobRecord(
            job_id=item["job_id"],
            user_id=item["user_id"],
            task_id=item["task_id"],
            schedule=Schedule(**item["schedule"]),
            parameters=item.get("parameters", {}),
            created_at=int(item["created_at"]),
        )

    def get_execution(self, execution_id: str) -> ExecutionRecord:
        response = self.executions.query(
            IndexName="execution_id_index",
            KeyConditionExpression=Key("execution_id").eq(execution_id),
            Limit=1,
        )
        items = response.get("Items", [])
        if not items:
            raise NotFoundError(f"execution not found: {execution_id}")
        return self._item_to_execution(items[0])

    def list_user_executions(
        self,
        user_id: str,
        limit: int = 50,
        status: ExecutionStatus | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[ExecutionRecord]:
        key_condition = Key("user_id").eq(user_id)
        if start_time is not None and end_time is not None:
            key_condition &= Key("execution_time_key").between(
                f"{start_time:010d}#",
                f"{end_time:010d}#~",
            )
        elif start_time is not None:
            key_condition &= Key("execution_time_key").gte(f"{start_time:010d}#")
        elif end_time is not None:
            key_condition &= Key("execution_time_key").lte(f"{end_time:010d}#~")

        response = self.executions.query(
            IndexName=self.user_execution_index,
            KeyConditionExpression=key_condition,
            Limit=limit,
        )
        executions = [self._item_to_execution(item) for item in response.get("Items", [])]
        if status is not None:
            executions = [execution for execution in executions if execution.status == status]
        return executions

    def due_executions(self, now: int, window_seconds: int, limit: int = 500) -> list[ExecutionRecord]:
        response = self.executions.query(
            IndexName=self.status_index,
            KeyConditionExpression=Key("status").eq(ExecutionStatus.PENDING.value)
            & Key("execution_time_key").between(
                "0000000000#",
                f"{now + window_seconds:010d}#~",
            ),
            Limit=limit,
        )
        return [self._item_to_execution(item) for item in response.get("Items", [])]

    def mark_in_progress(self, execution: ExecutionRecord) -> ExecutionRecord:
        return self._transition(
            execution,
            new_status=ExecutionStatus.IN_PROGRESS,
            allowed_statuses={ExecutionStatus.PENDING, ExecutionStatus.RETRYING},
            extra={"attempt": execution.attempt + 1, "error": None},
        )

    def mark_completed(self, execution: ExecutionRecord) -> ExecutionRecord:
        return self._transition(
            execution,
            new_status=ExecutionStatus.COMPLETED,
            allowed_statuses={ExecutionStatus.IN_PROGRESS},
            extra={"error": None},
        )

    def mark_retrying(self, execution: ExecutionRecord, error: str) -> ExecutionRecord:
        return self._transition(
            execution,
            new_status=ExecutionStatus.RETRYING,
            allowed_statuses={ExecutionStatus.IN_PROGRESS},
            extra={"error": error},
        )

    def mark_failed(self, execution: ExecutionRecord, error: str) -> ExecutionRecord:
        return self._transition(
            execution,
            new_status=ExecutionStatus.FAILED,
            allowed_statuses={ExecutionStatus.IN_PROGRESS, ExecutionStatus.RETRYING},
            extra={"error": error},
        )

    def _transition(
        self,
        execution: ExecutionRecord,
        new_status: ExecutionStatus,
        allowed_statuses: set[ExecutionStatus],
        extra: dict[str, Any] | None = None,
    ) -> ExecutionRecord:
        now = epoch_seconds()
        names = {"#status": "status"}
        values: dict[str, Any] = {
            ":updated_at": now,
            ":new_status": new_status.value,
        }
        allowed_placeholders: list[str] = []
        for index, status in enumerate(allowed_statuses):
            placeholder = f":allowed_{index}"
            values[placeholder] = status.value
            allowed_placeholders.append(placeholder)
        set_parts = ["#status = :new_status", "updated_at = :updated_at"]

        for name, value in (extra or {}).items():
            name_placeholder = f"#{name}"
            placeholder = f":{name}"
            names[name_placeholder] = name
            set_parts.append(f"{name_placeholder} = {placeholder}")
            values[placeholder] = value

        try:
            response = self.executions.update_item(
                Key={
                    "time_bucket": execution.time_bucket,
                    "execution_time_key": execution.execution_time_key,
                },
                UpdateExpression="SET " + ", ".join(set_parts),
                ConditionExpression=f"#status IN ({', '.join(allowed_placeholders)})",
                ExpressionAttributeNames=names,
                ExpressionAttributeValues=values,
                ReturnValues="ALL_NEW",
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                raise ConflictError("execution status changed concurrently") from exc
            raise

        return self._item_to_execution(response["Attributes"])

    @staticmethod
    def _execution_to_item(execution: ExecutionRecord) -> dict[str, Any]:
        item = execution.model_dump(mode="json")
        item["time_bucket"] = execution.time_bucket
        item["execution_time_key"] = execution.execution_time_key
        return item

    @staticmethod
    def _item_to_execution(item: dict[str, Any]) -> ExecutionRecord:
        return ExecutionRecord(
            execution_id=item["execution_id"],
            job_id=item["job_id"],
            user_id=item["user_id"],
            scheduled_at=int(item["scheduled_at"]),
            status=ExecutionStatus(item["status"]),
            attempt=int(item.get("attempt", 0)),
            created_at=int(item["created_at"]),
            updated_at=int(item["updated_at"]),
            error=item.get("error"),
        )
