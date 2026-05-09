from datetime import datetime, timezone

from croniter import croniter
from dateutil.parser import isoparse

from app.models import Schedule, ScheduleType, epoch_seconds


def parse_date_expression(expression: str) -> int:
    value = isoparse(expression)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return epoch_seconds(value.astimezone(timezone.utc))


def next_scheduled_at(schedule: Schedule, after: int | None = None) -> int | None:
    base_epoch = after or epoch_seconds()

    if schedule.type == ScheduleType.IMMEDIATE:
        return base_epoch

    if schedule.type == ScheduleType.DATE:
        return parse_date_expression(schedule.expression or "")

    if schedule.type == ScheduleType.CRON:
        base_dt = datetime.fromtimestamp(base_epoch, tz=timezone.utc)
        next_dt = croniter(schedule.expression or "", base_dt).get_next(datetime)
        return epoch_seconds(next_dt)

    raise ValueError(f"unsupported schedule type: {schedule.type}")
