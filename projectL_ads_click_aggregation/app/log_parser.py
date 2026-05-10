from __future__ import annotations

import json
import shlex
from datetime import datetime, timezone
from typing import Any

from app.models import ClickInput


FIELD_ALIASES = {
    "advertiser": "advertiser_id",
    "advertiserId": "advertiser_id",
    "ad": "ad_id",
    "adId": "ad_id",
    "impression": "impression_id",
    "impressionId": "impression_id",
    "url": "target_url",
    "target": "target_url",
    "redirect_url": "target_url",
    "sig": "signature",
    "ts": "occurred_at",
    "timestamp": "occurred_at",
    "time": "occurred_at",
    "user": "user_id",
    "userId": "user_id",
}


def parse_click_log_line(line: str) -> ClickInput:
    """Parse a click event from JSON or key=value log text."""
    text = line.strip()
    if not text:
        raise ValueError("log line is empty")

    if text.startswith("{"):
        data = json.loads(text)
    else:
        data = _parse_key_value_line(text)

    normalized = _normalize_keys(data)
    missing = [
        field
        for field in (
            "advertiser_id",
            "ad_id",
            "impression_id",
            "target_url",
            "signature",
        )
        if not normalized.get(field)
    ]
    if missing:
        raise ValueError(f"missing required click fields: {', '.join(missing)}")

    occurred_at = normalized.get("occurred_at")
    return ClickInput(
        advertiser_id=str(normalized["advertiser_id"]),
        ad_id=str(normalized["ad_id"]),
        impression_id=str(normalized["impression_id"]),
        target_url=str(normalized["target_url"]),
        signature=str(normalized["signature"]),
        occurred_at=_parse_timestamp(occurred_at) if occurred_at is not None else None,
        user_id=str(normalized["user_id"]) if normalized.get("user_id") else None,
    )


def _parse_key_value_line(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for token in shlex.split(text):
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _normalize_keys(data: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in data.items():
        normalized[FIELD_ALIASES.get(key, key)] = value
    return normalized


def _parse_timestamp(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)

    text = str(value).strip()
    if text.isdigit():
        return int(text)

    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())

