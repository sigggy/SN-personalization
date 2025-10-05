"""Transformation utilities for normalizing Manifold user payloads."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Iterable, List, Tuple


def _epoch_ms_to_datetime(value: int | float | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc)


def _string_or_none(value) -> str | None:
    if value is None:
        return None
    value_str = str(value).strip()
    return value_str or None


def normalize_user(user: Dict, collected_at: datetime) -> Tuple[Dict, Dict]:
    """Return (raw_record, clean_record) for a user payload."""
    raw_record = {
        "id": user["id"],
        "json_data": user,
        "collected_at": collected_at,
    }

    clean_record = {
        "id": user["id"],
        "username": _string_or_none(user.get("username")),
        "name": _string_or_none(user.get("name") or user.get("displayName")),
        "created_time": _epoch_ms_to_datetime(user.get("createdTime")),
        "is_bot": bool(user.get("isBot")) if user.get("isBot") is not None else None,
        "is_banned": bool(user.get("isBanned")) if user.get("isBanned") is not None else None,
        "total_bets": user.get("totalBets") or user.get("numBets"),
        "total_profit": (user.get("profitCached") or {}).get("allTime"),
        "total_volume": user.get("profitCached", {}).get("sinceCreation")
        if isinstance(user.get("profitCached"), dict)
        else None,
        "last_login": _epoch_ms_to_datetime(user.get("lastBetTime")),
        "collected_at": collected_at,
    }

    return raw_record, clean_record


def prepare_records(
    items: Iterable[Dict],
    normalizer,
    *,
    collected_at: datetime,
) -> Tuple[List[Dict], List[Dict]]:
    """Run ``normalizer`` over ``items`` returning (raw, clean) lists."""
    raw_records: List[Dict] = []
    clean_records: List[Dict] = []

    for item in items:
        raw_record, clean_record = normalizer(item, collected_at)
        raw_records.append(raw_record)
        clean_records.append(clean_record)

    return raw_records, clean_records
