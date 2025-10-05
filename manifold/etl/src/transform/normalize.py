"""Transformation utilities for normalizing Manifold user payloads."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, List, Tuple

from ..models import UserClean


def normalize_user(user: Dict, collected_at: datetime) -> Tuple[Dict, Dict]:
    """Return (raw_record, clean_record) for a user payload."""
    raw_record = {
        "id": user["id"],
        "json_data": user,
        "collected_at": collected_at,
    }

    clean_model = UserClean.from_payload(user, collected_at=collected_at)
    clean_record = clean_model.to_db_dict()

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
        raw_record, clean_record = normalizer(item, collected_at=collected_at)
        raw_records.append(raw_record)
        clean_records.append(clean_record)

    return raw_records, clean_records
