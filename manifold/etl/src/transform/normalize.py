"""Transformation utilities for normalizing Manifold payloads."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Iterable, List

from ..models import Bet, BetClean, UserClean

logger = logging.getLogger(__name__)


def normalize_user(user: Dict, collected_at: datetime) -> Dict:
    """Return clean record for a user payload."""
    clean_model = UserClean.from_payload(user, collected_at=collected_at)
    return clean_model.to_db_dict()


def normalize_bet(bet_payload: Dict, collected_at: datetime) -> Dict:
    """Return clean record for a bet payload."""
    bet_model = Bet.from_payload(bet_payload)

    clean_model = BetClean.from_bet(bet_model, collected_at=collected_at)
    return clean_model.to_db_dict()


def prepare_records(
    items: Iterable[Dict],
    normalizer,
    *,
    collected_at: datetime,
) -> List[Dict]:
    """Run ``normalizer`` over ``items`` returning clean records."""
    clean_records: List[Dict] = []
    skipped = 0

    for item in items:
        try:
            clean_record = normalizer(item, collected_at=collected_at)
        except ValueError as exc:
            logger.warning("Skipping malformed item %s: %s", item.get("id"), exc)
            skipped += 1
            continue

        clean_records.append(clean_record)

    if skipped:
        logger.info("Skipped %s malformed items in this batch", skipped)

    return clean_records
