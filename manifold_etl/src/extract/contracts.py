"""Extraction helpers for Manifold contracts (markets)."""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

import requests

import config
from ..utils.manifold import ManifoldClient

logger = logging.getLogger(__name__)


def fetch_contracts_for_user(
    client: ManifoldClient,
    user_id: str,
    *,
    page_limit: Optional[int] = None,
) -> List[Dict]:
    """Return all contract payloads for the provided ``user_id``."""
    per_page = page_limit or config.CONTRACT_PAGE_LIMIT
    if per_page <= 0:
        raise ValueError("page_limit must be positive")

    all_contracts: List[Dict] = []
    before: Optional[str] = None

    while True:
        params = {"creatorId": user_id, "limit": per_page}
        if before:
            params["before"] = before
        try:
            page = client.get_json("/markets", params=params)
        except requests.HTTPError as exc:
            logger.warning(
                "Failed to fetch contracts for user %s: %s",
                user_id,
                exc.response.text if exc.response is not None else exc,
            )
            break
        except requests.RequestException as exc:  # pragma: no cover - network failure logs
            logger.warning("Request exception while fetching contracts for %s: %s", user_id, exc)
            break
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Unexpected error while fetching contracts for %s: %s", user_id, exc)
            break

        if not page:
            break

        for contract in page:
            if contract.get("creatorId") == user_id:
                all_contracts.append(contract)

        if len(page) < per_page:
            break

        before = page[-1].get("id")
        if not before:
            break

    return all_contracts
