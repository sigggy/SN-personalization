"""Extraction helpers for Manifold users."""

from __future__ import annotations

from typing import Dict, Iterator, List, Optional

from ..utils.manifold import ManifoldClient


def stream_users(
    client: ManifoldClient,
    *,
    page_size: int = 500,
    before: Optional[str] = None,
    max_pages: Optional[int] = None,
) -> Iterator[Dict]:
    """Yield users from the API, handling pagination."""
    page = 0
    while True:
        params: Dict[str, object] = {"limit": page_size}
        if before:
            params["before"] = before

        batch: List[Dict] = client.get_json("/users", params=params)
        if not batch:
            break

        for user in batch:
            yield user

        before = batch[-1].get("id")
        page += 1

        if len(batch) < page_size:
            break
        if max_pages is not None and page >= max_pages:
            break
