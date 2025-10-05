"""Extraction helpers for Manifold users."""

from __future__ import annotations

from typing import Dict, Iterable, Iterator, List, Optional

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


def fetch_users(
    client: ManifoldClient,
    *,
    limit: Optional[int] = None,
    page_size: int = 500,
    before: Optional[str] = None,
) -> List[Dict]:
    """Return a list of users up to ``limit`` records."""
    results: List[Dict] = []
    for user in stream_users(client, page_size=page_size, before=before):
        results.append(user)
        if limit is not None and len(results) >= limit:
            break
    return results
