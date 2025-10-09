"""HTTP client utilities for communicating with the Manifold Markets API."""

from __future__ import annotations

import logging
import time
from typing import Any, Mapping, Optional

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://manifold.markets/api/v0"


class ManifoldClient:
    """Thin wrapper around :mod:`requests` with retries and exponential backoff."""

    def __init__(
        self,
        *,
        base_url: str = BASE_URL,
        max_retries: int = 5,
        backoff_factor: float = 1.5,
        timeout: int = 30,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor
        self._headers: dict[str, str] = {}

    def _request(self, method: str, path: str, *, params: Optional[Mapping[str, Any]] = None) -> requests.Response:
        url = f"{self.base_url}/{path.lstrip('/')}"
        params = dict(params or {})

        for attempt in range(1, self._max_retries + 1):
            try:
                response = requests.request(
                    method,
                    url,
                    params=params,
                    headers=self._headers,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                logger.warning("Request exception on %s %s: %s", method, url, exc)
                if attempt == self._max_retries:
                    raise
                sleep_for = self._backoff_factor ** (attempt - 1)
                logger.info("Sleeping %.2fs before retry", sleep_for)
                time.sleep(sleep_for)
                continue

            if response.status_code == 429 and attempt < self._max_retries:
                retry_after = response.headers.get("Retry-After")
                sleep_for = float(retry_after) if retry_after else self._backoff_factor ** (attempt - 1)
                logger.info("Rate limited; sleeping %.2fs", sleep_for)
                time.sleep(sleep_for)
                continue

            if response.status_code >= 500 and attempt < self._max_retries:
                sleep_for = self._backoff_factor ** (attempt - 1)
                logger.warning("Server error %s; retrying", response.status_code)
                time.sleep(sleep_for)
                continue

            return response

        raise RuntimeError("Exceeded maximum retries for request")

    def get_json(self, path: str, *, params: Optional[Mapping[str, Any]] = None) -> Any:
        response = self._request("GET", path, params=params)
        response.raise_for_status()
        return response.json()

    def close(self) -> None:  # pragma: no cover - nothing to close when using requests.request
        return

    def __enter__(self) -> "ManifoldClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
