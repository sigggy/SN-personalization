"""HTTP client utilities for communicating with the Manifold Markets API."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Mapping, MutableMapping, Optional

import requests

from .rate_limit import RateLimiter

logger = logging.getLogger(__name__)

BASE_URL = "https://manifold.markets/api/v0"


class ManifoldClient:
    """Thin wrapper around :mod:`requests` with rate limiting and retries."""

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str = BASE_URL,
        rate_limiter: RateLimiter | None = None,
        max_retries: int = 5,
        backoff_factor: float = 1.5,
        timeout: int = 30,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.timeout = timeout
        self._rate_limiter = rate_limiter or RateLimiter()
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor

        if api_key:
            self.session.headers.update({"Authorization": f"Key {api_key}"})

    def _request(self, method: str, path: str, *, params: Optional[Mapping[str, Any]] = None) -> requests.Response:
        url = f"{self.base_url}/{path.lstrip('/')}"
        params = dict(params or {})

        for attempt in range(1, self._max_retries + 1):
            self._rate_limiter.wait()
            try:
                response = self.session.request(method, url, params=params, timeout=self.timeout)
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

        # Last attempt either returned due to success or we raise.
        raise RuntimeError("Exceeded maximum retries for request")

    def get_json(self, path: str, *, params: Optional[Mapping[str, Any]] = None) -> Any:
        response = self._request("GET", path, params=params)
        response.raise_for_status()
        return response.json()

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "ManifoldClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()
