"""Utilities for rate limiting Manifold API requests."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager

DEFAULT_RPS = 500 / 60  # 500 requests per minute
DEFAULT_MIN_INTERVAL = 1 / DEFAULT_RPS


class RateLimiter:
    """Simple thread-safe rate limiter using sleep-based throttling."""

    def __init__(self, min_interval: float = DEFAULT_MIN_INTERVAL) -> None:
        self._min_interval = min_interval
        self._lock = threading.Lock()
        self._last_request = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request
            sleep_for = self._min_interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)
                now = time.monotonic()
            self._last_request = now

    @contextmanager
    def limit(self) -> None:
        self.wait()
        yield


_GLOBAL_LIMITER = RateLimiter()


def throttle() -> None:
    """Sleep if required to respect the API's rate limit."""
    _GLOBAL_LIMITER.wait()


@contextmanager
def throttled() -> None:
    """Context manager wrapper for the global limiter."""
    with _GLOBAL_LIMITER.limit():
        yield
