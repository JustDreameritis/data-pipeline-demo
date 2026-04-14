"""
Shared utilities: rate limiting, retry, logging helpers, path helpers.
"""

from __future__ import annotations

import functools
import logging
import time
from pathlib import Path
from typing import Any, Callable, TypeVar

import config as cfg

log = logging.getLogger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


# ---------------------------------------------------------------------------
# Rate limiter decorator
# ---------------------------------------------------------------------------

def rate_limit(seconds: float | None = None) -> Callable[[F], F]:
    """
    Decorator that enforces a minimum delay between successive calls.

    Usage::

        @rate_limit(0.5)
        def fetch_page(url: str) -> dict: ...
    """
    delay = seconds if seconds is not None else cfg.general.request_delay

    def decorator(fn: F) -> F:
        last_called: list[float] = [0.0]  # mutable container for closure

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            elapsed = time.monotonic() - last_called[0]
            remaining = delay - elapsed
            if remaining > 0:
                log.debug("Rate limiter: sleeping %.2fs before %s", remaining, fn.__name__)
                time.sleep(remaining)
            last_called[0] = time.monotonic()
            return fn(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator


# ---------------------------------------------------------------------------
# Retry decorator with exponential backoff
# ---------------------------------------------------------------------------

def retry(
    max_attempts: int | None = None,
    base_delay: float = 1.0,
    max_delay: float = 30.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[F], F]:
    """
    Decorator that retries a function on failure with exponential backoff.

    Args:
        max_attempts: Maximum number of attempts (default from config).
        base_delay: Initial wait time in seconds.
        max_delay: Cap on wait time in seconds.
        exceptions: Exception types that trigger a retry.
    """
    attempts = max_attempts if max_attempts is not None else cfg.general.max_retries

    def decorator(fn: F) -> F:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: Exception = RuntimeError("no attempts made")
            for attempt in range(1, attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    last_exc = exc
                    if attempt == attempts:
                        break
                    wait = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    log.warning(
                        "%s failed (attempt %d/%d): %s — retrying in %.1fs",
                        fn.__name__, attempt, attempts, exc, wait,
                    )
                    time.sleep(wait)
            log.error("%s failed after %d attempts: %s", fn.__name__, attempts, last_exc)
            raise last_exc

        return wrapper  # type: ignore[return-value]

    return decorator


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def ensure_dir(path: Path | str) -> Path:
    """Create directory (and parents) if it doesn't exist. Return the Path."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def output_path(filename: str) -> Path:
    """Return an absolute path inside the configured export directory."""
    return ensure_dir(cfg.export.export_dir) / filename


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def get_logger(name: str) -> logging.Logger:
    """Return a named logger (call after configure_logging())."""
    return logging.getLogger(name)
