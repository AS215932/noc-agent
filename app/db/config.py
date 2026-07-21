"""Database configuration helpers.

Postgres is the planned authoritative production backend for case state. This
module is intentionally independent from the rest of app.config so the substrate
can be tested and adopted incrementally.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DatabaseSettings:
    url: str = ""
    require_postgres: bool = False
    pool_min_size: int = 1
    pool_max_size: int = 5
    command_timeout_s: float = 10.0
    handoff_delivery_lock_timeout_s: float = 120.0
    statement_timeout_ms: int = 10000
    lock_timeout_ms: int = 5000

    @property
    def enabled(self) -> bool:
        return bool(self.url.strip())

    def assert_ready_for_production(self) -> None:
        """Fail loud instead of silently falling back to in-memory state."""

        if self.require_postgres and not self.enabled:
            raise RuntimeError("Postgres is required but DATABASE_URL/NOC_DATABASE_URL is not set")


def load_database_settings() -> DatabaseSettings:
    return DatabaseSettings(
        url=_env_str("NOC_DATABASE_URL", _env_str("DATABASE_URL", "")),
        require_postgres=_env_bool("NOC_REQUIRE_POSTGRES", False),
        pool_min_size=_env_int("NOC_DATABASE_POOL_MIN_SIZE", 1),
        pool_max_size=_env_int("NOC_DATABASE_POOL_MAX_SIZE", 5),
        command_timeout_s=_env_float("NOC_DATABASE_COMMAND_TIMEOUT_S", 10.0),
        handoff_delivery_lock_timeout_s=_env_float("NOC_HANDOFF_DELIVERY_LOCK_TIMEOUT_S", 120.0),
        statement_timeout_ms=_env_int("NOC_DATABASE_STATEMENT_TIMEOUT_MS", 10000),
        lock_timeout_ms=_env_int("NOC_DATABASE_LOCK_TIMEOUT_MS", 5000),
    )


def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value.strip() if value and value.strip() else default


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value.strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return float(value.strip())
    except ValueError:
        return default
