"""AS215932 NOC agent application package."""

# Centralised structured logging per the AS215932 application logging
# contract (hyrule-infra/docs/application-logging.md). Newline-delimited
# JSON to stdout; systemd-journald captures it; the host's Vector agent
# ships to Loki.
from __future__ import annotations

import logging
import sys

import structlog

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts"),
        structlog.contextvars.merge_contextvars,
        structlog.processors.dict_tracebacks,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
    cache_logger_on_first_use=True,
)

log = structlog.get_logger().bind(service="noc-agent")
