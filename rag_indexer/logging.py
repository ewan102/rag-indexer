"""Structured logging configuration with sensitive field filtering."""

import logging
import sys

import structlog

SENSITIVE_KEYS = frozenset({
    "rag_api_key",
    "api_key",
    "bearer",
    "authorization",
    "password",
    "token",
    "secret",
})

# Keys whose value is a cozy-stack download URL: the secret lives in the path
# (/files/downloads/<secret>/<filename>), so the full URL is a credential and
# must never be logged verbatim. We keep the domain/path prefix for debugging.
URL_KEYS = frozenset({"file_url"})


def _redact_url(value):
    """Mask everything after '/downloads/' in a cozy-stack download URL."""
    if not isinstance(value, str):
        return value
    marker = "/downloads/"
    idx = value.find(marker)
    if idx == -1:
        return "<redacted>"
    return value[: idx + len(marker)] + "<redacted>"


def drop_sensitive_keys(logger, method_name, event_dict):
    """Remove sensitive fields from log output entirely and redact URL credentials."""
    for key in list(event_dict.keys()):
        if key.lower() in SENSITIVE_KEYS:
            del event_dict[key]
        elif key.lower() in URL_KEYS:
            event_dict[key] = _redact_url(event_dict[key])
        elif isinstance(event_dict[key], dict):
            event_dict[key] = {
                k: (_redact_url(v) if k.lower() in URL_KEYS else v)
                for k, v in event_dict[key].items()
                if k.lower() not in SENSITIVE_KEYS
            }
    return event_dict


def setup_logging():
    """Configure structlog with JSON rendering, timestamps, and sensitive field scrubbing."""
    from rag_indexer.config import LOG_LEVEL

    level = getattr(logging, LOG_LEVEL, logging.INFO)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.format_exc_info,
            drop_sensitive_keys,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )
