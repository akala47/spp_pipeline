"""Structured logging.

Every log line is one JSON object with a stable `event` field, so a log
shipper (Loki, CloudWatch, Datadog) can alert on `event="quality.short"` or
`event="parse.schema_drift"` without regex-scraping free text. That is the
whole point of the `extra={"event": ...}` convention used across the package.

Two helpers carry ambient context so call sites stay clean:

    with bind(feed="rt_lmp", run_id=rid):     # every line inside gets these
        with timed(log, "extract"):          # emits extract.start / extract.ok
            ...

`bind` uses a ContextVar, so it is safe under threads and asyncio and does not
leak across Airflow task instances in the same worker process.
"""
from __future__ import annotations

import contextvars
import json
import logging
import os
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from .config import settings

_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar(
    "spp_log_context", default={}
)

# Attributes present on every LogRecord; anything else came from `extra=`.
_STANDARD = {
    "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
    "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
    "created", "msecs", "relativeCreated", "thread", "threadName",
    "processName", "process", "taskName", "message", "asctime",
}

_configured = False


class _ContextFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        for key, value in _context.get().items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD and not key.startswith("_"):
                payload[key] = _jsonable(value)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class TextFormatter(logging.Formatter):
    """Human-readable fallback for interactive debugging (SPP_LOG_FORMAT=text)."""

    def format(self, record: logging.LogRecord) -> str:
        extras = {
            k: v for k, v in record.__dict__.items()
            if k not in _STANDARD and not k.startswith("_")
        }
        event = extras.pop("event", "-")
        tail = " ".join(f"{k}={v}" for k, v in sorted(extras.items()))
        base = (f"{datetime.fromtimestamp(record.created).strftime('%H:%M:%S')} "
                f"{record.levelname:<7} {event:<28} {record.getMessage()}")
        if tail:
            base = f"{base}  |  {tail}"
        if record.exc_info:
            base = f"{base}\n{self.formatException(record.exc_info)}"
        return base


def _jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return str(value)


def configure(force: bool = False) -> None:
    """Attach one handler to the root logger. Idempotent."""
    global _configured
    if _configured and not force:
        return
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, "_spp", False):
            root.removeHandler(handler)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        TextFormatter() if settings.log_format == "text" else JsonFormatter()
    )
    handler.addFilter(_ContextFilter())
    handler._spp = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(getattr(logging, settings.log_level, logging.INFO))
    _configured = True


def get_logger(name: str) -> logging.Logger:
    configure()
    logger = logging.getLogger(name)
    # Propagate to root so pytest's caplog and Airflow's task handler both see it.
    logger.propagate = True
    return logger


def new_run_id() -> str:
    """Correlates every line emitted by one logical run (or Airflow DAG run)."""
    return os.environ.get("AIRFLOW_CTX_DAG_RUN_ID") or uuid.uuid4().hex[:12]


@contextmanager
def bind(**fields: Any):
    """Add fields to every log line emitted inside the block."""
    current = dict(_context.get())
    current.update({k: v for k, v in fields.items() if v is not None})
    token = _context.set(current)
    try:
        yield
    finally:
        _context.reset(token)


@contextmanager
def timed(log: logging.Logger, event: str, **fields: Any):
    """Emit `<event>.start` / `<event>.ok` / `<event>.error` with elapsed ms."""
    started = time.perf_counter()
    log.debug(f"{event} start", extra={"event": f"{event}.start", **fields})
    try:
        yield
    except Exception:
        elapsed = round((time.perf_counter() - started) * 1000, 1)
        log.error(f"{event} failed",
                  extra={"event": f"{event}.error", "elapsed_ms": elapsed, **fields})
        raise
    else:
        elapsed = round((time.perf_counter() - started) * 1000, 1)
        log.info(f"{event} ok",
                 extra={"event": f"{event}.ok", "elapsed_ms": elapsed, **fields})
