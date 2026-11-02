"""The timestamps spine key.

Every row in `timestamps` is identified by a readable, deterministic id:

    <EXCEL_DATE_SERIAL>HB<hour-beginning>MB<minute-beginning>

    2026-09-03 17:45  ->  46268HB17MB45
    2026-09-03 05:00  ->  46268HB05MB00

The Excel serial is the day count from 1899-12-30 (Excel's effective epoch,
which absorbs its 1900 leap-year bug for every date after 1900-02-28). Hour and
minute are zero-padded, so every id is the same length and ids sort — and range
filter with BETWEEN — in time order as plain text.

Postgres computes the same value with the `spine_id(timestamp)` SQL function
(schema/01_schema.sql), which drives the generated `timestamps.id` column.
This module is the Python twin, for callers that need an id without a database
round-trip. The two must stay identical; tests/test_spine.py checks this module
and tests/test_lifecycle.py checks it against the SQL function.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta

EXCEL_EPOCH = date(1899, 12, 30)

_PATTERN = re.compile(r"^(\d+)HB(\d{2})MB(\d{2})$")


def excel_serial(day: date) -> int:
    """Excel's date serial number for `day` (e.g. 2026-09-03 -> 46268)."""
    if isinstance(day, datetime):
        day = day.date()
    return (day - EXCEL_EPOCH).days


def spine_id(ts: datetime) -> str:
    """Interval-beginning Central wall-clock -> spine id, e.g. '46268HB17MB45'."""
    return f"{excel_serial(ts)}HB{ts.hour:02d}MB{ts.minute:02d}"


def interval_begin(ts: datetime, minutes: int = 5) -> datetime:
    """Floor a Central wall-clock time to the start of its spine interval.

    A reading taken at 10:47:11.3 belongs to the 10:45 interval.
    """
    return ts.replace(minute=ts.minute - ts.minute % minutes, second=0,
                      microsecond=0)


def spine_id_for(ts: datetime) -> str:
    """Spine id of the 5-minute interval containing `ts` (any second/microsecond).

    10:47:11.3 -> '...HB10MB45'. Use this for point-in-time readings;
    spine_id() expects a time already on the grid.
    """
    return spine_id(interval_begin(ts))


def parse_spine_id(value: str) -> datetime:
    """Inverse of spine_id: '46268HB17MB45' -> datetime(2026, 9, 3, 17, 45)."""
    m = _PATTERN.match(value.strip())
    if not m:
        raise ValueError(f"not a spine id: {value!r}")
    serial, hour, minute = (int(g) for g in m.groups())
    if hour > 23 or minute > 59:
        raise ValueError(f"not a spine id: {value!r}")
    day = EXCEL_EPOCH + timedelta(days=serial)
    return datetime(day.year, day.month, day.day, hour, minute)
