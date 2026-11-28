"""The timestamps spine key.

Every row in `timestamps` is identified by a readable, deterministic id:

    <YYYYMMDD>HB<hour-beginning>MB<minute-beginning>

    2026-09-03 17:45  ->  20260903HB17MB45
    2026-09-03 05:00  ->  20260903HB05MB00

The date is the Central calendar date of the interval beginning. Date, hour
and minute are zero-padded, so every id is 16 characters long and ids sort —
and range filter with BETWEEN — in time order as plain text.

(Until 2026-11 the date part was the Excel date serial, e.g. 46268HB17MB45.
schema/patches/2026-11-23_spine_id_yyyymmdd.sql rewrites an existing database.)

Postgres computes the same value with the `spine_id(timestamp)` SQL function
(schema/01_schema.sql), which drives the generated `timestamps.id` column.
This module is the Python twin, for callers that need an id without a database
round-trip. The two must stay identical; tests/test_spine.py checks this module
and tests/test_lifecycle.py checks it against the SQL function.
"""
from __future__ import annotations

import re
from datetime import datetime

# Exactly 8 date digits: an old Excel-serial id (5 digits) is rejected, so a
# caller that still builds old ids fails loudly instead of matching nothing.
_PATTERN = re.compile(r"^(\d{4})(\d{2})(\d{2})HB(\d{2})MB(\d{2})$")


def spine_id(ts: datetime) -> str:
    """Interval-beginning Central wall-clock -> spine id, e.g. '20260903HB17MB45'."""
    return f"{ts.year:04d}{ts.month:02d}{ts.day:02d}HB{ts.hour:02d}MB{ts.minute:02d}"


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
    """Inverse of spine_id: '20260903HB17MB45' -> datetime(2026, 9, 3, 17, 45)."""
    m = _PATTERN.match(value.strip())
    if not m:
        raise ValueError(f"not a spine id: {value!r}")
    try:
        return datetime(*(int(g) for g in m.groups()))
    except ValueError:
        raise ValueError(f"not a spine id: {value!r}") from None
