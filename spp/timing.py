"""Time conventions for the whole pipeline. One place, used everywhere.

1. Interval-beginning, Central wall clock
   -------------------------------------
   SPP publishes interval-ENDING times ("Interval", "Market Hour",
   "GMTIntervalEnd"). This database stores interval-BEGINNING times. Every
   parser subtracts one period from SPP's time:

       RT feeds (rt_lmp, rt_mcp)            period = 5 minutes
           SPP 10:40 (ending)   ->  stored 10:35 (beginning)  ...HB10MB35
       hourly feeds (da_lmp, da_mcp, hourly_load)
                                            period = 1 hour
           SPP HE 01:00         ->  stored 00:00              ...HB00MB00
       hourly_load gives "Market Hour" in GMT, hour-ending: 06:00Z on a CDT
       day is HE 01:00 Central. The parser subtracts 1 hour as an instant,
       then converts to America/Chicago.

   The RT file names also use the ending time, so the URL builder adds one
   period: the 10:35 interval is in RTBM-...-YYYYMMDD1040.csv.

   SCADA readings (mw_updates) are point readings, not SPP intervals. A
   reading at 10:47:11 goes on the id of the 5-minute interval that contains
   it (10:45, ...HB10MB45).

2. Run day -> operating day
   ------------------------
   The operating day is the date of the prints (the data). The run day is
   the Central date on which the job runs.

       feed           operating day        example
       da_lmp/da_mcp  run day + 1          a run on Nov 1 loads Nov 2
       hourly_load    run day - 1          a run on Nov 1 loads Oct 31
       rt_lmp/rt_mcp  same day             the interval that the run is for

   The DAGs calculate the run day from the run's data_interval_end (the fire
   time of a cron run), never from the wall clock. The CLI (spp.run) takes
   operating days by default, or run days with --by-run-date.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

CENTRAL = ZoneInfo("America/Chicago")

RT_PERIOD = timedelta(minutes=5)
HOURLY_PERIOD = timedelta(hours=1)

# Operating day minus run day, per feed.
DA_OFFSET_DAYS = 1
HOURLY_LOAD_OFFSET_DAYS = -1
RT_OFFSET_DAYS = 0


def begin_from_end(interval_end: datetime, period: timedelta) -> datetime:
    """SPP interval-ending time -> interval-beginning time stored here."""
    return interval_end - period


def end_from_begin(interval_begin: datetime, period: timedelta) -> datetime:
    """Interval-beginning time -> SPP interval-ending time (file names)."""
    return interval_begin + period


def to_central_naive(when: datetime) -> datetime:
    """Any datetime (aware, or naive Central) -> naive Central wall clock."""
    if when.tzinfo is not None:
        when = when.astimezone(CENTRAL).replace(tzinfo=None)
    return when


def run_day(run_time: datetime) -> date:
    """Central calendar date of a run's fire time (e.g. data_interval_end)."""
    return to_central_naive(run_time).date()


def midnight(day: date) -> datetime:
    return datetime(day.year, day.month, day.day)

