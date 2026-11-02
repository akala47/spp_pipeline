"""Real-time feeds: RT LMP and RT MCP, every 5 minutes.

Timing
------
SPP publishes each RT file roughly 2m10s-2m15s after the interval begins
(the 22:50-beginning interval, file ...2255.csv, lands at about 22:52:14).
The schedule therefore fires at :02 past each interval and retries every 10
seconds, so a normal run lands within ~10s of publication.

The target interval is derived from the DAG run's own data interval, never
from the wall clock, so every retry and every backfill hits the same interval
regardless of what host or timezone the scheduler runs in.

Which nodes / zones get loaded
------------------------------
Set the Airflow Variable `spp_rt_lmp_groups` (or `spp_rt_mcp_groups`) to a
comma-separated list of group names from `config/groups.json`:

    airflow variables set spp_rt_lmp_groups nppd,kmea
    airflow variables set spp_rt_mcp_groups nppd_zone

Unset falls back to the feed's `default` group. Changing the subset is a
Variable edit — no code deploy, no DAG file change. The file is downloaded once
per interval regardless of how many groups are listed.
"""
from __future__ import annotations

import os
import time
from datetime import datetime, timedelta

import pendulum
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk import Variable, dag, task

from spp import http
from spp.run import ingest
from spp.spine import interval_begin
from spp.timing import RT_PERIOD, to_central_naive

CENTRAL = pendulum.timezone("US/Central")
INTERVAL = RT_PERIOD
# Fire at :02 past, wait out the remaining seconds, then attempt.
# SPP publishes ~2m10-2m15s after the interval begins.
START_OFFSET_SECONDS = int(os.getenv("SPP_START_OFFSET_SECONDS", "15"))

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": int(os.getenv("SPP_TASK_RETRIES", "3")),
    "retry_delay": pendulum.duration(seconds=10),
    "retry_exponential_backoff": False,
    "execution_timeout": pendulum.duration(seconds=60),
    "email_on_failure": False,
}


def _groups_for(feed_name: str) -> list[str] | None:
    raw = Variable.get(f"spp_{feed_name}_groups", default="").strip()
    return [g.strip() for g in raw.split(",") if g.strip()] or None


def _target(data_interval_end) -> datetime:
    """Interval-BEGINNING (Central) to ingest, floored to the 5-minute grid.

    The schedule fires at :02 past, so data_interval_end is e.g. 10:37,
    which floors to 10:35: the interval 10:35-10:40. SPP names that file by
    its ENDING time (...1040.csv); feeds._rt_path adds the 5 minutes.
    """
    return interval_begin(to_central_naive(data_interval_end))


def _build(feed_name: str, description: str):
    @dag(
        dag_id=f"spp_{feed_name}",
        description=description,
        schedule="2,7,12,17,22,27,32,37,42,47,52,57 * * * *",
        start_date=pendulum.datetime(2026, 1, 1, tz=CENTRAL),
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["spp", "realtime", feed_name],
        doc_md=__doc__,
        params={"groups": None},     # override per manual trigger run
    )
    def _dag():
        @task(task_id="ingest")
        def _ingest(params=None, data_interval_end=None, ti=None, **_):
            interval = _target(data_interval_end)
            groups = (params or {}).get("groups") or _groups_for(feed_name)
            # Align to ~2m20s past the interval start. Only on the first
            # attempt: a retry is already well past that point.
            if ti and ti.try_number == 1:
                time.sleep(START_OFFSET_SECONDS)

            try:
                return ingest(feed_name, interval, groups=groups)
            except http.FileNotPublished:
                # Retry while attempts remain — SPP is usually just seconds
                # late. Only the final attempt skips, which keeps the
                # failure-rate metric meaningful; spp_gap_sweeper picks up
                # anything published after the retry budget runs out.
                if ti.try_number > ti.max_tries:
                    raise AirflowSkipException(
                        f"{feed_name} not published for {interval}"
                    )
                raise
            finally:
                http.close()

        @task(task_id="verify")
        def _verify(written=None, params=None, data_interval_end=None, **_):
            from spp import db
            from spp.feeds import FEEDS
            from spp.run import resolve_selections

            feed = FEEDS[feed_name]
            interval = _target(data_interval_end)
            groups = (params or {}).get("groups") or _groups_for(feed_name)
            selections = resolve_selections(feed, groups=groups)
            expected = sum(feed.expected_rows(s) for s in selections)

            with db.connect() as conn:
                found = db.coverage(conn, table=feed.table, start=interval,
                                    end=interval + INTERVAL)
            if found < expected:
                raise ValueError(
                    f"{feed.table}: {found} rows for {interval}, expected "
                    f"{expected} across groups "
                    f"{[s.label for s in selections if s]}"
                )
            return found

        _verify(_ingest())

    return _dag()


rt_lmp_dag = _build("rt_lmp", "SPP RTBM LMP by settlement location, 5-minute")
rt_mcp_dag = _build("rt_mcp", "SPP RTBM MCP by reserve zone, 5-minute")
