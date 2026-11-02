"""Daily feeds: DA LMP, DA MCP (posted early afternoon for the next operating
day) and the daily hourly-load file (posted for the previous day).

Run day -> operating day (spp/timing.py, Feed.operating_day):

    spp_day_ahead    run on Nov 1  ->  loads the Nov 2 prints   (run day + 1)
    spp_hourly_load  run on Nov 1  ->  loads the Oct 31 prints  (run day - 1)

The run day is the Central date of data_interval_end (the run's fire time),
never the wall clock, so retries and catchup runs load the same day.

Node / zone subsets come from Airflow Variables, same as the RT DAGs:

    airflow variables set spp_da_lmp_groups nppd,kmea
    airflow variables set spp_da_mcp_groups numbered

`hourly_load` is not subsettable — that file has one column per utility rather
than one row, so there are no rows to filter.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task, Variable

from spp import http
from spp.feeds import FEEDS
from spp.run import ingest
from spp.timing import midnight, run_day

CENTRAL = pendulum.timezone("US/Central")

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 4,
    "retry_delay": pendulum.duration(minutes=10),
    "execution_timeout": pendulum.duration(minutes=20),
}


def _groups_for(feed_name: str) -> list[str] | None:
    raw = Variable.get(f"spp_{feed_name}_groups", default="").strip()
    return [g.strip() for g in raw.split(",") if g.strip()] or None

def _resolve_groups(params, feed_name: str) -> list[str] | None:
    """Normalise a params override to a list, or fall back to the Variable.

    A bare string would be iterated character-by-character by
    Selection.resolve, raising UnknownGroup on the first letter.
    """
    override = (params or {}).get("groups")
    if isinstance(override, str):
        override = [g.strip() for g in override.split(",") if g.strip()]
    return override or _groups_for(feed_name)

def _target(feed_name: str, data_interval_end):
    """Operating day (midnight, naive Central) that this run loads."""
    return midnight(FEEDS[feed_name].operating_day(run_day(data_interval_end)))


@dag(
    dag_id="spp_day_ahead",
    description="SPP day-ahead LMP and MCP for the next operating day",
    schedule="0 14 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=CENTRAL),
    catchup=False,
    max_active_runs=1,
    default_args={
        **DEFAULT_ARGS,
        # Publication time drifts; retries cover ~3h from 14:00 Central.
        "retries": 12,
        "retry_delay": pendulum.duration(minutes=15),
    },
    tags=["spp", "day-ahead"],
    doc_md=__doc__,
    params={"groups": None},
)
def spp_day_ahead():
    @task(task_id="ingest_da_lmp")
    def da_lmp(params=None, data_interval_end=None, **_):
        groups = _resolve_groups(params, "da_lmp")
        try:
            return ingest("da_lmp", _target("da_lmp", data_interval_end),
                          groups=groups)
        finally:
            http.close()

    @task(task_id="ingest_da_mcp")
    def da_mcp(params=None, data_interval_end=None, **_):
        groups = _resolve_groups(params, "da_mcp")
        try:
            return ingest("da_mcp", _target("da_mcp", data_interval_end),
                          groups=groups)
        finally:
            http.close()

    da_lmp()
    da_mcp()

@dag(
    dag_id="spp_hourly_load",
    description="SPP daily hourly load by utility for the previous operating day",
    schedule="15 17 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=CENTRAL),
    catchup=True,                 # this one is worth backfilling automatically
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["spp", "load"],
)
def spp_hourly_load():
    @task
    def load(data_interval_end=None, **_):
        # A run on Nov 1 loads the Oct 31 file (run day - 1).
        try:
            return ingest("hourly_load", _target("hourly_load", data_interval_end))
        finally:
            http.close()

    load()


@dag(
    dag_id="spp_gap_sweeper",
    description="Re-ingests any 5-minute interval missing from the RT tables",
    schedule="30 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=CENTRAL),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 1},
    tags=["spp", "repair"],
)
def spp_gap_sweeper():
    """Safety net that makes the double-cron unnecessary.

    Looks back over the last 6 hours and finds intervals where the *configured*
    member count is short — so adding a node group to a Variable automatically
    causes the sweeper to backfill that group over the recent window, without a
    manual backfill command.

    Widening a group therefore triggers a six-hour re-ingest on the next run.
    That is the intent, but it is worth knowing before you edit a Variable at
    peak time.
    """

    @task
    def sweep(feed_name: str, data_interval_end=None, **_):
        from spp import db
        from spp.run import ingest as _ingest, resolve_selections
        from spp.spine import interval_begin, spine_id
        from spp.timing import RT_PERIOD, to_central_naive

        feed = FEEDS[feed_name]
        groups = _groups_for(feed_name)
        selections = resolve_selections(feed, groups=groups)
        expected = sum(feed.expected_rows(s) for s in selections)

        # data_interval_end is this run's fire time, so the window really is
        # "the last 6 hours". logical_date was an hour behind on an hourly
        # schedule, leaving the most recent hour unswept.
        end = interval_begin(to_central_naive(data_interval_end))
        start = end - timedelta(hours=6)

        # Filter and group on timestamp_id: it prunes to the RT partitions the
        # window touches and is served by the unique index.
        with db.connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT timestamp_id, count(*) FROM {feed.table} "
                "WHERE timestamp_id >= %s AND timestamp_id < %s "
                "GROUP BY timestamp_id",
                (spine_id(start), spine_id(end)),
            )
            counts = dict(cur.fetchall())

        repaired = []
        cursor = start
        while cursor < end:
            if counts.get(spine_id(cursor), 0) < expected:
                try:
                    _ingest(feed_name, cursor, groups=groups, strict=False)
                    repaired.append(cursor.isoformat())
                except http.FileNotPublished:
                    pass
            cursor += RT_PERIOD
        http.close()
        return repaired

    sweep.override(task_id="sweep_rt_lmp")("rt_lmp")
    sweep.override(task_id="sweep_rt_mcp")("rt_mcp")


spp_day_ahead()
spp_hourly_load()
spp_gap_sweeper()
