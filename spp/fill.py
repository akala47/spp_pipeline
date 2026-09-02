"""Forward-fill: carry the last known value into missing intervals.

Rules (from the spec):
  * A missing (interval, dimension) is filled with the SAME dimension's value
    at the most recent PRIOR interval — not a neighbouring node, not a later one.
  * A filled row is marked source='forward_filled', is_forward_filled=true, so
    it is never mistaken for published data and never overwrites a published row.
  * The job is bounded to ONE target day: it fills gaps within that day only and
    never writes an interval beyond it. It runs once daily, just before midnight,
    for the day that is ending.

Why this lives in SQL, not a Python row loop: for a whole day the fill is a
single set-based statement per feed — find the day's expected intervals, left
join what exists, and for each hole pull the latest earlier value with a
lateral. One round trip instead of 288 per node.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

from . import db
from .feeds import FEEDS, Feed
from .logging_setup import bind, get_logger, new_run_id, timed

log = get_logger("spp.fill")


def _step(feed: Feed) -> timedelta:
    return timedelta(minutes=5) if feed.cadence == "5min" else timedelta(hours=1)


def _measure_columns(feed: Feed) -> list[str]:
    """Everything that gets carried forward: not keys, not provenance."""
    skip = {"timestamp_id", "interval_central", "source", "is_forward_filled",
            "loaded_at", feed.dimension}
    return [c for c in feed.columns if c not in skip]


def _fill_sql(feed: Feed) -> str:
    """Build the day-bounded forward-fill statement for one feed.

    For each (expected interval, dimension) with no row, take the latest earlier
    row for that dimension (published OR previously filled, so a long gap chains
    forward) whose interval is < the hole and >= the lookback floor, copy its
    measures, stamp it filled, and resolve timestamp_id.

    The dimension universe is bounded to the lookback window rather than
    SELECT DISTINCT over the whole table — on a partitioned RT table that is the
    difference between scanning one month and scanning every month on disk.
    """
    dim = feed.dimension
    measures = _measure_columns(feed)
    set_measures = ", ".join(measures)
    src_measures = ", ".join(f"src.{c}" for c in measures)

    return f"""
    WITH expected AS (
        SELECT g AS interval_central
        FROM generate_series(%(day_start)s::timestamp,
                             %(last_interval)s::timestamp,
                             %(step)s::interval) g
    ),
    dims AS (
        SELECT DISTINCT {dim} AS d
        FROM {feed.table}
        WHERE interval_central >= %(floor)s
          AND interval_central <  %(day_end)s
    ),
    grid AS (
        SELECT e.interval_central, d.d AS {dim}
        FROM expected e CROSS JOIN dims d
    ),
    holes AS (
        SELECT grid.interval_central, grid.{dim}
        FROM grid
        LEFT JOIN {feed.table} f
          ON f.interval_central = grid.interval_central
         AND f.{dim} = grid.{dim}
        WHERE f.id IS NULL
    ),
    filled AS (
        SELECT h.interval_central, h.{dim}, t.id AS timestamp_id, {src_measures}
        FROM holes h
        JOIN LATERAL (
            SELECT {set_measures}
            FROM {feed.table} prev
            WHERE prev.{dim} = h.{dim}
              AND prev.interval_central < h.interval_central
              AND prev.interval_central >= %(floor)s
            ORDER BY prev.interval_central DESC
            LIMIT 1
        ) src ON true
        JOIN timestamps t ON t.ts_central = h.interval_central
    )
    INSERT INTO {feed.table}
        (timestamp_id, interval_central, {dim}, {set_measures},
         source, is_forward_filled)
    SELECT timestamp_id, interval_central, {dim}, {set_measures},
           'forward_filled', true
    FROM filled
    ON CONFLICT ({', '.join(feed.conflict_keys)}) DO NOTHING
    """


def _fill_sql_no_dim(feed: Feed) -> str:
    """hourly_load variant — one series per hour, no dimension cross join."""
    measures = _measure_columns(feed)
    set_measures = ", ".join(measures)
    src_measures = ", ".join(f"src.{c}" for c in measures)
    return f"""
    WITH expected AS (
        SELECT g AS interval_central
        FROM generate_series(%(day_start)s::timestamp,
                             %(last_interval)s::timestamp,
                             %(step)s::interval) g
    ),
    holes AS (
        SELECT e.interval_central
        FROM expected e
        LEFT JOIN {feed.table} f ON f.interval_central = e.interval_central
        WHERE f.id IS NULL
          AND %(day_end)s::timestamp IS NOT NULL
    ),
    filled AS (
        SELECT h.interval_central, t.id AS timestamp_id, {src_measures}
        FROM holes h
        JOIN LATERAL (
            SELECT {set_measures} FROM {feed.table} prev
            WHERE prev.interval_central < h.interval_central
              AND prev.interval_central >= %(floor)s
            ORDER BY prev.interval_central DESC LIMIT 1
        ) src ON true
        JOIN timestamps t ON t.ts_central = h.interval_central
    )
    INSERT INTO {feed.table}
        (timestamp_id, interval_central, {set_measures}, source, is_forward_filled)
    SELECT timestamp_id, interval_central, {set_measures},
           'forward_filled', true
    FROM filled
    ON CONFLICT ({', '.join(feed.conflict_keys)}) DO NOTHING
    """


def expected_intervals(feed: Feed, day: date) -> list[datetime]:
    """The interval-beginning timestamps this feed should have on `day`."""
    start = datetime(day.year, day.month, day.day)
    step = _step(feed)
    n = 288 if feed.cadence == "5min" else 24
    return [start + i * step for i in range(n)]


def forward_fill_day(feed_name: str, day: date, *, lookback_days: int = 7,
                     run_id: str | None = None) -> int:
    """Fill gaps for one feed on one day. Returns rows inserted."""
    feed = FEEDS[feed_name]
    run_id = run_id or new_run_id()

    day_start = datetime(day.year, day.month, day.day)
    day_end = day_start + timedelta(days=1)
    step = _step(feed)
    params = {
        "day_start": day_start,
        "day_end": day_end,
        "last_interval": day_end - step,
        "floor": day_start - timedelta(days=lookback_days),
        "step": "5 minutes" if feed.cadence == "5min" else "1 hour",
    }
    sql = _fill_sql(feed) if feed.dimension else _fill_sql_no_dim(feed)

    with bind(feed=feed_name, run_id=run_id, day=day.isoformat()):
        with timed(log, "forward_fill", table=feed.table):
            with db.connect() as conn:
                if feed.partitioned:
                    db.ensure_partitions(conn, feed.table, [day_start])
                with conn.cursor() as cur:
                    try:
                        cur.execute(sql, params)
                        inserted = cur.rowcount
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        raise
            log.info("forward fill complete",
                     extra={"event": "fill.day_ok", "feed": feed_name,
                            "day": day.isoformat(), "rows_filled": inserted})
    return inserted


def forward_fill_range(feed_name: str, start: date, end: date,
                       *, lookback_days: int = 7) -> int:
    """Fill each day in [start, end]. Used for backfilling historical gaps."""
    run_id = new_run_id()
    total = 0
    day = start
    while day <= end:
        total += forward_fill_day(feed_name, day, lookback_days=lookback_days,
                                  run_id=run_id)
        day += timedelta(days=1)
    return total
