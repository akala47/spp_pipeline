"""Postgres layer.

Three ideas carry all the weight here:

1. **Every write is an idempotent upsert.** The old scripts inserted blindly,
   which is why running a scraper twice was risky and why a re-run after a
   partial failure produced duplicates. With ON CONFLICT DO UPDATE, running the
   same interval five times is indistinguishable from running it once — so the
   defensive double-cron becomes a plain Airflow retry.

2. **The timestamps spine is synced before the facts.** Each load upserts the
   (ts_central, ts_gmt) pairs it is about to reference, taking ts_gmt from
   SPP's own GMTIntervalEnd where the feed publishes one and computing it from
   the America/Chicago rules otherwise. The seed script's `central + 6h`
   placeholder is therefore corrected by real data, and a load can never fail
   because an interval was outside the seeded range.

3. **Partitions are created just in time.** ensure_month_partition() is called
   for the months a batch touches, so RT writes into a future month work
   without a cron job pre-creating tables. RT tables are partitioned on
   timestamp_id, so each month is a spine-id range.

4. **timestamp_id is the access path.** Facts carry the spine id
   (spp/spine.py), computed here from interval_central rather than looked up,
   and every read in this package filters on timestamp_id — the column the
   fact tables are partitioned and indexed on.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
from psycopg2.extensions import AsIs, connection as Connection, register_adapter

from .config import settings
from .logging_setup import get_logger
from .spine import spine_id

log = get_logger("spp.db")

CENTRAL = ZoneInfo("America/Chicago")


# --------------------------------------------------------------------------- #
# type adapters
# --------------------------------------------------------------------------- #
def _register_numpy_adapters() -> None:
    """pandas hands back numpy scalars; psycopg2 refuses to adapt them.

    The parsers already coerce measures to plain floats, so this is a belt-and-
    braces guard for any column that slips through (counts, ints from groupby).
    Without it the first INSERT dies with `can't adapt type 'numpy.float64'`.
    """
    try:
        import numpy as np
    except ImportError:  # pragma: no cover - numpy always ships with pandas
        return
    register_adapter(np.float64, lambda v: AsIs(repr(float(v))))
    register_adapter(np.float32, lambda v: AsIs(repr(float(v))))
    register_adapter(np.int64, lambda v: AsIs(int(v)))
    register_adapter(np.int32, lambda v: AsIs(int(v)))
    register_adapter(np.bool_, lambda v: AsIs("true" if bool(v) else "false"))


_register_numpy_adapters()


@contextmanager
def connect(dsn: str | None = None):
    conn = psycopg2.connect(dsn or settings.require_dsn())
    conn.autocommit = False
    try:
        yield conn
    finally:
        conn.close()


def _columns_sql(cols: Sequence[str]) -> str:
    return ", ".join(cols)


def _update_sql(cols: Sequence[str], keys: Sequence[str]) -> str:
    updatable = [c for c in cols if c not in keys]
    return ", ".join(f"{c} = EXCLUDED.{c}" for c in updatable)


def _central_to_utc(ts: datetime) -> datetime:
    """Naive Central wall-clock -> naive UTC, DST-correct.

    On the fall-back repeat hour the earlier of the two instants is chosen
    (fold=0), matching what SPP's own GMT column reports for the first pass.
    """
    return (ts.replace(tzinfo=CENTRAL, fold=0)
              .astimezone(timezone.utc)
              .replace(tzinfo=None))


# --------------------------------------------------------------------------- #
# spine + partitions
# --------------------------------------------------------------------------- #
def sync_timestamps(conn: Connection, rows: Iterable[dict[str, Any]], *,
                    feed: str) -> int:
    """Ensure every interval in `rows` exists in timestamps, with a real ts_gmt.

    Called before the fact upsert. Makes the load self-seeding: an interval
    outside the range seeded by schema/02_seed.sql is created here
    rather than silently dropping the row.
    """
    pairs: dict[datetime, datetime] = {}
    for row in rows:
        central = row.get("interval_central")
        if central is None:
            continue
        gmt = row.get("gmt_interval_begin") or _central_to_utc(central)
        pairs[central] = gmt
    if not pairs:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO timestamps (ts_central, ts_gmt) VALUES %s "
            "ON CONFLICT (ts_central) DO UPDATE SET ts_gmt = EXCLUDED.ts_gmt",
            sorted(pairs.items()),
            page_size=500,
        )
    conn.commit()
    log.debug("timestamps synced",
              extra={"event": "load.spine_sync", "feed": feed,
                     "intervals": len(pairs)})
    return len(pairs)


def ensure_partitions(conn: Connection, table: str,
                      intervals: Iterable[datetime]) -> None:
    """Create the monthly partitions a batch needs, if they don't exist yet."""
    months = sorted({ts.replace(day=1).date() for ts in intervals if ts})
    if not months:
        return
    with conn.cursor() as cur:
        for month in months:
            cur.execute("SELECT ensure_month_partition(%s, %s)", (table, month))
    conn.commit()


# --------------------------------------------------------------------------- #
# writes
# --------------------------------------------------------------------------- #
def upsert_rows(
    conn: Connection,
    *,
    table: str,
    columns: Sequence[str],
    conflict_keys: Sequence[str],
    rows: Iterable[dict[str, Any]],
    feed: str,
    partitioned: bool = False,
) -> int:
    """Idempotent upsert for the v2 uniform schema.

    Every fact table carries a timestamp_id FK to the spine, so there is ONE
    write path: sync the spine, stamp each row with spine_id(interval_central)
    (deterministic, so no lookup), then INSERT ... ON CONFLICT on the natural
    key. RT (5-min) and DA/load (hourly) differ only in how many source rows
    they carry, not in shape — so both use this.

    Rows must supply every name in `columns` except timestamp_id, plus
    interval_central. A row that already carries timestamp_id keeps it.
    """
    rows = list(rows)
    if not rows:
        log.warning("nothing to write", extra={"event": "load.empty",
                                               "feed": feed, "table": table})
        return 0

    for row in rows:
        row.setdefault("timestamp_id", spine_id(row["interval_central"]))

    # The FK needs every spine row to exist before the facts reference it.
    sync_timestamps(conn, rows, feed=feed)
    if partitioned:
        ensure_partitions(conn, table, (r["interval_central"] for r in rows))

    placeholders = ", ".join(f"%({c})s" for c in columns)
    sql = f"""
        INSERT INTO {table} ({_columns_sql(columns)})
        VALUES ({placeholders})
        ON CONFLICT ({_columns_sql(conflict_keys)})
        DO UPDATE SET {_update_sql(columns, conflict_keys)}
    """
    written = 0
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, row)
                written += cur.rowcount
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    log.info("rows written", extra={"event": "load.ok", "feed": feed,
                                    "table": table, "rows_in": len(rows),
                                    "rows_written": written})
    return written


# One uniform write path for every feed.


def coverage(conn: Connection, *, table: str, start: datetime, end: datetime) -> int:
    """How many rows landed in [start, end) — used by the data-quality checks.

    Filters on timestamp_id, so it prunes RT partitions and uses the index.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {table} "
            f"WHERE timestamp_id >= %s AND timestamp_id < %s",
            (spine_id(start), spine_id(end)),
        )
        return cur.fetchone()[0]


def ping(dsn: str | None = None) -> str:
    """Connectivity check used by `python -m spp.run --check-db`."""
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("SELECT version(), current_database(), "
                    "(SELECT count(*) FROM timestamps)")
        version, database, spine = cur.fetchone()
    return f"{database}: {version.split(',')[0]}, timestamps spine = {spine} rows"
