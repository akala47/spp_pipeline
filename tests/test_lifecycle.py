"""Provenance lifecycle against a real Postgres.

    createdb spp_test
    psql spp_test -f schema/01_schema.sql
    psql spp_test -v seed_from=2026-01-01 -v seed_to=2026-12-31 -f schema/02_seed.sql
    SPP_TEST_DSN=postgresql://localhost/spp_test pytest tests/test_lifecycle.py

Skipped when SPP_TEST_DSN is unset, so `pytest` stays offline-clean.

This is the invariant the README claims and nothing else covers: published data
wins, forward-fill only ever writes into holes, and re-running either job is a
no-op. It also exercises the two things that only fail against a real server —
ON CONFLICT matching the partitioned unique constraint, and psycopg2 adapting
whatever the parsers hand it.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta

import pytest

DSN = os.environ.get("SPP_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="SPP_TEST_DSN not set")

if DSN:
    os.environ.setdefault("SPP_DSN", DSN)

DAY = date(2026, 7, 22)
D0 = datetime(2026, 7, 22, 0, 0)
NODE = "TEST_NODE"


def _rows(*specs):
    """(interval, lmp) -> loader-shaped rows."""
    return [
        {"interval_central": ts, "gmt_interval_begin": ts + timedelta(hours=5),
         "settlement_location": NODE, "pnode": NODE, "lmp": lmp,
         "mlc": 0.0, "mcc": 0.0, "mec": lmp,
         "source": "published", "is_forward_filled": False}
        for ts, lmp in specs
    ]


@pytest.fixture()
def conn():
    from spp import db

    with db.connect(DSN) as c:
        with c.cursor() as cur:
            cur.execute("DELETE FROM rt_lmp WHERE settlement_location = %s", (NODE,))
        c.commit()
        yield c
        with c.cursor() as cur:
            cur.execute("DELETE FROM rt_lmp WHERE settlement_location = %s", (NODE,))
        c.commit()


def _load(conn, rows):
    from spp import db
    from spp.feeds import FEEDS

    feed = FEEDS["rt_lmp"]
    return db.upsert_rows(conn, table=feed.table, columns=feed.columns,
                          conflict_keys=feed.conflict_keys, rows=rows,
                          feed=feed.name, partitioned=feed.partitioned)


def _get(conn, ts):
    with conn.cursor() as cur:
        cur.execute("SELECT lmp, source, is_forward_filled FROM rt_lmp "
                    "WHERE settlement_location = %s AND interval_central = %s",
                    (NODE, ts))
        return cur.fetchone()


def test_upsert_is_idempotent(conn):
    rows = _rows((D0, 10.0), (D0 + timedelta(minutes=10), 12.0))
    assert _load(conn, rows) == 2
    _load(conn, rows)
    _load(conn, rows)
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM rt_lmp WHERE settlement_location = %s",
                    (NODE,))
        assert cur.fetchone()[0] == 2


def test_upsert_updates_in_place(conn):
    _load(conn, _rows((D0, 10.0)))
    _load(conn, _rows((D0, 99.5)))
    lmp, source, filled = _get(conn, D0)
    assert float(lmp) == 99.5 and source == "published" and filled is False


def test_publish_fill_republish_lifecycle(conn):
    from spp.fill import forward_fill_day

    hole = D0 + timedelta(minutes=5)
    _load(conn, _rows((D0, 10.0), (D0 + timedelta(minutes=10), 12.0)))
    assert _get(conn, hole) is None

    # 1. fill carries the 00:00 value into 00:05 and marks it
    assert forward_fill_day("rt_lmp", DAY) > 0
    lmp, source, filled = _get(conn, hole)
    assert float(lmp) == 10.0 and source == "forward_filled" and filled is True

    # 2. re-running the fill never touches an existing row
    before = forward_fill_day("rt_lmp", DAY)
    assert before == 0
    assert _get(conn, hole)[1] == "forward_filled"

    # 3. real data arrives late -> published always wins
    _load(conn, _rows((hole, 11.25)))
    lmp, source, filled = _get(conn, hole)
    assert float(lmp) == 11.25 and source == "published" and filled is False

    # 4. and a later fill does not undo that
    forward_fill_day("rt_lmp", DAY)
    assert _get(conn, hole)[1] == "published"


def test_fill_never_writes_past_the_target_day(conn):
    from spp.fill import forward_fill_day

    _load(conn, _rows((D0, 10.0)))
    forward_fill_day("rt_lmp", DAY)
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM rt_lmp WHERE settlement_location = %s "
                    "AND interval_central >= %s", (NODE, D0 + timedelta(days=1)))
        assert cur.fetchone()[0] == 0


def test_spine_gets_real_gmt_from_the_feed(conn):
    """ts_gmt is corrected from SPP's GMTIntervalEnd, not left at the seed value."""
    _load(conn, _rows((D0, 10.0)))
    with conn.cursor() as cur:
        cur.execute("SELECT ts_gmt FROM timestamps WHERE ts_central = %s", (D0,))
        assert cur.fetchone()[0] == D0 + timedelta(hours=5)


def test_spine_id_sql_matches_python(conn):
    """timestamps.id, the SQL spine_id() and spp.spine.spine_id agree."""
    from spp.spine import spine_id

    with conn.cursor() as cur:
        cur.execute(
            "SELECT ts_central, id, spine_id(ts_central) FROM timestamps "
            "WHERE ts_central >= %s AND ts_central < %s ORDER BY ts_central",
            (D0, D0 + timedelta(days=1)),
        )
        rows = cur.fetchall()
    assert len(rows) == 288
    for ts, stored, computed in rows:
        assert stored == computed == spine_id(ts)


def test_fact_rows_carry_spine_id(conn):
    from spp.spine import spine_id

    _load(conn, _rows((D0 + timedelta(hours=17, minutes=45), 30.0)))
    with conn.cursor() as cur:
        cur.execute("SELECT timestamp_id FROM rt_lmp WHERE settlement_location = %s",
                    (NODE,))
        (tid,) = cur.fetchone()
    assert tid == spine_id(D0 + timedelta(hours=17, minutes=45)) == "46225HB17MB45"


# --------------------------------------------------------------------------- #
# timestamp_id is the access path: partition pruning and index use
# --------------------------------------------------------------------------- #
def _plan(conn, sql, params):
    with conn.cursor() as cur:
        cur.execute("EXPLAIN (COSTS OFF) " + sql, params)
        return "\n".join(r[0] for r in cur.fetchall())


def test_rt_partitions_are_bounded_by_spine_id(conn):
    from spp import db

    db.ensure_partitions(conn, "rt_lmp", [D0])
    with conn.cursor() as cur:
        cur.execute("SELECT pg_get_expr(c.relpartbound, c.oid) FROM pg_class c "
                    "WHERE c.relname = 'rt_lmp_y2026m07'")
        (bound,) = cur.fetchone()
    # July 2026 = Excel 46204 (Jul 1) .. 46235 (Aug 1)
    assert bound == "FOR VALUES FROM ('46204HB00MB00') TO ('46235HB00MB00')"


def test_query_by_spine_id_prunes_to_one_partition(conn):
    from spp.spine import spine_id

    plan = _plan(conn,
                 "SELECT lmp FROM rt_lmp WHERE timestamp_id BETWEEN %s AND %s",
                 (spine_id(D0), spine_id(D0 + timedelta(hours=23, minutes=55))))
    assert "rt_lmp_y2026m07" in plan
    assert "rt_lmp_y2026m06" not in plan and "rt_lmp_y2026m08" not in plan


def test_node_over_time_uses_the_dimension_index(conn):
    """One node over a window is served by (settlement_location, timestamp_id),
    index-only, once other nodes share the partition (as in production)."""
    from spp.spine import spine_id

    others = [f"TEST_OTHER_{i}" for i in range(4)]
    day = [D0 + timedelta(minutes=5 * i) for i in range(288)]
    rows = _rows(*[(t, 1.0) for t in day])
    for other in others:
        rows += [dict(r, settlement_location=other, pnode=other)
                 for r in _rows(*[(t, 2.0) for t in day])]
    try:
        _load(conn, rows)
        # Vacuum so the visibility map allows index-only scans — what
        # autovacuum gives a live table.
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("VACUUM ANALYZE rt_lmp_y2026m07")
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET enable_seqscan = off")   # small table: force an index
        plan = _plan(conn,
                     "SELECT timestamp_id, lmp FROM rt_lmp "
                     "WHERE settlement_location = %s AND timestamp_id BETWEEN %s AND %s",
                     (NODE, spine_id(D0), spine_id(D0 + timedelta(days=1))))
        assert "rt_lmp_y2026m07_settlement_location_timestamp_id" in plan
        assert "Index Only Scan" in plan
    finally:
        conn.rollback()
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("DELETE FROM rt_lmp WHERE settlement_location = ANY(%s)",
                        (others,))
        conn.commit()


def test_coverage_counts_by_spine_id(conn):
    from spp import db

    _load(conn, _rows((D0, 1.0), (D0 + timedelta(minutes=5), 2.0),
                      (D0 + timedelta(minutes=10), 3.0)))
    assert db.coverage(conn, table="rt_lmp", start=D0,
                       end=D0 + timedelta(minutes=10)) >= 2
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM rt_lmp WHERE settlement_location = %s "
                    "AND timestamp_id >= %s AND timestamp_id < %s",
                    (NODE, "46225HB00MB00", "46225HB00MB10"))
        assert cur.fetchone()[0] == 2
