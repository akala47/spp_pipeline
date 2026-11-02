"""SCADA readings: parsing, spine ids, legacy import, mw_updates writes.

Offline tests always run. Database tests need SPP_TEST_DSN (schema from
schema/01_schema.sql + 02_seed.sql) and are skipped otherwise. No browser is started anywhere.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from spp.scada.common import (Reading, ReadingRejected, ReadingUnavailable,
                              parse_mw_text)
from spp.scada.import_legacy import BadRow, parse_row
from spp.spine import interval_begin, spine_id_for

CENTRAL = ZoneInfo("America/Chicago")


# --------------------------------------------------------------------------- #
# parsing
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("text, want", [
    ("33.372", "33.372"),
    ("38.302937 MW", "38.303"),        # half-up to 3 dp
    ("7.2365", "7.237"),
    ("1,234.5", "1234.500"),
])
def test_parse_mw_text(text, want):
    assert parse_mw_text(text) == Decimal(want)


def test_hmi_placeholder_is_unavailable_not_an_error():
    with pytest.raises(ReadingUnavailable):
        parse_mw_text("###.#")          # 59 of these in the old GC log


@pytest.mark.parametrize("text", ["-0.5", "10.001"])
def test_sanity_check(text):
    with pytest.raises(ReadingRejected):
        parse_mw_text(text, floor=0, ceiling=10)


# --------------------------------------------------------------------------- #
# readings -> spine ids
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("ts, want", [
    (datetime(2026, 9, 3, 10, 47, 11, 300000), "46268HB10MB45"),
    (datetime(2026, 9, 3, 10, 49, 59, 999999), "46268HB10MB45"),
    (datetime(2026, 9, 3, 10, 50, 0), "46268HB10MB50"),
    (datetime(2026, 9, 3, 0, 4, 59), "46268HB00MB00"),
])
def test_reading_gets_its_interval_id(ts, want):
    r = Reading("BEATRICE", ts, Decimal("33.372"))
    assert r.timestamp_id == spine_id_for(ts) == want
    assert r.interval_central == interval_begin(ts)


def test_a_minute_cadence_gives_five_readings_per_id():
    start = datetime(2026, 9, 3, 10, 45, 11)
    ids = [Reading("BEATRICE", start + timedelta(minutes=i), Decimal(1)).timestamp_id
           for i in range(10)]
    assert ids == ["46268HB10MB45"] * 5 + ["46268HB10MB50"] * 5


def test_aware_read_time_becomes_central_wall_clock():
    from datetime import timezone

    # 15:47:11Z on Nov 2 2026 = 09:47:11 CST.
    r = Reading.from_aware("BEATRICE",
                           datetime(2026, 11, 2, 15, 47, 11, tzinfo=timezone.utc),
                           Decimal("33.372"))
    assert r.reading_timestamp == datetime(2026, 11, 2, 9, 47, 11)
    assert r.timestamp_id == "46328HB09MB45"
    with pytest.raises(ValueError):
        Reading.from_aware("BEATRICE", datetime(2026, 11, 2, 9, 47), Decimal(1))


def test_chrome_paths_change_nothing_when_unset(monkeypatch):
    from spp.scada.common import chrome_kwargs

    class Opts:
        binary_location = ""

    monkeypatch.delenv("SCADA_CHROME_BINARY", raising=False)
    monkeypatch.delenv("SCADA_CHROMEDRIVER", raising=False)
    opts = Opts()
    assert chrome_kwargs(opts) == {} and opts.binary_location == ""
    monkeypatch.setenv("SCADA_CHROME_BINARY", "/usr/bin/chromium")
    chrome_kwargs(opts)
    assert opts.binary_location == "/usr/bin/chromium"


# --------------------------------------------------------------------------- #
# legacy CSV rows
# --------------------------------------------------------------------------- #
def test_legacy_row_with_header():
    row = {"id": "188162", "location": "BEATRICE",
           "reading_timestamp": "2026-10-19 10:47:11.3", "mw_reading": "33.372"}
    r = parse_row(row, CENTRAL)
    assert (r.location, r.reading_timestamp, r.mw_reading, r.timestamp_id) == (
        "BEATRICE", datetime(2026, 10, 19, 10, 47, 11, 300000),
        Decimal("33.372"), "46314HB10MB45")


def test_legacy_row_without_header_any_column_order():
    a = parse_row(["188162", "BEATRICE", "2026-10-19 10:47:11.3", "33.372"], CENTRAL)
    b = parse_row(["188162", "BEATRICE", "33.372", "2026-10-19 10:47:11.3"], CENTRAL)
    assert a == b


def test_excel_mangled_time_is_rejected():
    # What the sample looks like after Excel: minutes:seconds, no date.
    with pytest.raises(BadRow, match="no date"):
        parse_row(["188161", "GARDEN_CITY", "46:51.5", "7.236"], CENTRAL)


def test_legacy_utc_timestamps_convert_to_central():
    r = parse_row(["1", "BEATRICE", "2025-08-22T16:57:47+00:00", "39.748"], CENTRAL)
    assert r.reading_timestamp == datetime(2025, 8, 22, 11, 57, 47)   # CDT


# --------------------------------------------------------------------------- #
# database
# --------------------------------------------------------------------------- #
DSN = os.environ.get("SPP_TEST_DSN")
db_only = pytest.mark.skipif(not DSN, reason="SPP_TEST_DSN not set")
if DSN:
    os.environ.setdefault("SPP_DSN", DSN)

LOC = "TEST_SITE"
T0 = datetime(2026, 7, 22, 10, 45, 11)


@pytest.fixture()
def conn():
    from spp import db

    with db.connect(DSN) as c:
        def wipe():
            with c.cursor() as cur:
                cur.execute("DELETE FROM mw_updates WHERE location LIKE 'TEST_%%'")
            c.commit()
        wipe()
        yield c
        c.rollback()
        wipe()


def _readings(n=5, start=T0, loc=LOC):
    return [Reading(loc, start + timedelta(minutes=i), Decimal("33.1") + i)
            for i in range(n)]


@db_only
def test_write_is_idempotent_and_groups_by_interval(conn):
    from spp.scada.common import write_readings

    rows = _readings(7)
    assert write_readings(rows, dsn=DSN) == 7
    assert write_readings(rows, dsn=DSN) == 0          # re-sent readings ignored
    with conn.cursor() as cur:
        cur.execute("SELECT timestamp_id, count(*) FROM mw_updates "
                    "WHERE location = %s GROUP BY 1 ORDER BY 1", (LOC,))
        assert cur.fetchall() == [("46225HB10MB45", 5), ("46225HB10MB50", 2)]


@db_only
def test_table_rejects_a_mismatched_timestamp_id(conn):
    import psycopg2

    with pytest.raises(psycopg2.errors.CheckViolation):
        with conn.cursor() as cur:
            cur.execute("INSERT INTO mw_updates VALUES (%s, %s, %s, %s)",
                        ("46225HB10MB50", LOC, T0, 1))     # T0 is in MB45


@db_only
def test_location_over_time_prunes_and_uses_the_index(conn):
    from spp.scada.common import write_readings
    from spp.spine import spine_id

    rows = _readings(288)
    for other in ("TEST_OTHER_A", "TEST_OTHER_B", "TEST_OTHER_C"):
        rows += _readings(288, loc=other)
    write_readings(rows, dsn=DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("VACUUM ANALYZE mw_updates_y2026m07")
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SET enable_seqscan = off")
        cur.execute("EXPLAIN (COSTS OFF) SELECT reading_timestamp, mw_reading "
                    "FROM mw_updates WHERE location = %s "
                    "AND timestamp_id BETWEEN %s AND %s",
                    (LOC, spine_id(datetime(2026, 7, 22)),
                     spine_id(datetime(2026, 7, 22, 23, 55))))
        plan = "\n".join(r[0] for r in cur.fetchall())
    assert "mw_updates_y2026m07" in plan and "mw_updates_y2026m06" not in plan
    assert "Index Only Scan" in plan and "location_timestamp_id" in plan


@db_only
def test_import_legacy_csv(conn, tmp_path):
    from spp.scada.import_legacy import import_csv

    dump = tmp_path / "dump.csv"
    dump.write_text(
        "id,location,reading_timestamp,mw_reading\n"
        f"1,{LOC},2026-07-22 10:46:51.5,7.236\n"
        f"2,{LOC},2026-07-22 10:48:13.3,7.236\n"
        f"3,{LOC},48:13.3,7.236\n"                    # Excel-mangled: rejected
    )
    stats = import_csv(str(dump))
    assert stats["inserted"] == 2 and stats["bad"] == 1
    assert import_csv(str(dump))["inserted"] == 0    # re-run is a no-op
