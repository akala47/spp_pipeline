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


def test_scraper_codes_are_in_the_schema():
    """Every code a scraper writes is seeded in 01_schema.sql and the patch."""
    import re
    from pathlib import Path

    from spp.scada import beatrice, garden_city

    root = Path(__file__).resolve().parent.parent / "schema"
    want = {beatrice.LOCATION, *garden_city.XPATHS}
    assert want == {"BEATRICE", "GC_JAMESON", "GC_LEWIS", "GC_TOTAL"}

    def seeded(*files):
        text = "".join((root / f).read_text() for f in files)
        return set(re.findall(r"\('([A-Z][A-Z0-9_]*)',\s*'", text))

    assert want <= seeded("01_schema.sql")
    assert want <= seeded("patches/2026-11-06_scada_locations.sql",
                          "patches/2026-11-08_gc_total.sql")


# --------------------------------------------------------------------------- #
# Garden City: three values from the load SVG page (no browser)
# --------------------------------------------------------------------------- #
def test_svg_xpath_rewrites_only_the_svg_steps():
    from spp.scada.garden_city import svg_xpath

    assert svg_xpath("/html/body/div[1]/div[2]/div/div[2]/svg/g/g[3]/text[14]/tspan") == (
        "/html/body/div[1]/div[2]/div/div[2]/*[local-name()='svg']"
        "/*[local-name()='g']/*[local-name()='g'][3]"
        "/*[local-name()='text'][14]/*[local-name()='tspan']")


def test_svg_xpath_selects_the_copied_nodes():
    lxml = pytest.importorskip("lxml.html")
    from spp.scada.garden_city import XPATHS, svg_xpath

    texts = "".join(f"<text><tspan>t{i}</tspan></text>" for i in range(1, 18))
    inner = "".join(f"<text><tspan>j{i}</tspan></text>" for i in range(1, 15))
    page = ("<html><body><div><div></div><div><div><div></div><div>"
            "<svg xmlns='http://www.w3.org/2000/svg'><g>"
            f"<g></g><g></g><g>{inner}</g>{texts}"
            "</g></svg></div></div></div></div></body></html>")
    doc = lxml.document_fromstring(page)
    got = {loc: doc.xpath(svg_xpath(xp))[0].text_content() for loc, xp in XPATHS.items()}
    assert got == {"GC_LEWIS": "t17", "GC_JAMESON": "j14", "GC_TOTAL": "t3"}


def test_to_readings_keeps_good_values_and_reports_bad_ones(monkeypatch):
    from datetime import timezone

    from selenium.common.exceptions import NoSuchElementException

    from spp.scada import garden_city
    from spp.scada.garden_city import to_readings

    monkeypatch.delenv("GC_MW_CEIL", raising=False)
    monkeypatch.delenv("GC_TOTAL_MW_CEIL", raising=False)
    at = datetime(2026, 11, 8, 16, 47, 11, tzinfo=timezone.utc)   # 10:47 CST
    readings, errors = to_readings(
        {"GC_LEWIS": "3.2105 MW", "GC_JAMESON": "###.#",
         "GC_TOTAL": NoSuchElementException("gone")}, at)
    assert [(r.location, r.mw_reading, r.timestamp_id) for r in readings] == [
        ("GC_LEWIS", Decimal("3.211"), "46334HB10MB45")]
    assert isinstance(errors["GC_JAMESON"], garden_city.ReadingUnavailable)
    assert isinstance(errors["GC_TOTAL"], NoSuchElementException)


def test_total_has_its_own_ceiling(monkeypatch):
    from datetime import timezone

    from spp.scada import garden_city
    from spp.scada.garden_city import to_readings

    monkeypatch.setenv("GC_MW_CEIL", "10")
    monkeypatch.setenv("GC_TOTAL_MW_CEIL", "20")
    at = datetime(2026, 11, 8, 16, 47, tzinfo=timezone.utc)
    readings, errors = to_readings({"GC_JAMESON": "11", "GC_TOTAL": "15"}, at)
    assert [r.location for r in readings] == ["GC_TOTAL"]
    assert isinstance(errors["GC_JAMESON"], garden_city.ReadingRejected)


def test_check_total(monkeypatch):
    from spp.scada.garden_city import Reading as GcReading, check_total

    monkeypatch.delenv("GC_TOTAL_TOLERANCE", raising=False)
    t = datetime(2026, 11, 8, 10, 47)
    rs = [GcReading("GC_JAMESON", t, Decimal("4.1")), GcReading("GC_LEWIS", t, Decimal("3.2")),
          GcReading("GC_TOTAL", t, Decimal("7.5"))]
    assert check_total(rs) == Decimal("0.2")
    assert check_total(rs[:2]) is None


class _FakeElem:
    def __init__(self, text):
        self.text = text

    def get_attribute(self, name):
        return self.text


class _FakeDriver:
    """Answers find_element for the rewritten XPaths; no browser."""

    def __init__(self, values):
        from spp.scada.garden_city import XPATHS, svg_xpath

        self.by_xpath = {svg_xpath(XPATHS[k]): v for k, v in values.items()}
        self.visited = []

    def get(self, url):
        self.visited.append(url)

    def find_element(self, by, value):
        from selenium.common.exceptions import NoSuchElementException

        if value in self.by_xpath:
            return _FakeElem(self.by_xpath[value])
        raise NoSuchElementException(value)


def test_single_run_reads_three_values_from_the_svg_link(monkeypatch, capsys):
    from spp.scada import garden_city

    monkeypatch.setenv("GC_SVG_LINK", "https://hmi/load.svg")
    monkeypatch.setenv("GC_PAGE_WAIT", "0")
    monkeypatch.setattr(garden_city.time, "sleep", lambda s: None)
    driver = _FakeDriver({"GC_JAMESON": "4.100", "GC_LEWIS": "3.200", "GC_TOTAL": "7.300"})
    values, _ = garden_city.scrape_mw_single_run(driver, dry_run=True)
    assert driver.visited == ["https://hmi/load.svg"]      # no login page
    assert values == {"GC_LEWIS": Decimal("3.200"), "GC_JAMESON": Decimal("4.100"),
                      "GC_TOTAL": Decimal("7.300")}
    assert "not written" in capsys.readouterr().out


def test_single_run_fails_only_when_nothing_was_read(monkeypatch):
    from selenium.common.exceptions import NoSuchElementException

    from spp.scada import garden_city

    monkeypatch.setenv("GC_SVG_LINK", "https://hmi/load.svg")
    monkeypatch.setenv("GC_PAGE_WAIT", "0")
    monkeypatch.setattr(garden_city.time, "sleep", lambda s: None)
    with pytest.raises(NoSuchElementException):
        garden_city.scrape_mw_single_run(_FakeDriver({}), dry_run=True)


def test_standalone_copy_matches_the_pipeline():
    """garden_city.py carries its own copy of the spine id, the reading and
    the parser; they must give the same results as the pipeline's."""
    from spp import db, spine
    from spp.scada import common, garden_city as gc

    t = datetime(2019, 1, 1)
    while t < datetime(2028, 1, 1):
        for minute in (0, 4, 5, 47, 59):
            ts = t.replace(minute=minute, second=11, microsecond=300000)
            assert gc.interval_begin(ts) == spine.interval_begin(ts)
            assert gc.spine_id(gc.interval_begin(ts)) == spine.spine_id_for(ts)
            assert gc.central_to_utc(gc.interval_begin(ts)) == db._central_to_utc(
                spine.interval_begin(ts))
        t += timedelta(hours=7, minutes=13)
    for text in ("33.372", "38.302937 MW", "7.2365", "1,234.5", "0"):
        assert gc.parse_mw_text(text) == common.parse_mw_text(text)
    for exc in (gc.ReadingUnavailable, gc.ReadingRejected):
        assert issubclass(exc, ValueError)     # the loop counts these as failures
    assert gc.INSERT_SQL.split() == common.INSERT_SQL.split()


def test_garden_city_runs_outside_the_repository(tmp_path):
    """A copy of the file in an empty folder starts and checks its settings,
    so it imports nothing from this repository."""
    import shutil
    import subprocess
    import sys

    from spp.scada import garden_city

    script = tmp_path / "garden_city.py"
    shutil.copy(garden_city.__file__, script)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("GC_", "SPP_", "PYTHONPATH"))}
    proc = subprocess.run([sys.executable, "-I", str(script), "--once", "--dry-run"],
                          cwd=tmp_path, env=env, capture_output=True, text=True,
                          timeout=60)
    assert proc.returncode == 2, proc.stderr
    assert "missing settings: GC_SVG_LINK" in proc.stderr


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


def test_legacy_garden_city_becomes_gc_jameson():
    a = parse_row(["188161", "GARDEN_CITY", "2026-10-19 10:46:51.5", "7.236"], CENTRAL)
    b = parse_row({"location": "GARDEN_CITY", "reading_timestamp": "2026-10-19 10:46:51.5",
                   "mw_reading": "7.236"}, CENTRAL)
    assert a.location == b.location == "GC_JAMESON"


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
TEST_LOCATIONS = (LOC, "TEST_OTHER_A", "TEST_OTHER_B", "TEST_OTHER_C")
T0 = datetime(2026, 7, 22, 10, 45, 11)


@pytest.fixture()
def conn():
    from spp import db

    with db.connect(DSN) as c:
        def wipe():
            with c.cursor() as cur:
                cur.execute("DELETE FROM mw_updates WHERE location LIKE 'TEST_%%'")
                cur.execute("DELETE FROM scada_locations WHERE location LIKE 'TEST_%%'")
            c.commit()
        wipe()
        with c.cursor() as cur:
            cur.executemany(
                "INSERT INTO scada_locations (location, site) VALUES (%s, 'Test')",
                [(code,) for code in TEST_LOCATIONS])
        c.commit()
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
def test_unknown_location_is_rejected(conn):
    from spp.scada.common import UnknownLocation, write_readings

    with pytest.raises(UnknownLocation, match="TEST_NOT_REGISTERED"):
        write_readings(_readings(1, loc="TEST_NOT_REGISTERED"), dsn=DSN)


@db_only
def test_garden_city_writes_its_own_rows(conn):
    from spp.scada import garden_city as gc

    rows = [gc.Reading(loc, T0, Decimal("4.2")) for loc in TEST_LOCATIONS[:3]]
    assert gc.write_readings(rows, dsn=DSN) == 3
    assert gc.write_readings(rows, dsn=DSN) == 0          # re-sent: ignored
    with pytest.raises(gc.UnknownLocation):
        gc.write_readings([gc.Reading("TEST_NOT_REGISTERED", T0, Decimal(1))], dsn=DSN)


@db_only
def test_scraper_codes_are_registered(conn):
    from spp.scada import beatrice, garden_city

    with conn.cursor() as cur:
        cur.execute("SELECT location FROM scada_locations")
        codes = {r[0] for r in cur.fetchall()}
    assert {beatrice.LOCATION, *garden_city.XPATHS} <= codes


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
