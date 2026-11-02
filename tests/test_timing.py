"""The time conventions (spp/timing.py) in every place they apply.

* SPP times are interval-ENDING; stored times are interval-BEGINNING.
  RT: minus 5 minutes. DA and hourly load: minus 1 hour.
* Run day -> operating day: DA +1 day, hourly load -1 day.
No database, no network.
"""
from datetime import date, datetime, timezone

import pendulum
import pytest

from spp.feeds import FEEDS, parse_hourly_load, parse_lmp, parse_rt_mcp
from spp.selection import RESERVE_ZONE, SETTLEMENT_LOCATION, Selection
from spp.spine import spine_id
from spp.timing import HOURLY_PERIOD, RT_PERIOD, run_day

NODE = Selection.from_members(SETTLEMENT_LOCATION, ["N1"], "t")
ZONE = Selection.from_members(RESERVE_ZONE, ["1"], "t")

RT_LMP = b"""Interval,GMTIntervalEnd,Settlement Location,Pnode,LMP,MLC,MCC,MEC
11/01/2026 10:40:00,11/01/2026 16:40:00,N1,P1,20,0,0,20
"""
DA_LMP = b"""Interval,GMTIntervalEnd,Settlement Location,Pnode,LMP,MLC,MCC,MEC
11/02/2026 01:00:00,11/02/2026 07:00:00,N1,P1,20,0,0,20
11/02/2026 24:00:00,11/03/2026 06:00:00,N1,P1,21,0,0,21
""".replace(b"11/02/2026 24:00:00", b"11/03/2026 00:00:00")
RT_MCP = b"""Interval,GMTIntervalEnd,Reserve Zone,RegUPService,RegDNService,RegUPMile,RegDNMile,Spin,Supp,RampUP,RampDN,UncUP
11/01/2026 10:40:00,11/01/2026 16:40:00,1,1,1,1,1,1,1,1,1,1
"""


# --------------------------------------------------------------------------- #
# interval-ending -> interval-beginning
# --------------------------------------------------------------------------- #
def test_feed_periods():
    assert FEEDS["rt_lmp"].period == FEEDS["rt_mcp"].period == RT_PERIOD
    for name in ("da_lmp", "da_mcp", "hourly_load"):
        assert FEEDS[name].period == HOURLY_PERIOD


def test_rt_lmp_minus_five_minutes():
    row = FEEDS["rt_lmp"].parse(RT_LMP, datetime(2026, 11, 1, 10, 35), NODE)[0]
    assert row["interval_central"] == datetime(2026, 11, 1, 10, 35)
    assert row["gmt_interval_begin"] == datetime(2026, 11, 1, 16, 35)
    assert spine_id(row["interval_central"]) == "46327HB10MB35"


def test_rt_mcp_minus_five_minutes():
    row = parse_rt_mcp(RT_MCP, datetime(2026, 11, 1, 10, 35), ZONE)[0]
    assert row["interval_central"] == datetime(2026, 11, 1, 10, 35)


def test_da_lmp_minus_one_hour_he01_is_hb00():
    rows = FEEDS["da_lmp"].parse(DA_LMP, datetime(2026, 11, 2), NODE)
    assert [r["interval_central"] for r in rows] == [
        datetime(2026, 11, 2, 0, 0),       # HE 01:00 -> HB 00:00
        datetime(2026, 11, 2, 23, 0),      # HE 24:00 (next day 00:00) -> HB 23:00
    ]
    assert spine_id(rows[0]["interval_central"]) == "46328HB00MB00"


def test_hourly_load_long_format_is_gmt_cdt():
    # Real file DAILY_HOURLY_LOAD-20261031.csv: Market Hour runs from
    # 10/31 06:00 to 11/01 05:00 GMT (hour-ending) = Oct 31 Central (CDT).
    csv = (b"Market Hour,Balancing Area Name,Control Zone Name,Forecast Area Type,Load MW\n"
           b"10/31/2026 06:00:00,SPP,NPPD,CF,100\n"
           b"10/31/2026 06:00:00,SPP,NPPD,NC,5\n"
           b"11/01/2026 05:00:00,SPP,NPPD,CF,200\n")
    rows = parse_hourly_load(csv, datetime(2026, 10, 31))
    assert [r["interval_central"] for r in rows] == [
        datetime(2026, 10, 31, 0, 0), datetime(2026, 10, 31, 23, 0)]
    assert rows[0]["gmt_interval_begin"] == datetime(2026, 10, 31, 5, 0)
    assert rows[0]["nppd"] == 105
    assert spine_id(rows[0]["interval_central"]) == "46326HB00MB00"


def test_hourly_load_long_format_is_gmt_cst():
    # A CST day starts at 07:00 GMT (hour-ending).
    csv = (b"Market Hour,Control Zone Name,Forecast Area Type,Load MW\n"
           b"01/15/2026 07:00:00,NPPD,CF,100\n"
           b"01/16/2026 06:00:00,NPPD,CF,200\n")
    rows = parse_hourly_load(csv, datetime(2026, 1, 15))
    assert [r["interval_central"] for r in rows] == [
        datetime(2026, 1, 15, 0, 0), datetime(2026, 1, 15, 23, 0)]


def test_hourly_load_dst_fall_back_day_keeps_first_pass():
    # Nov 1 2026: 25 GMT hours, 06:00Z .. 06:00Z next day (hour-ending).
    from datetime import timedelta
    start = datetime(2026, 11, 1, 6)
    csv = b"Market Hour,Control Zone Name,Forecast Area Type,Load MW\n" + b"".join(
        f"{start + timedelta(hours=i):%m/%d/%Y %H:%M:%S},NPPD,CF,{i}\n".encode()
        for i in range(25))
    rows = parse_hourly_load(csv, datetime(2026, 11, 1))
    assert len(rows) == 24
    assert rows[1]["interval_central"] == datetime(2026, 11, 1, 1, 0)
    assert rows[1]["nppd"] == 1                      # first (CDT) pass
    assert rows[1]["gmt_interval_begin"] == datetime(2026, 11, 1, 6, 0)
    assert rows[-1]["interval_central"] == datetime(2026, 11, 1, 23, 0)


def test_hourly_load_wide_format_gmt_minus_one_hour_then_central():
    # GMT hour-ending 06:00Z on Oct 31 (CDT, UTC-5) = 01:00 Central ending
    # -> 00:00 Central beginning.
    csv = b"MarketHour,NPPD\n10/31/2025 06:00:00,100\n"
    row = parse_hourly_load(csv, datetime(2025, 10, 31))[0]
    assert row["interval_central"] == datetime(2025, 10, 31, 0, 0)
    assert row["gmt_interval_begin"] == datetime(2025, 10, 31, 5, 0)


def test_rt_url_adds_five_minutes_for_spp_file_name():
    url = FEEDS["rt_lmp"].url(datetime(2026, 11, 1, 10, 35))
    assert url.endswith("RTBM-LMP-SL-202611011040.csv")


def test_da_url_is_the_operating_day_file():
    assert FEEDS["da_lmp"].url(datetime(2026, 11, 2)).endswith(
        "DA-LMP-SL-202611020100.csv")
    assert FEEDS["hourly_load"].url(datetime(2026, 10, 31)).endswith(
        "DAILY_HOURLY_LOAD-20261031.csv")


# --------------------------------------------------------------------------- #
# run day -> operating day
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("feed, run, prints", [
    ("da_lmp", date(2026, 11, 1), date(2026, 11, 2)),
    ("da_mcp", date(2026, 11, 1), date(2026, 11, 2)),
    ("hourly_load", date(2026, 11, 1), date(2026, 10, 31)),
    ("hourly_load", date(2026, 3, 1), date(2026, 2, 28)),
    ("rt_lmp", date(2026, 11, 1), date(2026, 11, 1)),
])
def test_operating_day(feed, run, prints):
    assert FEEDS[feed].operating_day(run) == prints
    assert FEEDS[feed].run_day(prints) == run


def test_run_day_is_the_central_date_of_the_fire_time():
    # 14:00 Central on Nov 1 = 20:00Z (CST after Nov 1 02:00).
    fire = pendulum.datetime(2026, 11, 1, 20, 0, tz="UTC")
    assert run_day(fire) == date(2026, 11, 1)
    # 01:30Z on Nov 2 is still Nov 1 in Central.
    assert run_day(datetime(2026, 11, 2, 1, 30, tzinfo=timezone.utc)) == date(2026, 11, 1)


def test_cli_by_run_date_converts_per_feed(monkeypatch):
    from spp import run

    seen = {}

    def fake_range(feed, start, end, **_):
        seen[feed] = (start, end)
        return 0

    monkeypatch.setattr(run, "run_range", fake_range)
    run.main(["--feed", "da_lmp", "--start", "2026-11-01", "--by-run-date"])
    run.main(["--feed", "hourly_load", "--start", "2026-11-01",
              "--end", "2026-11-02", "--by-run-date"])
    run.main(["--feed", "da_mcp", "--start", "2026-11-02"])
    assert seen["da_lmp"] == (date(2026, 11, 2), date(2026, 11, 2))
    assert seen["hourly_load"] == (date(2026, 10, 31), date(2026, 11, 1))
    assert seen["da_mcp"] == (date(2026, 11, 2), date(2026, 11, 2))
