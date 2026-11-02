"""Parser and URL tests. No database, no network."""
from datetime import datetime, timedelta

import pytest

from spp.feeds import FEEDS, parse_da_mcp, parse_lmp, parse_rt_mcp
from spp.selection import RESERVE_ZONE, SETTLEMENT_LOCATION, Selection

DA_LMP_CSV = b"""Interval,GMTIntervalEnd,Settlement Location,Pnode,LMP,MLC,MCC,MEC
07/22/2026 01:00:00,07/22/2026 06:00:00,NPPD_BPWN_NPPD,NPPD_BPWN,24.51,-0.12,0.00,24.63
07/22/2026 02:00:00,07/22/2026 07:00:00,NPPD_BPWN_NPPD,NPPD_BPWN,22.10,-0.10,0.00,22.20
07/22/2026 01:00:00,07/22/2026 06:00:00,KMEA_EMP2_SECI,KMEA_EMP2,25.00,0.00,0.00,25.00
07/22/2026 01:00:00,07/22/2026 06:00:00,SOME_OTHER_NODE,X,99.00,0,0,99.00
"""

RT_MCP_CSV = b"""Interval,GMTIntervalEnd,Reserve Zone,RegUPService,RegDNService, RegUPMile, RegDNMile,Spin,Supp,RampUP,RampDN,UncUP
07/21/2026 14:40:00,07/21/2026 19:40:00,1,5.1,4.2,0.3,0.2,6.0,1.0,0.5,0.4,0.0
07/21/2026 14:40:00,07/21/2026 19:40:00,5,5.5,4.5,0.5,0.5,6.5,1.5,0.5,0.5,0.0
07/21/2026 14:40:00,07/21/2026 19:40:00,SPP,7.0,6.0,0.7,0.6,8.0,2.0,0.7,0.6,0.0
"""

NPPD = Selection.from_members(SETTLEMENT_LOCATION, ["NPPD_BPWN_NPPD"], "nppd")
BOTH_NODES = Selection.from_members(
    SETTLEMENT_LOCATION, ["NPPD_BPWN_NPPD", "KMEA_EMP2_SECI"], "core")
ZONE_5 = Selection.from_members(RESERVE_ZONE, ["5"], "nppd_zone")


def _lmp(csv, selection):
    return parse_lmp(csv, datetime(2026, 7, 22), selection,
                     period=timedelta(hours=1), feed="da_lmp")


# --------------------------------------------------------------------------- #
# subsetting
# --------------------------------------------------------------------------- #
def test_lmp_loads_only_the_selected_node():
    rows = _lmp(DA_LMP_CSV, NPPD)
    assert {r["settlement_location"] for r in rows} == {"NPPD_BPWN_NPPD"}
    assert len(rows) == 2


def test_lmp_widening_the_selection_picks_up_more_nodes():
    assert len(_lmp(DA_LMP_CSV, BOTH_NODES)) == 3


def test_mcp_loads_only_the_selected_zone():
    rows = parse_rt_mcp(RT_MCP_CSV, datetime(2026, 7, 21, 14, 35), ZONE_5)
    assert [r["reserve_zone"] for r in rows] == ["5"]


def test_absent_member_warns_but_does_not_crash(caplog):
    ghost = Selection.from_members(SETTLEMENT_LOCATION,
                                   ["NPPD_BPWN_NPPD", "TYPO_NODE"], "typo")
    rows = _lmp(DA_LMP_CSV, ghost)
    assert len(rows) == 2
    assert any(getattr(r, "event", "") == "parse.member_missing"
               for r in caplog.records)


def test_expected_rows_scales_with_selection_size():
    assert FEEDS["rt_lmp"].expected_rows(NPPD) == 1
    assert FEEDS["rt_lmp"].expected_rows(BOTH_NODES) == 2
    assert FEEDS["da_lmp"].expected_rows(BOTH_NODES) == 48
    assert FEEDS["hourly_load"].expected_rows(None) == 24


# --------------------------------------------------------------------------- #
# selection resolution
# --------------------------------------------------------------------------- #
def test_group_references_expand_transitively():
    groups = {SETTLEMENT_LOCATION: {"a": ["N1"], "b": ["N2"],
                                    "both": ["@a", "@b"]}}
    sel = Selection.from_group(SETTLEMENT_LOCATION, "both", groups)
    assert sel.members == ("N1", "N2")


def test_duplicate_members_are_collapsed():
    groups = {RESERVE_ZONE: {"a": ["1"], "dupe": ["@a", "1", "5"]}}
    assert Selection.from_group(RESERVE_ZONE, "dupe", groups).members == ("1", "5")


def test_unknown_group_fails_loudly():
    from spp.selection import UnknownGroup

    with pytest.raises(UnknownGroup):
        Selection.from_group(RESERVE_ZONE, "nope", {RESERVE_ZONE: {"a": ["1"]}})


def test_circular_group_reference_is_caught():
    groups = {RESERVE_ZONE: {"a": ["@b"], "b": ["@a"]}}
    with pytest.raises(ValueError):
        Selection.from_group(RESERVE_ZONE, "a", groups)


def test_empty_selection_rejected():
    from spp.selection import EmptySelection

    with pytest.raises(EmptySelection):
        Selection.from_members(RESERVE_ZONE, [])


def test_resolve_precedence_members_over_groups():
    from spp.run import resolve_selections

    sel = resolve_selections(FEEDS["rt_mcp"], groups=["numbered"],
                             members=["SPP"])
    assert sel[0].members == ("SPP",)


def test_non_subsettable_feed_gets_a_single_none():
    from spp.run import resolve_selections

    assert resolve_selections(FEEDS["hourly_load"], groups=["nppd"]) == [None]


def test_multiple_groups_resolve_to_multiple_selections():
    from spp.run import resolve_selections

    sels = resolve_selections(FEEDS["rt_lmp"], groups=["nppd", "kmea"])
    assert [s.label for s in sels] == ["nppd", "kmea"]


# --------------------------------------------------------------------------- #
# parsing / URLs
# --------------------------------------------------------------------------- #
def test_shifts_to_interval_begin():
    rows = _lmp(DA_LMP_CSV, NPPD)
    assert rows[0]["interval_central"] == datetime(2026, 7, 22, 0, 0)


def test_rt_mcp_tolerates_leading_space_headers():
    rows = parse_rt_mcp(RT_MCP_CSV, datetime(2026, 7, 21, 14, 35), ZONE_5)
    assert rows[0]["regupmile"] == 0.5        # header was ' RegUPMile'
    assert rows[0]["interval_central"] == datetime(2026, 7, 21, 14, 35)


def test_schema_drift_raises_not_silently_skips():
    with pytest.raises(KeyError):
        parse_da_mcp(b"Interval,Nope\n1,2\n", datetime(2026, 7, 22), ZONE_5)


@pytest.mark.parametrize("name", sorted(FEEDS))
def test_urls_are_wellformed(name):
    url = FEEDS[name].url(datetime(2026, 7, 21, 14, 35))
    assert url.startswith("https://portal.spp.org/")
    assert url.endswith(".csv")


def test_rt_midnight_uses_previous_day_folder():
    # Interval-beginning 23:55 on Jul 20 ends at 00:00 Jul 21; SPP files that
    # midnight-ending interval under the Jul 20 folder.
    url = FEEDS["rt_lmp"].url(datetime(2026, 7, 20, 23, 55))
    assert "%2F07%2FBy_Interval%2F20%2F" in url
    assert url.endswith("RTBM-LMP-SL-202607210000.csv")


# --------------------------------------------------------------------------- #
# new-build: provenance columns, date-range CLI, fill SQL
# --------------------------------------------------------------------------- #
def test_every_feed_carries_provenance_columns():
    for feed in FEEDS.values():
        assert "source" in feed.columns
        assert "is_forward_filled" in feed.columns


def test_prepare_stamps_published_provenance():
    from spp.run import _prepare
    feed = FEEDS["rt_lmp"]
    rows = [{"interval_central": datetime(2026, 7, 22, 0, 0),
             "settlement_location": "NPPD_BPWN_NPPD", "lmp": 1.0}]
    out = _prepare(rows, feed)
    assert out[0]["source"] == "published"
    assert out[0]["is_forward_filled"] is False


def test_cli_requires_start():
    import pytest as _pytest
    from spp.run import main
    with _pytest.raises(SystemExit):
        main(["--feed", "rt_lmp"])


def test_cli_rejects_end_before_start():
    import pytest as _pytest
    from spp.run import main
    with _pytest.raises(SystemExit):
        main(["--feed", "rt_lmp", "--start", "2026-07-10", "--end", "2026-07-01"])


def test_fill_sql_builds_for_every_feed():
    import re
    import sqlglot
    from spp.fill import _fill_sql, _fill_sql_no_dim
    for feed in FEEDS.values():
        sql = _fill_sql(feed) if feed.dimension else _fill_sql_no_dim(feed)
        probe = re.sub(r"%\((\w+)\)s", "NULL", sql)
        sqlglot.parse_one(probe, read="postgres")   # raises on malformed SQL


def test_gmt_not_in_fact_columns_but_parsed():
    # GMT is written to the timestamps spine, not stored per fact row.
    for feed in FEEDS.values():
        assert "gmt_interval_begin" not in feed.columns


# --------------------------------------------------------------------------- #
# conflict keys: timestamp_id leads every natural key, and is the RT
# partition key, which Postgres requires in every unique constraint.
# --------------------------------------------------------------------------- #
def test_every_feed_conflicts_on_timestamp_id():
    for feed in FEEDS.values():
        assert feed.conflict_keys[0] == "timestamp_id"
        assert "interval_central" not in feed.conflict_keys
        if feed.dimension:
            assert feed.conflict_keys == ("timestamp_id", feed.dimension)


# --------------------------------------------------------------------------- #
# real-file regression fixtures — trimmed samples of actual SPP downloads.
# These lock in the exact headers/format SPP ships, so a silent upstream change
# (like the load feed's long-format switch, or RegUpMile casing) fails loudly.
# --------------------------------------------------------------------------- #
import os as _os

_FIX = _os.path.join(_os.path.dirname(__file__), "fixtures")


def _fixture(name):
    path = _os.path.join(_FIX, f"{name}_sample.csv")
    if not _os.path.exists(path):
        pytest.skip(f"fixture {name}_sample.csv not in tests/fixtures")
    with open(path, "rb") as fh:
        return fh.read()


def test_real_da_lmp_parses():
    from spp.feeds import FEEDS
    sel = Selection.from_members(SETTLEMENT_LOCATION, ["AEC"], "t")
    rows = FEEDS["da_lmp"].parse(_fixture("da_lmp"), datetime(2026, 7, 10), sel)
    assert rows and rows[0]["settlement_location"] == "AEC"


def test_real_rt_lmp_parses_with_trailing_baa_column():
    from spp.feeds import FEEDS
    sel = Selection.from_members(SETTLEMENT_LOCATION, ["AEC"], "t")
    rows = FEEDS["rt_lmp"].parse(_fixture("rt_lmp"), datetime(2026, 7, 22), sel)
    assert rows and rows[0]["interval_central"] == datetime(2026, 7, 22, 0, 0)


def test_real_da_mcp_parses():
    from spp.feeds import FEEDS
    sel = Selection.from_members(RESERVE_ZONE, ["1"], "t")
    rows = FEEDS["da_mcp"].parse(_fixture("da_mcp"), datetime(2026, 7, 16), sel)
    assert rows and rows[0]["reserve_zone"] == "1"


def test_real_rt_mcp_parses_despite_regupmile_casing():
    # File header is 'RegUpMile' (lowercase p); parser must normalise it.
    from spp.feeds import FEEDS
    sel = Selection.from_members(RESERVE_ZONE, ["1"], "t")
    rows = FEEDS["rt_mcp"].parse(_fixture("rt_mcp"), datetime(2026, 7, 22), sel)
    assert rows and rows[0]["regupmile"] is not None


def test_real_hourly_load_long_format_pivots_and_sums():
    # Long format: one row per (hour, zone, forecast_area_type). NPPD splits into
    # CF + NC which must sum. One full hour in the fixture -> one output row.
    from spp.feeds import FEEDS
    rows = FEEDS["hourly_load"].parse(_fixture("hourly_load"), datetime(2026, 7, 18))
    assert len(rows) == 1
    r = rows[0]
    # 06:00 GMT hour-ending = 01:00 CDT hour-ending -> 00:00 Central beginning
    assert r["interval_central"] == datetime(2026, 7, 18, 0, 0)
    assert abs(r["nppd"] - (3686.212 + 165.787)) < 0.001          # CF + NC summed
    assert r["csws"] == 6170.903


def test_new_control_zones_are_not_dropped():
    # PRPA / WACM / WAUW appeared upstream in July 2026 and used to be pivoted
    # and then silently discarded.
    from spp.feeds import FEEDS
    rows = FEEDS["hourly_load"].parse(_fixture("hourly_load"), datetime(2026, 7, 18))
    assert rows[0]["prpa"] == 571.220
    assert rows[0]["wacm"] == 1844.960
    assert rows[0]["wauw"] == 233.415


def test_measures_are_plain_floats_psycopg2_can_adapt():
    # numpy scalars from iterrows() are the classic "can't adapt type" failure.
    rows = _lmp(DA_LMP_CSV, NPPD)
    assert type(rows[0]["lmp"]) is float
