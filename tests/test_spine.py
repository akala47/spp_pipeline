"""Spine id format. No database, no network."""
from datetime import date, datetime, timedelta

import pytest

from spp.spine import excel_serial, parse_spine_id, spine_id


def test_example_from_spec():
    assert spine_id(datetime(2026, 9, 3, 17, 45)) == "46268HB17MB45"
    assert parse_spine_id("46268HB17MB45") == datetime(2026, 9, 3, 17, 45)


def test_hour_and_minute_are_zero_padded():
    assert spine_id(datetime(2026, 9, 3, 5, 0)) == "46268HB05MB00"
    assert spine_id(datetime(2026, 9, 3, 0, 5)) == "46268HB00MB05"


def test_excel_serial_matches_excel():
    # Reference values as Excel itself reports them.
    assert excel_serial(date(1900, 3, 1)) == 61
    assert excel_serial(date(2000, 1, 1)) == 36526
    assert excel_serial(date(2019, 1, 1)) == 43466
    assert excel_serial(date(2026, 9, 3)) == 46268


def test_ids_sort_in_time_order_as_text():
    start = datetime(2026, 3, 7, 0, 0)
    stamps = [start + timedelta(minutes=5 * i) for i in range(3 * 288)]
    ids = [spine_id(t) for t in stamps]
    assert sorted(ids) == ids
    assert len(set(ids)) == len(ids)
    assert len({len(i) for i in ids}) == 1          # fixed width


def test_roundtrip_over_a_day():
    start = datetime(2026, 11, 1, 0, 0)            # DST fall-back day
    for i in range(288):
        t = start + timedelta(minutes=5 * i)
        assert parse_spine_id(spine_id(t)) == t


@pytest.mark.parametrize("bad", ["", "46268", "46268HB17MB4", "46268HB24MB00",
                                 "46268HB7MB45", "x46268HB17MB45"])
def test_parse_rejects_malformed(bad):
    with pytest.raises(ValueError):
        parse_spine_id(bad)
