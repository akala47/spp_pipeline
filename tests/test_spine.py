"""Spine id format. No database, no network."""
import re
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from spp.spine import parse_spine_id, spine_id, spine_id_for


def test_example_from_spec():
    assert spine_id(datetime(2026, 9, 3, 17, 45)) == "20260903HB17MB45"
    assert parse_spine_id("20260903HB17MB45") == datetime(2026, 9, 3, 17, 45)
    assert spine_id_for(datetime(2026, 11, 8, 10, 47, 11)) == "20261108HB10MB45"


def test_date_hour_and_minute_are_zero_padded():
    assert spine_id(datetime(2026, 9, 3, 5, 0)) == "20260903HB05MB00"
    assert spine_id(datetime(2026, 9, 3, 0, 5)) == "20260903HB00MB05"
    assert spine_id(datetime(2026, 1, 2, 0, 0)) == "20260102HB00MB00"


def test_ids_sort_in_time_order_as_text():
    # Spans month and year ends, where a YYYYMMDD id must still sort in order.
    start = datetime(2026, 12, 30, 0, 0)
    stamps = [start + timedelta(minutes=5 * i) for i in range(4 * 288)]
    ids = [spine_id(t) for t in stamps]
    assert sorted(ids) == ids
    assert len(set(ids)) == len(ids)
    assert {len(i) for i in ids} == {16}            # fixed width


def test_roundtrip_over_a_day():
    start = datetime(2026, 11, 1, 0, 0)            # DST fall-back day
    for i in range(288):
        t = start + timedelta(minutes=5 * i)
        assert parse_spine_id(spine_id(t)) == t


@pytest.mark.parametrize("bad", [
    "", "20260903", "20260903HB17MB4", "20260903HB24MB00", "20260903HB7MB45",
    "x20260903HB17MB45", "20260231HB00MB00", "20261301HB00MB00",
    "46268HB17MB45",                                # old Excel-serial id
])
def test_parse_rejects_malformed(bad):
    with pytest.raises(ValueError):
        parse_spine_id(bad)


def test_patch_installs_the_same_sql_function_as_the_schema():
    """schema/01_schema.sql and the migration patch define one spine_id()."""
    schema = Path(__file__).resolve().parent.parent / "schema"

    def body(path):
        text = path.read_text()
        m = re.search(r"FUNCTION spine_id\(ts timestamp\).*?\$\$(.*?)\$\$", text, re.S)
        assert m, path
        return " ".join(m.group(1).split())

    assert body(schema / "01_schema.sql") == body(
        schema / "patches" / "2026-11-23_spine_id_yyyymmdd.sql")
