"""Full ingest path with the network stubbed out. Requires SPP_TEST_DSN.

Covers what the parser tests can't: fetch-once/parse-per-group, the timestamp
spine picking up SPP's real GMT, and the partitioned upsert actually matching
its unique constraint.
"""
from __future__ import annotations

import os
from datetime import datetime

import pytest

DSN = os.environ.get("SPP_TEST_DSN")
FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
pytestmark = [
    pytest.mark.skipif(not DSN, reason="SPP_TEST_DSN not set"),
    pytest.mark.skipif(not os.path.exists(os.path.join(FIXTURES, "rt_lmp_sample.csv")),
                       reason="tests/fixtures/rt_lmp_sample.csv not present"),
]

if DSN:
    os.environ.setdefault("SPP_DSN", DSN)

INTERVAL = datetime(2026, 7, 22, 0, 0)


@pytest.fixture()
def clean():
    from spp import db

    with db.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM rt_lmp WHERE interval_central = %s", (INTERVAL,))
        conn.commit()
    yield
    with db.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM rt_lmp WHERE interval_central = %s", (INTERVAL,))
        conn.commit()


def test_one_download_loads_every_group(monkeypatch, clean):
    from spp import db, http, run

    with open(os.path.join(FIXTURES, "rt_lmp_sample.csv"), "rb") as fh:
        payload = fh.read()

    calls: list[str] = []

    def fake_fetch(url: str) -> bytes:
        calls.append(url)
        return payload

    monkeypatch.setattr(http, "fetch_csv", fake_fetch)

    written = run.ingest("rt_lmp", INTERVAL, groups=["nppd", "kmea"])

    assert written == {"nppd": 1, "kmea": 1}
    assert len(calls) == 1          # fetched once, parsed twice

    with db.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT settlement_location, source FROM rt_lmp "
                    "WHERE interval_central = %s ORDER BY settlement_location",
                    (INTERVAL,))
        assert cur.fetchall() == [("KMEA_EMP2_SECI", "published"),
                                  ("NPPD_BPWN_NPPD", "published")]

        cur.execute("SELECT ts_gmt FROM timestamps WHERE ts_central = %s",
                    (INTERVAL,))
        # 00:05 CDT interval-end -> 05:05 GMT, shifted back 5 min = 05:00
        assert cur.fetchone()[0] == datetime(2026, 7, 22, 5, 0)


def test_reingest_is_idempotent(monkeypatch, clean):
    from spp import db, http, run

    with open(os.path.join(FIXTURES, "rt_lmp_sample.csv"), "rb") as fh:
        payload = fh.read()
    monkeypatch.setattr(http, "fetch_csv", lambda url: payload)

    run.ingest("rt_lmp", INTERVAL, groups=["nppd"])
    run.ingest("rt_lmp", INTERVAL, groups=["nppd"])

    with db.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM rt_lmp WHERE interval_central = %s",
                    (INTERVAL,))
        assert cur.fetchone()[0] == 1
