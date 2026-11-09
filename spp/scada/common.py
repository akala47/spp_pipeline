"""Shared TRANSFORM and LOAD for the SCADA scrapers. No scraping here.

garden_city.py does not use this module: it is a standalone script with its
own copy of these functions (tests/test_scada.py checks that they agree).

The EXTRACT part (Chrome options, login, page navigation, element ids,
waits) is in each scraper module and is a copy of the old scripts
(beatrice_mw.py, garden_city.py). This module holds what is new:

    TRANSFORM  parse_mw_text   display text -> Decimal MW, half-up 3 dp,
                               with a sanity range
               Reading         timezone-aware read time -> naive Central wall
                               clock, plus the spine id of the 5-minute
                               interval that contains it
    LOAD       record          spine row + partition + idempotent INSERT
                               into mw_updates

Configuration (environment / .env):

    SPP_DSN               Postgres DSN (same database as the SPP feeds;
                          the old scripts' `connection_string` is not read)
    SCADA_CHROME_BINARY   optional Chrome/Chromium path. Set /usr/bin/chromium
                          on the Airflow image. Unset = old behaviour.
    SCADA_CHROMEDRIVER    optional chromedriver path. Set /usr/bin/chromedriver
                          on the Airflow image. Unset = old behaviour.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo

from .. import db
from ..logging_setup import get_logger
from ..spine import interval_begin, spine_id

log = get_logger("spp.scada")

CENTRAL = ZoneInfo("America/Chicago")
TABLE = "mw_updates"

_NUMBER = re.compile(r"[-+]?\d*\.?\d+")


class ReadingUnavailable(ValueError):
    """The HMI is showing a placeholder (e.g. '###.#') instead of a value."""


class ReadingRejected(ValueError):
    """A value was read but failed the sanity check."""


class UnknownLocation(LookupError):
    """The reading's location code is not in scada_locations."""


@dataclass(frozen=True)
class Reading:
    location: str
    reading_timestamp: datetime        # Central wall clock, naive
    mw_reading: Decimal

    @property
    def interval_central(self) -> datetime:
        return interval_begin(self.reading_timestamp)

    @property
    def timestamp_id(self) -> str:
        return spine_id(self.interval_central)

    @classmethod
    def from_aware(cls, location: str, read_at: datetime,
                   mw_reading: Decimal) -> "Reading":
        """Old scripts take datetime.now(CENTRAL); the column is Central wall
        clock without a zone, so convert here (same rule as the old
        push_to_db)."""
        if read_at.tzinfo is None:
            raise ValueError("read_at must be timezone-aware")
        return cls(location, read_at.astimezone(CENTRAL).replace(tzinfo=None),
                   mw_reading)


# --------------------------------------------------------------------------- #
# parsing
# --------------------------------------------------------------------------- #
def parse_mw_text(text: str, *, floor: float | None = None,
                  ceiling: float | None = None) -> Decimal:
    """'38.302937' or '38.302937 MW' -> Decimal('38.303') (half-up, 3 dp).

    Raises ReadingUnavailable when the display shows no number (the HMI's
    '###.#' placeholder), ReadingRejected when the value is outside
    [floor, ceiling].
    """
    match = _NUMBER.search((text or "").replace(",", ""))
    if not match:
        raise ReadingUnavailable(f"no MW value in display text {text!r}")
    value = Decimal(match.group(0))
    if floor is not None and value < Decimal(str(floor)):
        raise ReadingRejected(f"MW value {value} below {floor}")
    if ceiling is not None and value > Decimal(str(ceiling)):
        raise ReadingRejected(f"MW value {value} above {ceiling}")
    return value.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)


# --------------------------------------------------------------------------- #
# browser: deployment paths only (the options are in each scraper)
# --------------------------------------------------------------------------- #
def chrome_kwargs(options) -> dict:
    """Apply SCADA_CHROME_BINARY / SCADA_CHROMEDRIVER, if set.

    Returns the extra keyword arguments for webdriver.Chrome(). With neither
    variable set it returns {} and changes nothing, so webdriver.Chrome()
    starts exactly as in the old scripts. The Airflow image needs both,
    because Chromium and chromedriver come from Debian (packages.txt).
    """
    binary = (os.environ.get("SCADA_CHROME_BINARY") or "").strip()
    if binary:
        options.binary_location = binary
    driver_path = (os.environ.get("SCADA_CHROMEDRIVER") or "").strip()
    if not driver_path:
        return {}
    from selenium.webdriver.chrome.service import Service

    return {"service": Service(executable_path=driver_path)}


def check_env(*names: str) -> None:
    """Stop before Chrome starts if a required setting is missing.

    Without this, the old extract code passes None to driver.get() and Chrome
    reports "'url' must be a string". This check does not change the extract.
    """
    from ..config import MissingConfiguration

    missing = [n for n in names if not (os.environ.get(n) or "").strip()]
    if missing:
        raise MissingConfiguration(
            f"missing settings: {', '.join(missing)}. Set them in .env.")


def append_reading_log(path: str, line: str) -> None:
    """Append one line to a local reading log, as the old scripts did."""
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "a") as fh:
            fh.write(f"{line}\n")
    except OSError as exc:      # a full or read-only disk must not stop a load
        log.warning("could not write reading log",
                    extra={"event": "scada.reading_log_error", "path": path,
                           "error": str(exc)})


# --------------------------------------------------------------------------- #
# database
# --------------------------------------------------------------------------- #
INSERT_SQL = f"""
    INSERT INTO {TABLE} (timestamp_id, location, reading_timestamp, mw_reading)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (timestamp_id, location, reading_timestamp) DO NOTHING
"""


def ensure_spine(conn, intervals) -> None:
    """Make sure each interval has a spine row, without touching existing ones.

    Unlike db.sync_timestamps this never overwrites ts_gmt, which the SPP
    feeds correct from published data.
    """
    pairs = sorted({(t, db._central_to_utc(t)) for t in intervals})
    if not pairs:
        return
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO timestamps (ts_central, ts_gmt) VALUES (%s, %s) "
            "ON CONFLICT (ts_central) DO NOTHING",
            pairs,
        )
    conn.commit()


def write_readings(readings: list[Reading], *, dsn: str | None = None) -> int:
    """Insert readings; a re-sent reading is ignored. Returns rows inserted.

    Raises UnknownLocation when a location code is not in scada_locations.
    """
    if not readings:
        return 0
    import psycopg2


    with db.connect(dsn) as conn:
        ensure_spine(conn, [r.interval_central for r in readings])
        db.ensure_partitions(conn, TABLE, [r.interval_central for r in readings])
        inserted = 0
        try:
            with conn.cursor() as cur:
                for r in readings:
                    cur.execute(INSERT_SQL, (r.timestamp_id, r.location,
                                             r.reading_timestamp, r.mw_reading))
                    inserted += cur.rowcount
            conn.commit()
        except psycopg2.errors.ForeignKeyViolation as exc:
            conn.rollback()
            if "scada_locations" not in str(exc):
                raise
            codes = sorted({r.location for r in readings})
            raise UnknownLocation(
                f"location {', '.join(codes)} is not in scada_locations; "
                "add it there before this scraper can write") from exc
        except Exception:
            conn.rollback()
            raise
    return inserted


def record(reading: Reading, *, dsn: str | None = None) -> int:
    """Write one reading and log it."""
    return record_all([reading], dsn=dsn)


def record_all(readings: list[Reading], *, dsn: str | None = None) -> int:
    """Write readings in one transaction and log each one."""
    inserted = write_readings(readings, dsn=dsn)
    for reading in readings:
        log.info(f"{reading.location} {reading.mw_reading} MW",
                 extra={"event": "scada.reading", "location": reading.location,
                        "timestamp_id": reading.timestamp_id,
                        "reading_timestamp": reading.reading_timestamp.isoformat(),
                        "mw": str(reading.mw_reading)})
    log.info(f"{inserted} of {len(readings)} readings inserted",
             extra={"event": "scada.write", "inserted": inserted,
                    "readings": len(readings)})
    return inserted
