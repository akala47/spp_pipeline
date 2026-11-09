"""Garden City site load scraper: a standalone script for the VPN device.

It does not import anything from this repository, so you can copy this one
file to any computer and run it there. It needs Python 3.9+, selenium and
psycopg2 (psycopg2-binary); python-dotenv is optional (reads a .env file).

    pip install selenium psycopg2-binary python-dotenv

Each run opens the Garden City load SVG page (GC_SVG_LINK) and reads three
values from it, all stamped with the same read time:

    GC_JAMESON   Jameson meter   (the old single-site script wrote GARDEN_CITY)
    GC_LEWIS     Lewis meter
    GC_TOTAL     total load, as the HMI shows it

The HMI is only reachable over the site VPN. Connect the VPN first, then:

    python garden_city.py              # loop: about one reading a minute
    python garden_city.py --once       # one reading, then exit
    python garden_city.py --headed     # visible Chrome (development)
    python garden_city.py --dry-run    # scrape and print, no writes

(In the repository, `python -m spp.scada.garden_city` does the same.)

The Chrome options and loop are the old garden_city.py (new driver per
reading, 60 s sleep after a success, 5-30 s back-off after a failure, 5-minute
pause after 5 failures in a row). The HMI does not ask for a login, so the
scraper opens the load SVG page directly.

A value that cannot be read or fails the sanity check is logged and skipped;
the other values are still written. The run counts as a failure only when no
value could be written.

DATABASE CONTRACT (schema/01_schema.sql in the pipeline repository). Each
reading is one row of mw_updates:

    timestamp_id       spine id of the 5-minute interval that contains the
                       reading: '<Excel date serial>HB<hh>MB<mm>', Central
                       wall clock, e.g. 10:47:11 on 2026-11-08 -> '46334HB10MB45'
    location           GC_JAMESON | GC_LEWIS | GC_TOTAL (rows of scada_locations)
    reading_timestamp  Central wall-clock read time, without a time zone
    mw_reading         MW, rounded half-up to 3 decimals

Before the insert the script adds the timestamps row for the interval and the
monthly partition (ensure_month_partition) if they are missing. A re-sent
reading is ignored (ON CONFLICT DO NOTHING). spine_id() below must stay the
same as spp/spine.py and the SQL spine_id(); the repository tests check this.

Environment (or a .env file in the working directory or above):
    GC_SVG_LINK          the load SVG page                       (required)
    SPP_DSN              Postgres DSN                 (required unless --dry-run)
    GC_MW_CEIL           upper limit for Jameson and Lewis, MW   (default 10)
    GC_TOTAL_MW_CEIL     upper limit for the total, MW           (default 20)
    GC_TOTAL_TOLERANCE   warn when total != Jameson + Lewis by more, MW (0.1)
    GC_PAGE_WAIT         seconds to wait for the values          (default 15)
    GC_MW_LOG            reading log file      (default logs/current_mw_log.txt)
    SCADA_LOG_FILE       also write the log to this rotating file
    SCADA_LOG_LEVEL      DEBUG, INFO, ...                        (default INFO)
    SCADA_CHROME_BINARY / SCADA_CHROMEDRIVER   optional Chrome / driver paths
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from logging.handlers import RotatingFileHandler
from zoneinfo import ZoneInfo

import psycopg2
from selenium import webdriver
from selenium.common.exceptions import (NoSuchElementException,
                                        TimeoutException, WebDriverException)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger("gc_scraper")

CENTRAL = ZoneInfo("America/Chicago")
TABLE = "mw_updates"
REQUIRED = ("GC_SVG_LINK",)

# mw_updates.location codes (rows of scada_locations).
JAMESON = "GC_JAMESON"
LEWIS = "GC_LEWIS"
TOTAL = "GC_TOTAL"

# Full XPaths of the values on the load SVG page, as Chrome's "Copy full
# XPath" gives them. svg_xpath() makes them usable by Selenium.
XPATHS = {
    LEWIS:   "/html/body/div[1]/div[2]/div/div[2]/svg/g/text[17]/tspan",
    JAMESON: "/html/body/div[1]/div[2]/div/div[2]/svg/g/g[3]/text[14]/tspan",
    TOTAL:   "/html/body/div[1]/div[2]/div/div[2]/svg/g/text[3]/tspan",
}


# --------------------------------------------------------------------------- #
# settings
# --------------------------------------------------------------------------- #
def _env_float(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    return float(raw) if raw else default


def ceiling(location: str) -> float:
    """Upper sanity limit in MW (the old script had MW_CEIL = 10)."""
    if location == TOTAL:
        return _env_float("GC_TOTAL_MW_CEIL", 20)
    return _env_float("GC_MW_CEIL", 10)


def mw_log_path() -> str:
    return os.environ.get("GC_MW_LOG") or os.path.join("logs", "current_mw_log.txt")


class MissingConfiguration(RuntimeError):
    """A required setting is not set."""


def check_env(*names: str) -> None:
    """Stop before Chrome starts if a required setting is missing."""
    missing = [n for n in names if not (os.environ.get(n) or "").strip()]
    if missing:
        raise MissingConfiguration(
            f"missing settings: {', '.join(missing)}. Set them in .env.")


def load_env_file() -> None:
    """Read a .env file if python-dotenv is installed (it is optional)."""
    try:
        from dotenv import find_dotenv, load_dotenv
    except ImportError:
        return
    load_dotenv(find_dotenv(usecwd=True))


# --------------------------------------------------------------------------- #
# TRANSFORM: spine id, reading, parsing
# --------------------------------------------------------------------------- #
EXCEL_EPOCH = date(1899, 12, 30)


def spine_id(ts: datetime) -> str:
    """Interval-beginning Central wall clock -> '46268HB17MB45'.

    Same as spp/spine.py and the SQL function spine_id(): the Excel date
    serial (days since 1899-12-30), then the zero-padded hour and minute.
    """
    return f"{(ts.date() - EXCEL_EPOCH).days}HB{ts.hour:02d}MB{ts.minute:02d}"


def interval_begin(ts: datetime, minutes: int = 5) -> datetime:
    """Floor to the start of the 5-minute interval: 10:47:11.3 -> 10:45."""
    return ts.replace(minute=ts.minute - ts.minute % minutes, second=0,
                      microsecond=0)


def central_to_utc(ts: datetime) -> datetime:
    """Naive Central wall clock -> naive UTC (first pass on the DST repeat)."""
    return (ts.replace(tzinfo=CENTRAL, fold=0)
              .astimezone(timezone.utc).replace(tzinfo=None))


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
        """The column is Central wall clock without a zone, so convert here."""
        if read_at.tzinfo is None:
            raise ValueError("read_at must be timezone-aware")
        return cls(location, read_at.astimezone(CENTRAL).replace(tzinfo=None),
                   mw_reading)


class ReadingUnavailable(ValueError):
    """The HMI is showing a placeholder (e.g. '###.#') instead of a value."""


class ReadingRejected(ValueError):
    """A value was read but failed the sanity check."""


class UnknownLocation(LookupError):
    """The reading's location code is not in scada_locations."""


_NUMBER = re.compile(r"[-+]?\d*\.?\d+")


def parse_mw_text(text: str, *, floor: float | None = None,
                  ceiling: float | None = None) -> Decimal:
    """'38.302937 MW' -> Decimal('38.303') (half-up, 3 dp), range-checked."""
    match = _NUMBER.search((text or "").replace(",", ""))
    if not match:
        raise ReadingUnavailable(f"no MW value in display text {text!r}")
    value = Decimal(match.group(0))
    if floor is not None and value < Decimal(str(floor)):
        raise ReadingRejected(f"MW value {value} below {floor}")
    if ceiling is not None and value > Decimal(str(ceiling)):
        raise ReadingRejected(f"MW value {value} above {ceiling}")
    return value.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)


_STEP = re.compile(r"^([A-Za-z][\w-]*)(\[\d+\])?$")


def svg_xpath(xpath: str) -> str:
    """Rewrite the steps from <svg> down so they match SVG-namespace elements.

    In an HTML page, an XPath step such as `svg` or `text` only matches HTML
    elements, so a copied path like .../div[2]/svg/g/text[3] finds nothing.
    `*[local-name()='text'][3]` selects the same node (the 3rd <text> child).
    """
    out, in_svg = [], False
    for step in xpath.split("/"):
        m = _STEP.match(step)
        if m and m.group(1).lower() == "svg":
            in_svg = True
        if in_svg and m:
            out.append(f"*[local-name()='{m.group(1)}']{m.group(2) or ''}")
        else:
            out.append(step)
    return "/".join(out)


# --------------------------------------------------------------------------- #
# EXTRACT — Chrome options are the old garden_city.py.
# --------------------------------------------------------------------------- #
def _chrome_options(headless: bool = True) -> Options:
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless')
    # chrome_options.add_argument('--disable-gpu')
    # chrome_options.add_argument('--no-sandbox')
    # chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')

    # NOTE : Add ignore ssl errors to continue to the website and ignore ssl errors.
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    chrome_options.add_argument('--allow-insecure-localhost')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    return chrome_options


def chrome_kwargs(options) -> dict:
    """Apply SCADA_CHROME_BINARY / SCADA_CHROMEDRIVER, if set (else no change)."""
    binary = (os.environ.get("SCADA_CHROME_BINARY") or "").strip()
    if binary:
        options.binary_location = binary
    driver_path = (os.environ.get("SCADA_CHROMEDRIVER") or "").strip()
    if not driver_path:
        return {}
    from selenium.webdriver.chrome.service import Service

    return {"service": Service(executable_path=driver_path)}


def create_driver(headless: bool = True):
    try:
        chrome_options = _chrome_options(headless)
        driver = webdriver.Chrome(options=chrome_options, **chrome_kwargs(chrome_options))
        driver.set_page_load_timeout(30)
        logger.info("WebDriver created successfully")
        return driver
    except Exception as e:
        logger.error(f"Failed to create WebDriver: {e}")
        raise


def _element_text(elem) -> str:
    # .text is the rendered text; textContent also works for SVG text that
    # Chrome does not report as displayed.
    text = (elem.text or "").strip()
    if not text:
        text = (elem.get_attribute("textContent") or "").strip()
    return text


def scrape_raw(driver) -> dict[str, str | Exception]:
    """Open the load SVG page and return {location: display text}.

    A value whose element is missing maps to the NoSuchElementException, so
    one moved label does not stop the other readings.
    """
    driver.get(os.getenv('GC_SVG_LINK'))

    # Wait until the SVG has drawn its values (the old script slept 3 s).
    wait = _env_float("GC_PAGE_WAIT", 15)
    first = svg_xpath(next(iter(XPATHS.values())))
    try:
        WebDriverWait(driver, wait).until(
            EC.presence_of_element_located((By.XPATH, first)))
    except TimeoutException:
        logger.warning(f"values not on the page after {wait:g} s; trying each one")

    out: dict[str, str | Exception] = {}
    for location, xpath in XPATHS.items():
        try:
            out[location] = _element_text(driver.find_element(By.XPATH, svg_xpath(xpath)))
            logger.info(f"{location} element text : {out[location]}")
        except NoSuchElementException as exc:
            out[location] = exc
    return out


def to_readings(raw: dict[str, str | Exception], read_at: datetime
                ) -> tuple[list[Reading], dict[str, Exception]]:
    """Parse each display text; return the good readings and the errors."""
    readings: list[Reading] = []
    errors: dict[str, Exception] = {}
    for location, text in raw.items():
        if isinstance(text, Exception):
            errors[location] = text
            continue
        try:
            mw = parse_mw_text(text, floor=0, ceiling=ceiling(location))
        except ValueError as exc:     # ReadingUnavailable / ReadingRejected
            errors[location] = exc
            continue
        readings.append(Reading.from_aware(location, read_at, mw))
    return readings, errors


def check_total(readings: list[Reading]) -> Decimal | None:
    """Warn when TOTAL differs from JAMESON + LEWIS; return the difference."""
    mw = {r.location: r.mw_reading for r in readings}
    if not {JAMESON, LEWIS, TOTAL} <= mw.keys():
        return None
    diff = mw[TOTAL] - (mw[JAMESON] + mw[LEWIS])
    tolerance = Decimal(str(_env_float("GC_TOTAL_TOLERANCE", 0.1)))
    if abs(diff) > tolerance:
        logger.warning(f"total {mw[TOTAL]} MW differs from Jameson + Lewis "
                       f"{mw[JAMESON] + mw[LEWIS]} MW by {diff}")
    return diff


# --------------------------------------------------------------------------- #
# LOAD
# --------------------------------------------------------------------------- #
INSERT_SQL = f"""
    INSERT INTO {TABLE} (timestamp_id, location, reading_timestamp, mw_reading)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (timestamp_id, location, reading_timestamp) DO NOTHING
"""


def write_readings(readings: list[Reading], *, dsn: str | None = None) -> int:
    """Insert readings in one transaction; a re-sent reading is ignored.

    Adds the timestamps row and the monthly partition first, if missing.
    Never overwrites timestamps.ts_gmt (the SPP feeds correct it). Returns
    the number of rows inserted. Raises UnknownLocation when a location code
    is not in scada_locations.
    """
    if not readings:
        return 0
    intervals = sorted({r.interval_central for r in readings})
    conn = psycopg2.connect(dsn or os.environ["SPP_DSN"])
    try:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO timestamps (ts_central, ts_gmt) VALUES (%s, %s) "
                "ON CONFLICT (ts_central) DO NOTHING",
                [(t, central_to_utc(t)) for t in intervals])
            for month in sorted({t.replace(day=1).date() for t in intervals}):
                cur.execute("SELECT ensure_month_partition(%s, %s)", (TABLE, month))
        conn.commit()

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
    finally:
        conn.close()
    logger.info(f"{inserted} of {len(readings)} readings inserted "
                f"({readings[0].timestamp_id})")
    return inserted


def append_reading_log(path: str, line: str) -> None:
    """Append one line to the local reading log, as the old script did."""
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "a") as fh:
            fh.write(f"{line}\n")
    except OSError as exc:      # a full or read-only disk must not stop a load
        logger.warning(f"could not write reading log {path}: {exc}")


# --------------------------------------------------------------------------- #
# one run
# --------------------------------------------------------------------------- #
def scrape_mw_single_run(driver, *, dry_run: bool = False):
    raw = scrape_raw(driver)
    ts = datetime.now(CENTRAL)
    readings, errors = to_readings(raw, ts)

    for location, exc in errors.items():
        logger.error(f"{location} not read: {exc}")
    if not readings:
        # Nothing to write: raise the first error, so the loop backs off.
        raise next(iter(errors.values()))

    check_total(readings)
    for r in readings:
        log_entry = f"{ts.isoformat()} - {r.location} {r.mw_reading} MW"
        print(log_entry)
        logger.info(f"Scraped: {log_entry}")
        if not dry_run:
            append_reading_log(mw_log_path(), log_entry)

    if dry_run:
        print(f"dry run: {readings[0].timestamp_id} "
              f"{', '.join(r.location for r in readings)} not written")
    else:
        write_readings(readings)
    return {r.location: r.mw_reading for r in readings}, ts


# --------------------------------------------------------------------------- #
# loop — same control flow as the old script
# --------------------------------------------------------------------------- #
def scrape_mw_continuous(*, headless: bool = True, dry_run: bool = False):
    consecutive_failures = 0
    max_consecutive_failures = 5
    scrape_count = 0

    logger.info("Starting continuous MW scraping...")

    while True:
        driver = None
        try:
            driver = create_driver(headless)

            mw_values, timestamp = scrape_mw_single_run(driver, dry_run=dry_run)

            consecutive_failures = 0
            scrape_count += 1

            logger.info(f"Scrape #{scrape_count} successful: {mw_values} at {timestamp}")

        except NoSuchElementException as e:
            consecutive_failures += 1
            logger.error(f"Element not found (attempt {consecutive_failures}): {e}")

        except ValueError as e:
            consecutive_failures += 1
            logger.error(f"Data validation error (attempt {consecutive_failures}): {e}")

        except WebDriverException as e:
            consecutive_failures += 1
            logger.error(f"WebDriver error (attempt {consecutive_failures}): {e}")

        except Exception as e:
            consecutive_failures += 1
            logger.exception(f"Unexpected error (attempt {consecutive_failures}): {e}")

        finally:
            if driver:
                try:
                    driver.quit()
                    logger.debug("Driver closed successfully")
                except Exception as e:
                    logger.warning(f"Error closing driver: {e}")

        # Check if we should abort due to too many failures
        if consecutive_failures >= max_consecutive_failures:
            logger.critical(f"Too many consecutive failures ({consecutive_failures}), pausing for 5 minutes")
            time.sleep(300)
            consecutive_failures = 0
        elif consecutive_failures > 0:
            wait_time = min(30, 5 * consecutive_failures)
            logger.info(f"Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
        else:
            time.sleep(60)


def scrape_mw_single(*, headless: bool = True, dry_run: bool = False):
    driver = None
    try:
        driver = create_driver(headless)
        mw_values, timestamp = scrape_mw_single_run(driver, dry_run=dry_run)
        logger.info(f"Single scrape successful: {mw_values} at {timestamp}")
    except Exception as e:
        logger.error(f"Single scrape failed: {e}")
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.warning(f"Error closing driver: {e}")


def setup_logging() -> None:
    """Console log in the old script's format; SCADA_LOG_FILE adds a rotating
    file (the old gc_scraper.log)."""
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    path = (os.environ.get("SCADA_LOG_FILE") or "").strip()
    if path:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        handlers.append(RotatingFileHandler(path, maxBytes=10_000_000, backupCount=5))
    logging.basicConfig(
        level=(os.environ.get("SCADA_LOG_LEVEL") or "INFO").upper(),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers,
        force=True,
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="garden_city.py")
    ap.add_argument("--once", action="store_true", help="one reading, then exit")
    ap.add_argument("--headed", action="store_true",
                    help="show the browser (development); production is headless")
    ap.add_argument("--dry-run", action="store_true", help="scrape but do not write")
    args = ap.parse_args(argv)
    load_env_file()
    setup_logging()
    headless = not args.headed
    try:
        check_env(*REQUIRED, *(() if args.dry_run else ("SPP_DSN",)))
    except MissingConfiguration as exc:
        logger.error(str(exc))
        return 2

    if args.once:
        try:
            scrape_mw_single(headless=headless, dry_run=args.dry_run)
        except Exception:
            return 1
        return 0
    try:
        scrape_mw_continuous(headless=headless, dry_run=args.dry_run)
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    return 0


if __name__ == "__main__":
    sys.exit(main())
