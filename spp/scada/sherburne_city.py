"""Sherburne city load scraper: a standalone script for the pipeline.

It does not import anything from this repository, so you can copy this one
file to any computer and run it there. It needs Python 3.9+, selenium and
psycopg2 (psycopg2-binary); python-dotenv is optional (reads a .env file).
On Windows also install tzdata (Windows has no time zone database).

    pip install -r requirements.txt

One CYCLE is one login and SC_READS readings (default 15), one a minute:

    1. open SC_LOGIN_LINK, type SC_USER and SC_PWD, press Enter
    2. click "View real time graphs"
    3. read the "Demand now" value once a minute, divide it by 1000 (the
       page shows kW) and write each reading to mw_updates as location
       SHERBURNE_CITY, in MW

then Chrome closes and the next cycle logs in again, so a new login happens
about every 15 minutes. If the site logs the scraper out during a cycle (the
login form comes back) or three readings in a row fail, it logs in again
within the same cycle, up to MAX_RELOGINS times.

Two ways to run it, both with the same cycle:

  * on the VPS, from the Airflow DAG `scada_sherburne` (dags/scada.py): one
    cycle per run, every 15 minutes, via run_scheduled_cycle(). The settings
    come from the Airflow .env.
  * standalone, on any computer (this file alone, see below). Keep one of
    the two running, not both: both would store every reading twice.

    python sherburne_city.py              # loop: cycle after cycle
    python sherburne_city.py --once       # log in, one reading, exit
    python sherburne_city.py --cycle      # one full cycle (15 readings), exit
    python sherburne_city.py --headed     # visible Chrome (development)
    python sherburne_city.py --dry-run    # scrape and print, no writes
    python sherburne_city.py --reads 3    # readings per cycle (overrides SC_READS)

A reading that cannot be read is logged and skipped. If the database
cannot be reached, the readings are kept in memory and sent with the next
write (the insert ignores a reading it already has).

DATABASE CONTRACT (schema/01_schema.sql in the pipeline repository). Each
reading is one row of mw_updates:

    timestamp_id       spine id of the 5-minute interval that contains the
                       reading: '<Excel date serial>HB<hh>MB<mm>', Central
                       wall clock, e.g. 10:47:11 on 2026-11-08 -> '46334HB10MB45'
    location           SHERBURNE_CITY (a row of scada_locations)
    reading_timestamp  Central wall-clock read time, without a time zone
    mw_reading         MW, rounded half-up to 3 decimals

Before the insert the script adds the timestamps row for the interval and the
monthly partition (ensure_month_partition) if they are missing. spine_id()
below must stay the same as spp/spine.py and the SQL spine_id(); the
repository tests check this.

Environment (or a .env file in the working directory or above):
    SC_LOGIN_LINK        login page                              (required)
    SC_USER, SC_PWD      login email and password                (required)
    SPP_DSN              Postgres DSN                 (required unless --dry-run)
    SC_READS             readings per login                      (default 15)
    SC_READ_INTERVAL     seconds between readings                (default 60)
    SC_PAGE_WAIT         seconds to wait for a page or a value   (default 30)
    SC_VALUE_UNIT        unit the page shows: kW (divided by 1000) or MW (default kW)
    SC_MW_CEIL           upper sanity limit, MW                  (default 100)
    SC_MW_LOG            reading log file   (default logs/sherburne_mw_log.txt)
    SCADA_LOG_FILE       also write the log to this rotating file
    SCADA_LOG_LEVEL      DEBUG, INFO, ...                        (default INFO)
    SCADA_CHROME_BINARY / SCADA_CHROMEDRIVER   optional Chrome / driver paths
                         (set on the Airflow image; Chrome then also runs
                         with --no-sandbox, as the Beatrice scraper does)
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import ROUND_HALF_UP, Decimal
from logging.handlers import RotatingFileHandler
from zoneinfo import ZoneInfo

import psycopg2
from selenium import webdriver
from selenium.common.exceptions import (ElementClickInterceptedException,
                                        ElementNotInteractableException,
                                        NoSuchElementException,
                                        StaleElementReferenceException,
                                        TimeoutException, WebDriverException)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait

logger = logging.getLogger("sc_scraper")

CENTRAL = ZoneInfo("America/Chicago")
TABLE = "mw_updates"
REQUIRED = ("SC_LOGIN_LINK", "SC_USER", "SC_PWD")

# mw_updates.location code (a row of scada_locations).
LOCATION = "SHERBURNE_CITY"

XPATHS = {
    "email":    '//*[@id="email"]',
    "password": '//*[@id="password"]',
    "graphs":   '//*[@id="root"]/div[1]/main/div[4]/div/a[1]/span',
    "demand":   '//*[@id="peak-body-7"]/div[1]/div[1]/div[2]/span[1]',
}

# Waits that retry while the page is still drawing.
_WAIT_IGNORES = (NoSuchElementException, StaleElementReferenceException,
                 ElementNotInteractableException, ElementClickInterceptedException)

MAX_READ_FAILURES = 3      # failed readings in a row before logging in again
MAX_RELOGINS = 3           # logins after the first, within one cycle
SCHEDULED_CYCLE_SECONDS = 14 * 60 + 30   # no reading after this, per Airflow run


# --------------------------------------------------------------------------- #
# settings
# --------------------------------------------------------------------------- #
def _env_float(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    return float(raw) if raw else default


def reads_per_cycle() -> int:
    return int(_env_float("SC_READS", 15))


def read_interval() -> float:
    return _env_float("SC_READ_INTERVAL", 60)


def page_wait() -> float:
    return _env_float("SC_PAGE_WAIT", 30)


def ceiling() -> float:
    return _env_float("SC_MW_CEIL", 100)


def value_unit() -> str:
    """Unit of the 'Demand now' value: kW (the page's unit) unless set to MW."""
    unit = (os.environ.get("SC_VALUE_UNIT") or "kW").strip()
    if unit.lower() not in ("mw", "kw"):
        raise MissingConfiguration(f"SC_VALUE_UNIT must be MW or kW, not {unit!r}")
    return "kW" if unit.lower() == "kw" else "MW"


def mw_log_path() -> str:
    return os.environ.get("SC_MW_LOG") or os.path.join("logs", "sherburne_mw_log.txt")


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
# TRANSFORM: spine id, reading, parsing (same as garden_city.py)
# --------------------------------------------------------------------------- #
EXCEL_EPOCH = date(1899, 12, 30)


def spine_id(ts: datetime) -> str:
    """Interval-beginning Central wall clock -> '46268HB17MB45'."""
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
    """The page shows no number (a placeholder or a blank) instead of a value."""


class ReadingRejected(ValueError):
    """A value was read but failed the sanity check."""


class UnknownLocation(LookupError):
    """The reading's location code is not in scada_locations."""


class LoginFailed(RuntimeError):
    """The login did not reach the page with 'View real time graphs'."""


class LoggedOut(RuntimeError):
    """The site ended the session: the login form is back."""


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


def parse_demand(value_text: str, unit: str, *, ceiling: float) -> Decimal:
    """Display value in `unit` -> MW, half-up 3 dp, in [0, ceiling] MW.

    '7,483.2' kW -> Decimal('7.483'). The division happens before rounding,
    so no precision is lost.
    """
    if unit == "kW":
        match = _NUMBER.search((value_text or "").replace(",", ""))
        if not match:
            raise ReadingUnavailable(f"no value in display text {value_text!r}")
        value_text = str(Decimal(match.group(0)) / 1000)
    return parse_mw_text(value_text, floor=0, ceiling=ceiling)


# --------------------------------------------------------------------------- #
# EXTRACT
# --------------------------------------------------------------------------- #
def _chrome_options(headless: bool = True) -> Options:
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
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
    chrome_options = _chrome_options(headless)
    if (os.environ.get("SCADA_CHROME_BINARY") or "").strip():
        # Chromium inside the Airflow container: no usable sandbox there
        # (same options as the Beatrice scraper).
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-gpu')
    driver = webdriver.Chrome(options=chrome_options, **chrome_kwargs(chrome_options))
    driver.set_page_load_timeout(60)
    logger.info("WebDriver created successfully")
    return driver


def _wait(driver, seconds: float | None = None) -> WebDriverWait:
    return WebDriverWait(driver, page_wait() if seconds is None else seconds,
                         poll_frequency=0.5, ignored_exceptions=_WAIT_IGNORES)


def _find(driver, name: str, seconds: float | None = None):
    """Wait for the element XPATHS[name] and return it."""
    return _wait(driver, seconds).until(
        lambda d: d.find_element(By.XPATH, XPATHS[name]))


def _element_text(elem) -> str:
    text = (elem.text or "").strip()
    if not text:
        text = (elem.get_attribute("textContent") or "").strip()
    return text


def login(driver) -> None:
    """Log in and wait for the page with 'View real time graphs'.

    If the login link opens the home page instead of the form (the session
    is still valid), there is nothing to type.
    """
    driver.get(os.environ["SC_LOGIN_LINK"])

    def form_or_home(d):
        for name in ("email", "graphs"):
            try:
                d.find_element(By.XPATH, XPATHS[name])
                return name
            except NoSuchElementException:
                pass
        return False

    try:
        shown = _wait(driver).until(form_or_home)
    except TimeoutException:
        raise LoginFailed(f"no login form on {os.environ['SC_LOGIN_LINK']} "
                          f"after {page_wait():g} s") from None
    if shown == "graphs":
        logger.info("still logged in")
        return
    email = _find(driver, "email")
    email.clear()
    email.send_keys(os.environ["SC_USER"])
    password = _find(driver, "password")
    password.clear()
    password.send_keys(os.environ["SC_PWD"])
    password.send_keys(Keys.RETURN)
    try:
        _find(driver, "graphs")
    except TimeoutException:
        raise LoginFailed(
            f"'View real time graphs' not shown {page_wait():g} s after login; "
            "check SC_USER / SC_PWD and SC_LOGIN_LINK") from None
    logger.info("logged in")


def open_real_time_graphs(driver) -> None:
    """Click 'View real time graphs'; follow a new tab if it opens one."""
    before = set(driver.window_handles)

    def clicked(d):
        d.find_element(By.XPATH, XPATHS["graphs"]).click()
        return True

    _wait(driver).until(clicked)

    def demand_shown(d):
        new = [h for h in d.window_handles if h not in before]
        if new and d.current_window_handle != new[-1]:
            d.switch_to.window(new[-1])
        return d.find_element(By.XPATH, XPATHS["demand"])

    try:
        _wait(driver).until(demand_shown)
    except TimeoutException:
        raise NoSuchElementException(
            f"'Demand now' not on the real time graphs page after {page_wait():g} s"
        ) from None
    logger.info("real time graphs page open")


def read_demand_text(driver) -> str:
    """Wait until 'Demand now' shows a number; return its text.

    The field can show a blank or a placeholder while the page fetches the
    value, so this waits for a number rather than for the element.
    """
    def shown(d):
        text = _element_text(d.find_element(By.XPATH, XPATHS["demand"]))
        return text if _NUMBER.search(text.replace(",", "")) else False

    try:
        value_text = _wait(driver).until(shown)
    except TimeoutException:
        try:
            value_text = _element_text(driver.find_element(By.XPATH, XPATHS["demand"]))
        except NoSuchElementException:
            raise NoSuchElementException("'Demand now' is gone from the page") from None
    return value_text


def logged_out(driver) -> bool:
    """True when the page shows the login form, i.e. the session ended."""
    try:
        driver.find_element(By.XPATH, XPATHS["email"])
        return True
    except (NoSuchElementException, StaleElementReferenceException):
        return False


def read_once(driver) -> Reading:
    value_text = read_demand_text(driver)
    unit = value_unit()
    mw = parse_demand(value_text, unit, ceiling=ceiling())
    logger.debug(f"Demand now text {value_text!r} {unit} -> {mw} MW")
    return Reading.from_aware(LOCATION, datetime.now(CENTRAL), mw)


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
    the number of rows inserted. Raises UnknownLocation when the location
    code is not in scada_locations.
    """
    if not readings:
        return 0
    intervals = sorted({r.interval_central for r in readings})
    conn = psycopg2.connect(dsn or os.environ["SPP_DSN"], connect_timeout=15)
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
                f"({readings[-1].timestamp_id})")
    return inserted


def append_reading_log(path: str, line: str) -> None:
    """Append one line to the local reading log."""
    try:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "a") as fh:
            fh.write(f"{line}\n")
    except OSError as exc:      # a full or read-only disk must not stop a load
        logger.warning(f"could not write reading log {path}: {exc}")


class Writer:
    """Writes each reading as it comes; keeps unsent ones for the next try."""

    MAX_PENDING = 2000          # about 33 hours of readings

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.pending: list[Reading] = []

    def add(self, reading: Reading) -> None:
        line = f"{datetime.now(CENTRAL).isoformat()} - {reading.location} {reading.mw_reading} MW"
        print(line, flush=True)
        if self.dry_run:
            print(f"dry run: {reading.timestamp_id} {reading.location} not written",
                  flush=True)
            return
        append_reading_log(mw_log_path(), line)
        self.pending.append(reading)
        self.flush()

    def flush(self) -> None:
        if not self.pending or self.dry_run:
            return
        try:
            write_readings(self.pending)
        except UnknownLocation:
            raise
        except Exception as exc:
            del self.pending[:-self.MAX_PENDING]
            logger.error(f"database write failed, {len(self.pending)} reading(s) "
                         f"kept for the next try: {exc}")
            return
        self.pending.clear()


# --------------------------------------------------------------------------- #
# one cycle: one login, n readings a minute apart
# --------------------------------------------------------------------------- #
def _sleep_until(deadline: float) -> None:
    remaining = deadline - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)


def run_cycle(writer: Writer, *, headless: bool = True,
              reads: int | None = None, max_seconds: float | None = None) -> int:
    """Log in, open the graphs page, take `reads` readings a minute apart.

    Logs in again in the same browser when the site logs out (the login form
    is back) or MAX_READ_FAILURES readings in a row fail, up to MAX_RELOGINS
    times; the reading that found the problem is retried after the login.
    With max_seconds, no reading is taken later than that many seconds after
    the start (the Airflow run must end before the next one is due).

    Returns the number of good readings. Raises when the first login or the
    page fails, or when no reading could be taken.
    """
    t0 = time.monotonic()
    reads = reads_per_cycle() if reads is None else reads
    interval = read_interval()
    driver = None
    good, failures_in_a_row, relogins = 0, 0, 0

    def take(i: int) -> None:
        if logged_out(driver):
            raise LoggedOut("the site logged out (login form shown)")
        writer.add(read_once(driver))

    def log_in_again() -> bool:
        nonlocal relogins
        if relogins >= MAX_RELOGINS:
            return False
        relogins += 1
        logger.warning(f"logging in again ({relogins} of {MAX_RELOGINS} this cycle)")
        try:
            login(driver)
            open_real_time_graphs(driver)
            return True
        except (LoginFailed, WebDriverException) as exc:
            logger.error(f"logging in again failed: {exc}")
            return False

    try:
        driver = create_driver(headless)
        login(driver)
        open_real_time_graphs(driver)
        start = time.monotonic()
        for i in range(reads):
            due = start + i * interval
            if max_seconds is not None and due > t0 + max_seconds:
                logger.info(f"stopping after {i} readings: next one is past "
                            f"{max_seconds:g} s")
                break
            if i:
                _sleep_until(due)
            try:
                take(i)
                good += 1
                failures_in_a_row = 0
                continue
            except UnknownLocation:
                raise
            except (ValueError, WebDriverException, LoggedOut) as exc:
                failures_in_a_row += 1
                logger.error(f"reading {i + 1} of {reads} failed: {exc}")
                if not (isinstance(exc, LoggedOut)
                        or failures_in_a_row >= MAX_READ_FAILURES):
                    continue
            # Logged out, or too many failures: log in again and retry this one.
            if not log_in_again():
                logger.error("could not log in again; ending this cycle")
                break
            failures_in_a_row = 0
            try:
                take(i)
                good += 1
            except UnknownLocation:
                raise
            except (ValueError, WebDriverException, LoggedOut) as exc:
                failures_in_a_row += 1
                logger.error(f"reading {i + 1} of {reads} failed after logging in "
                             f"again: {exc}")
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception as exc:
                logger.warning(f"Error closing driver: {exc}")
    logger.info(f"cycle done: {good} of {reads} readings, {relogins} extra login(s)")
    if not good:
        raise ReadingUnavailable(f"no reading in this cycle of {reads}")
    return good


def run_scheduled_cycle(max_seconds: float = SCHEDULED_CYCLE_SECONDS) -> dict:
    """One cycle for a scheduler: the Airflow DAG `scada_sherburne`.

    Headless, writes each reading as it is taken, and ends before the next
    15-minute run is due. Raises (the run turns red) when a setting is
    missing, no reading was taken, or readings could not be written.
    """
    check_env(*REQUIRED, "SPP_DSN")
    value_unit()
    writer = Writer()
    good = run_cycle(writer, headless=True, max_seconds=max_seconds)
    writer.flush()
    if writer.pending:
        raise RuntimeError(f"{len(writer.pending)} of {good} reading(s) not "
                           "written to the database")
    return {"location": LOCATION, "readings": good}


def scrape_continuous(*, headless: bool = True, dry_run: bool = False,
                      reads: int | None = None) -> None:
    writer = Writer(dry_run)
    consecutive_failures = 0
    logger.info("Starting continuous Sherburne city load scraping...")
    while True:
        cycle_start = time.monotonic()
        n = reads_per_cycle() if reads is None else reads
        try:
            run_cycle(writer, headless=headless, reads=n)
            consecutive_failures = 0
        except (UnknownLocation, MissingConfiguration):
            raise
        except Exception as exc:
            consecutive_failures += 1
            logger.error(f"cycle failed (attempt {consecutive_failures}): {exc}")

        if consecutive_failures >= 5:
            logger.critical("5 failed cycles in a row, pausing for 5 minutes")
            time.sleep(300)
            consecutive_failures = 0
        elif consecutive_failures:
            wait_time = min(30, 5 * consecutive_failures)
            logger.info(f"Waiting {wait_time}s before logging in again...")
            time.sleep(wait_time)
        else:
            # Start the next login one interval after the last reading.
            _sleep_until(cycle_start + n * read_interval())


# --------------------------------------------------------------------------- #
# command line
# --------------------------------------------------------------------------- #
def setup_logging() -> None:
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
    ap = argparse.ArgumentParser(prog="sherburne_city.py")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="log in, one reading, exit")
    mode.add_argument("--cycle", action="store_true",
                      help="one login cycle (SC_READS readings), then exit")
    ap.add_argument("--reads", type=int, help="readings per cycle (overrides SC_READS)")
    ap.add_argument("--headed", action="store_true",
                    help="show the browser (development); production is headless")
    ap.add_argument("--dry-run", action="store_true", help="scrape but do not write")
    args = ap.parse_args(argv)
    load_env_file()
    setup_logging()
    headless = not args.headed
    try:
        check_env(*REQUIRED, *(() if args.dry_run else ("SPP_DSN",)))
        value_unit()
    except MissingConfiguration as exc:
        logger.error(str(exc))
        return 2

    reads = 1 if args.once else args.reads
    if args.once or args.cycle:
        writer = Writer(args.dry_run)
        try:
            run_cycle(writer, headless=headless, reads=reads)
        except Exception as exc:
            logger.error(f"scrape failed: {exc}")
            return 1
        if writer.pending:
            logger.error(f"{len(writer.pending)} reading(s) not written")
            return 1
        return 0
    try:
        scrape_continuous(headless=headless, dry_run=args.dry_run, reads=reads)
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as exc:
        logger.error(str(exc))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
