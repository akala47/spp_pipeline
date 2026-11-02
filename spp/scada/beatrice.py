"""Beatrice city load: one scrape per run.

Runs once a minute from the Airflow DAG `scada_beatrice` (dags/scada.py) on
the VPS. The Beatrice HMI is on the internet: no VPN is necessary. Also
runnable by hand:

    python -m spp.scada.beatrice            # one reading, written to mw_updates
    python -m spp.scada.beatrice --dry-run  # scrape and print, no write
    python -m spp.scada.beatrice --headed   # visible Chrome (development)

EXTRACT is the old beatrice_mw.py, unchanged: same Chrome options, login,
page, element id and waits. Only TRANSFORM and LOAD are new (spp.scada.common):
the value gets a sanity check, the reading gets its spine id, and the row goes
to the new mw_updates with an idempotent insert.

Environment: BEATRICE_USER, BEATRICE_PWD, BEATRICE_LOGIN_URL,
BEATRICE_SVG_URL, SPP_DSN. Optional: BEATRICE_MW_CEIL (upper sanity limit,
unset = no limit), SCADA_CHROME_BINARY / SCADA_CHROMEDRIVER (Airflow image).
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from ..config import REPO_ROOT
from ..logging_setup import get_logger
from .common import (CENTRAL, Reading, append_reading_log, check_env,
                     chrome_kwargs, parse_mw_text, record)

REQUIRED = ("BEATRICE_USER", "BEATRICE_PWD", "BEATRICE_LOGIN_URL", "BEATRICE_SVG_URL")

log = get_logger("spp.scada.beatrice")

LOCATION = "BEATRICE"

# Local reading log, as in the old script: <repo>/logs/ (git-ignored).
MW_LOG_PATH = os.path.join(REPO_ROOT, "logs", "beatrice_mw.log")


# --------------------------------------------------------------------------- #
# EXTRACT — copied from the old beatrice_mw.py. Do not change.
# --------------------------------------------------------------------------- #
def create_driver(headless: bool = True) -> webdriver.Chrome:
    # Chrome is only started when a scrape actually runs, not when this module is imported.
    chrome_options = Options()
    if headless:
        chrome_options.add_argument('--headless')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--ignore-certificate-errors')
    return webdriver.Chrome(options=chrome_options, **chrome_kwargs(chrome_options))


def login(driver):
    driver.get(os.getenv('BEATRICE_LOGIN_URL'))
    driver.find_element(By.NAME, "username").send_keys(os.getenv('BEATRICE_USER'))
    driver.find_element(By.NAME, "password").send_keys(os.getenv('BEATRICE_PWD'))
    driver.find_element(By.XPATH, "/html/body/div[2]/form/div/div[8]/input").click()
    time.sleep(3)


def scrape_raw(headless: bool = True) -> str:
    """Return the MW display text, exactly as the old scrape_mw() read it."""
    driver = None
    try:
        driver = create_driver(headless)
        login(driver)
        driver.get(os.getenv('BEATRICE_SVG_URL'))
        time.sleep(3)
        mw_elem = driver.find_element(By.ID, "tspan4977-9")
        raw_value = mw_elem.text.strip()
    finally:
        if driver is not None:
            driver.quit()
    return raw_value


# --------------------------------------------------------------------------- #
# TRANSFORM + LOAD — new pipeline
# --------------------------------------------------------------------------- #
def _ceiling() -> float | None:
    raw = os.environ.get("BEATRICE_MW_CEIL", "").strip()
    return float(raw) if raw else None


def run_once(*, headless: bool = True, dry_run: bool = False) -> Reading:
    check_env(*REQUIRED, *(() if dry_run else ("SPP_DSN",)))
    raw_value = scrape_raw(headless)

    # Parse and round to 3 decimals; reject values outside [0, BEATRICE_MW_CEIL].
    mw_dec = parse_mw_text(raw_value, floor=0, ceiling=_ceiling())

    # Timezone-aware US Central timestamp (offset handles CST/CDT), taken at
    # the same point as in the old script.
    ts = datetime.now(CENTRAL)
    reading = Reading.from_aware(LOCATION, ts, mw_dec)

    line = f"{ts.isoformat()} - {mw_dec} MW"
    print(line)
    if dry_run:
        print(f"dry run: {reading.timestamp_id} {reading.location} not written")
        return reading
    append_reading_log(MW_LOG_PATH, line)
    record(reading)
    return reading


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="spp.scada.beatrice")
    ap.add_argument("--headed", action="store_true",
                    help="show the browser (development); production is headless")
    ap.add_argument("--dry-run", action="store_true", help="scrape but do not write")
    args = ap.parse_args(argv)
    try:
        run_once(headless=not args.headed, dry_run=args.dry_run)
    except Exception:
        log.exception("beatrice scrape failed", extra={"event": "scada.error",
                                                       "location": LOCATION})
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
