"""Garden City (Jameson) site load: standalone scraper on the VPN device.

The HMI is only reachable over the site VPN, so this runs on the device that
holds the VPN connection, not in Airflow. Connect the VPN first, then:

    python -m spp.scada.garden_city            # loop: about one reading a minute
    python -m spp.scada.garden_city --once     # one reading, then exit
    python -m spp.scada.garden_city --headed   # visible Chrome (development)
    python -m spp.scada.garden_city --dry-run  # scrape and print, no writes

EXTRACT is the old garden_city.py, unchanged: same Chrome options, login,
navigation, element id, waits, and the same loop (new driver per reading,
60 s sleep after a success, 5-30 s back-off after a failure, 5-minute pause
after 5 failures in a row). Only TRANSFORM and LOAD are new
(spp.scada.common): the reading gets its spine id and goes to the new
mw_updates with an idempotent insert.

Environment: GC_HOME_LINK, GC_SVG_XPATH, SPP_DSN; GC_USER / GC_PWD if the HMI
asks for a login. Optional: GC_MW_CEIL (sanity ceiling, default 10),
SCADA_LOG_FILE (also log to this rotating file), SCADA_CHROME_BINARY /
SCADA_CHROMEDRIVER.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from ..config import REPO_ROOT
from ..logging_setup import get_logger
from .common import (CENTRAL, Reading, append_reading_log, check_env,
                     chrome_kwargs, parse_mw_text, record)

REQUIRED = ("GC_HOME_LINK", "GC_SVG_XPATH")

logger = get_logger("spp.scada.garden_city")

LOCATION = "GARDEN_CITY"

# Local reading log (the old script wrote current_mw_log.txt in the working
# directory); now <repo>/logs/, which is git-ignored.
MW_LOG_PATH = os.path.join(REPO_ROOT, "logs", "current_mw_log.txt")


def _mw_ceil() -> float:
    # NOTE - upper threshold for the load sanity check (old MW_CEIL = 10)
    return float(os.environ.get("GC_MW_CEIL", "10"))


# --------------------------------------------------------------------------- #
# EXTRACT — copied from the old garden_city.py. Do not change.
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


def login(driver):
    try:
        driver.get(os.getenv('GC_HOME_LINK'))
        time.sleep(2)

        # Check if login form is present
        try:
            username_field = driver.find_element(By.NAME, "username")
            logger.info("Login required, authenticating...")

            username_field.send_keys(os.getenv('GC_USER'))
            driver.find_element(By.NAME, "password").send_keys(os.getenv('GC_PWD'))

            # If there's a submit button, uncomment and adjust the xpath
            # driver.find_element(By.XPATH, "//button[@type='submit']").click()

            time.sleep(3)
            logger.info("Login completed")
            return True

        except NoSuchElementException:
            logger.info("No login required, already authenticated")
            return False

    except Exception as e:
        logger.error(f"Login failed: {e}")
        raise


def scrape_raw(driver) -> str:
    """Return the MW display text, exactly as the old scrape_mw_single_run read it."""
    login(driver)

    # Navigate to SVG page
    svg_link = driver.find_element(By.XPATH, os.getenv('GC_SVG_XPATH'))
    svg_link.click()
    time.sleep(3)

    mw_elem = driver.find_element(By.ID, "tspan12863-0")
    logger.info(f"mw element text : {mw_elem.text}")
    raw_value = mw_elem.text.strip()
    logger.debug(f"Raw value extracted: {raw_value}")
    return raw_value


# --------------------------------------------------------------------------- #
# TRANSFORM + LOAD — new pipeline
# --------------------------------------------------------------------------- #
def scrape_mw_single_run(driver, *, dry_run: bool = False):
    raw_value = scrape_raw(driver)

    # Parse, sanity check (0 .. GC_MW_CEIL), round half-up to 3 dp.
    # Raises ReadingUnavailable / ReadingRejected (both ValueError, as before).
    mw_dec = parse_mw_text(raw_value, floor=0, ceiling=_mw_ceil())

    ts = datetime.now(CENTRAL)
    reading = Reading.from_aware(LOCATION, ts, mw_dec)

    log_entry = f"{ts.isoformat()} - {mw_dec} MW"
    print(log_entry)
    logger.info(f"Scraped: {log_entry}")
    if dry_run:
        print(f"dry run: {reading.timestamp_id} {reading.location} not written")
        return mw_dec, ts

    append_reading_log(MW_LOG_PATH, log_entry)
    record(reading)
    return mw_dec, ts


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

            mw_value, timestamp = scrape_mw_single_run(driver, dry_run=dry_run)

            consecutive_failures = 0
            scrape_count += 1

            logger.info(f"Scrape #{scrape_count} successful: {mw_value} MW at {timestamp}")

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
        mw_value, timestamp = scrape_mw_single_run(driver, dry_run=dry_run)
        logger.info(f"Single scrape successful: {mw_value} MW at {timestamp}")
    except Exception as e:
        logger.error(f"Single scrape failed: {e}")
        raise
    finally:
        if driver:
            try:
                driver.quit()
            except Exception as e:
                logger.warning(f"Error closing driver: {e}")


def _add_file_logging() -> None:
    """SCADA_LOG_FILE: also log to a rotating file (the old gc_scraper.log)."""
    path = os.environ.get("SCADA_LOG_FILE", "").strip()
    if not path:
        return
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    handler = RotatingFileHandler(path, maxBytes=10_000_000, backupCount=5)
    root = logging.getLogger()
    handler.setFormatter(root.handlers[0].formatter if root.handlers else None)
    root.addHandler(handler)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="spp.scada.garden_city")
    ap.add_argument("--once", action="store_true", help="one reading, then exit")
    ap.add_argument("--headed", action="store_true",
                    help="show the browser (development); production is headless")
    ap.add_argument("--dry-run", action="store_true", help="scrape but do not write")
    args = ap.parse_args(argv)
    _add_file_logging()
    headless = not args.headed
    try:
        check_env(*REQUIRED, *(() if args.dry_run else ("SPP_DSN",)))
    except Exception as exc:
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
