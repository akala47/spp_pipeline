"""Load a CSV dump of the OLD mw_updates table into the new one.

The old table was (id, location, reading_timestamp, mw_reading) with
reading_timestamp in Central wall-clock time. Each row gets its timestamp_id
from its reading time; the old id is dropped. Re-running is safe: a reading
already present is skipped.

    python -m spp.scada.import_legacy dump.csv --dry-run
    python -m spp.scada.import_legacy dump.csv

The columns are found by header name (location, reading_timestamp,
mw_reading; id is ignored). Without a header, each row's fields are told
apart by content: the location text, the date-time, and the MW number.

Export the dump straight from Postgres, NOT re-saved from Excel: Excel shows
the reading time as e.g. '46:51.5' (minutes:seconds) and drops the date on
save. Such rows are rejected with a message.

    psql "$OLD_DSN" -c "\\copy (SELECT id, location, reading_timestamp, mw_reading
                               FROM mw_updates ORDER BY id) TO 'dump.csv' CSV HEADER"

Use --source-tz UTC if the old timestamps were written in UTC instead of
Central (the early scrapers did this for a while).
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from ..logging_setup import get_logger
from .common import CENTRAL, Reading, write_readings

log = get_logger("spp.scada.import_legacy")

BATCH = 2000
_EXCEL_MANGLED = re.compile(r"^\d{1,2}:\d{2}(\.\d+)?$")
_LOCATION = re.compile(r"^[A-Z][A-Z0-9_]*$")


# Codes in the old table that have a new name. The old single-site Garden
# City scraper read the Jameson meter.
RENAMED = {"GARDEN_CITY": "GC_JAMESON"}


class BadRow(ValueError):
    pass


def _parse_ts(text: str, source_tz: ZoneInfo) -> datetime:
    text = text.strip()
    if _EXCEL_MANGLED.match(text):
        raise BadRow(f"reading time {text!r} has no date (re-saved from Excel?)")
    try:
        ts = datetime.fromisoformat(text.replace("/", "-"))
    except ValueError:
        try:
            ts = datetime.strptime(text, "%m-%d-%Y %H:%M:%S")
        except ValueError:
            raise BadRow(f"unrecognised reading time {text!r}") from None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=source_tz)
    return ts.astimezone(CENTRAL).replace(tzinfo=None)


def _parse_mw(text: str) -> Decimal:
    try:
        return Decimal(text.strip()).quantize(Decimal("0.001"))
    except InvalidOperation:
        raise BadRow(f"MW value {text!r} is not a number") from None


def parse_row(row: dict[str, str] | list[str], source_tz: ZoneInfo) -> Reading:
    """One CSV row -> Reading. Accepts a header-keyed dict or a bare list."""
    if isinstance(row, dict):
        keys = {k.strip().lower(): v for k, v in row.items() if k}
        try:
            loc = keys["location"]
            ts = keys["reading_timestamp"]
            mw = keys["mw_reading"]
        except KeyError as exc:
            raise BadRow(f"missing column {exc}") from None
        loc = loc.strip()
        return Reading(RENAMED.get(loc, loc), _parse_ts(ts, source_tz), _parse_mw(mw))

    loc = ts = mw = None
    for field in (f.strip() for f in row):
        if not field:
            continue
        if _LOCATION.match(field):
            loc = field
        elif ":" in field:
            ts = field
        elif "." in field:
            mw = field                      # MW has decimals; the old id does not
    if loc is None or ts is None or mw is None:
        raise BadRow(f"cannot identify location / time / MW in {row!r}")
    return Reading(RENAMED.get(loc, loc), _parse_ts(ts, source_tz), _parse_mw(mw))


def _rows(path: str):
    with open(path, newline="", encoding="utf-8-sig") as fh:
        sample = fh.read(4096)
        fh.seek(0)
        has_header = "location" in sample.splitlines()[0].lower() if sample else False
        reader = csv.DictReader(fh) if has_header else csv.reader(fh)
        for n, row in enumerate(reader, start=2 if has_header else 1):
            yield n, row


def import_csv(path: str, *, source_tz: str = "America/Chicago",
               dry_run: bool = False) -> dict[str, int]:
    tz = ZoneInfo(source_tz)
    stats = {"rows": 0, "inserted": 0, "bad": 0}
    batch: list[Reading] = []
    for line, row in _rows(path):
        stats["rows"] += 1
        try:
            batch.append(parse_row(row, tz))
        except BadRow as exc:
            stats["bad"] += 1
            if stats["bad"] <= 20:
                log.warning(f"line {line}: {exc}", extra={"event": "scada.import_bad_row",
                                                          "line": line})
            continue
        if len(batch) >= BATCH:
            stats["inserted"] += 0 if dry_run else write_readings(batch)
            batch.clear()
    if batch and not dry_run:
        stats["inserted"] += write_readings(batch)
    stats["skipped_existing"] = (0 if dry_run else
                                 stats["rows"] - stats["bad"] - stats["inserted"])
    log.info("legacy import complete", extra={"event": "scada.import_ok", **stats,
                                             "dry_run": dry_run})
    return stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="spp.scada.import_legacy")
    ap.add_argument("csv_path")
    ap.add_argument("--source-tz", default="America/Chicago",
                    help="zone of the old reading_timestamp values (default America/Chicago)")
    ap.add_argument("--dry-run", action="store_true",
                    help="parse and validate every row, write nothing")
    args = ap.parse_args(argv)
    stats = import_csv(args.csv_path, source_tz=args.source_tz, dry_run=args.dry_run)
    print(", ".join(f"{k}={v}" for k, v in stats.items()))
    return 1 if stats["bad"] else 0


if __name__ == "__main__":
    sys.exit(main())
