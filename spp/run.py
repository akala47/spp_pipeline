"""The single ingestion path for every feed.

    # one day of a feed, default group
    python -m spp.run --feed rt_lmp --start 2026-07-21

    # named groups — one download, loaded separately for each
    python -m spp.run --feed rt_lmp --start 2026-07-21 --groups nppd,kmea

    # ad-hoc subset, no config change needed
    python -m spp.run --feed rt_mcp --start 2026-07-21 --members 1,5,SPP

    # backfill a range for one group
    python -m spp.run --feed rt_lmp --start 2026-07-20 --end 2026-07-21 --groups nppd

    # forward-fill instead of ingesting
    python -m spp.run --feed rt_lmp --fill --start 2026-07-21

Airflow calls `ingest()` directly; the CLI exists so the same code path is
runnable by hand during an incident, which cron never gave you.

Fetch happens once per interval regardless of how many groups are requested —
the payload is filtered in memory per selection. Loading four node groups costs
four parses, not four downloads.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from typing import Sequence

from . import db, http
from .feeds import FEEDS, Feed, Row
from .logging_setup import bind, get_logger, new_run_id, timed
from .selection import Selection, selections_for_env

log = get_logger("spp.run")


class QualityCheckFailed(Exception):
    pass


class PartialRunFailure(Exception):
    """One or more selections failed while others succeeded."""


# --------------------------------------------------------------------------- #
# selection resolution
# --------------------------------------------------------------------------- #
def resolve_selections(
    feed: Feed,
    *,
    groups: Sequence[str] | None = None,
    members: Sequence[str] | None = None,
) -> list[Selection | None]:
    """Work out which subsets this run should load.

    Precedence: explicit members > explicit groups > per-feed env override >
    the feed's default group. Feeds with no dimension get a single `None`,
    meaning "load the whole payload".
    """
    if not feed.subsettable:
        if groups or members:
            log.warning(
                "feed is not subsettable, ignoring selection arguments",
                extra={"event": "selection.ignored", "feed": feed.name},
            )
        return [None]

    if members:
        return list(Selection.resolve(feed.dimension, members=list(members)))
    if groups:
        return list(Selection.resolve(feed.dimension, groups=list(groups)))

    from_env = selections_for_env(feed.dimension, feed.name)
    if from_env:
        return list(from_env)

    return list(Selection.resolve(feed.dimension,
                                  default_group=feed.default_group))


# --------------------------------------------------------------------------- #
# data quality
# --------------------------------------------------------------------------- #
def check_quality(rows: list[Row], feed: Feed, selection: Selection | None,
                  *, strict: bool = True) -> None:
    got, want = len(rows), feed.expected_rows(selection)
    payload = {"event": "quality.check", "feed": feed.name,
               "group": selection.label if selection else None,
               "members": len(selection) if selection else None,
               "rows": got, "expected": want}
    if got < want:
        log.error("row count below expectation",
                  extra={**payload, "event": "quality.short"})
        if strict:
            raise QualityCheckFailed(
                f"{feed.name}[{payload['group']}]: {got} rows, expected {want}"
            )
    else:
        log.info("quality ok", extra=payload)


# --------------------------------------------------------------------------- #
# main entrypoint
# --------------------------------------------------------------------------- #
def ingest(
    feed_name: str,
    interval: datetime,
    *,
    groups: Sequence[str] | None = None,
    members: Sequence[str] | None = None,
    strict: bool = True,
    run_id: str | None = None,
) -> dict[str, int]:
    """Extract once, then parse + load for each selection.

    Returns {group_label: rows_written}. Raises PartialRunFailure if some
    selections failed and others succeeded, so a broken node group surfaces
    without discarding the groups that worked.
    """
    feed = FEEDS[feed_name]
    run_id = run_id or new_run_id()
    selections = resolve_selections(feed, groups=groups, members=members)

    with bind(feed=feed.name, run_id=run_id, interval=interval.isoformat()):
        url = feed.url(interval)
        results: dict[str, int] = {}
        failures: dict[str, str] = {}

        with timed(log, "ingest", url=url,
                   groups=[s.label if s else "all" for s in selections]):
            with timed(log, "extract"):
                payload = http.fetch_csv(url)

            with db.connect() as conn:
                for selection in selections:
                    label = selection.label if selection else "all"
                    try:
                        results[label] = _load_one(
                            payload, interval, feed, selection, conn, strict=strict
                        )
                    except Exception as exc:
                        failures[label] = f"{type(exc).__name__}: {exc}"
                        log.exception(
                            "selection failed",
                            extra={"event": "ingest.selection_error",
                                   "feed": feed.name, "group": label},
                        )

        if failures and not results:
            raise RuntimeError(f"{feed.name}: {failures}")
        if failures:
            log.error(
                "run partially failed",
                extra={"event": "ingest.partial", "feed": feed.name,
                       "succeeded": sorted(results), "failed": sorted(failures)},
            )
            raise PartialRunFailure(f"{feed.name}: {failures}")

        return results


def _load_one(payload: bytes, interval: datetime, feed: Feed,
              selection: Selection | None, conn, *, strict: bool) -> int:
    label = selection.label if selection else "all"
    with bind(group=label, members=len(selection) if selection else None):
        with timed(log, "parse"):
            rows = feed.parse(payload, interval, selection)
            log.info("parsed", extra={"event": "parse.ok", "rows": len(rows)})

        # Ingest writes only what SPP published. Gaps are handled by the
        # dedicated forward-fill job (spp/fill.py), which runs once daily and
        # marks filled rows source='forward_filled' — keeping published and
        # filled provenance cleanly separated.
        check_quality(rows, feed, selection, strict=strict)

        with timed(log, "load", table=feed.table):
            return db.upsert_rows(
                conn,
                table=feed.table,
                columns=feed.columns,
                conflict_keys=feed.conflict_keys,
                rows=_prepare(rows, feed),
                feed=feed.name,
                partitioned=feed.partitioned,
            )


def _prepare(rows: list[Row], feed: Feed) -> list[Row]:
    """Project each row to the target columns plus provenance and interval.

    interval_central resolves the timestamp_id FK in the upsert and is stored.
    gmt_interval_begin is carried through but is NOT a fact column — db.py uses
    it to correct timestamps.ts_gmt, then the upsert ignores it.

    Every ingested row is stamped source='published' / is_forward_filled=false
    explicitly, so that re-ingesting a real value OVER a previously
    forward-filled one flips it back to published — published always wins.
    """
    out = []
    for row in rows:
        clean = {c: row.get(c) for c in feed.columns if c != "timestamp_id"}
        clean["interval_central"] = row["interval_central"]
        if row.get("gmt_interval_begin") is not None:
            clean["gmt_interval_begin"] = row["gmt_interval_begin"]
        clean["source"] = "published"
        clean["is_forward_filled"] = False
        out.append(clean)
    return out


def run_range(feed_name: str, start_day: date, end_day: date, *,
              groups: Sequence[str] | None = None,
              members: Sequence[str] | None = None,
              strict: bool = False) -> int:
    """Ingest every interval a feed produces across [start_day, end_day].

    One entrypoint for both the daily schedule (start=end=today) and historical
    backfills (a wider range). Iterates at the feed's own cadence.
    """
    feed = FEEDS[feed_name]
    run_id = new_run_id()
    total, failures, skipped = 0, 0, 0

    day = start_day
    while day <= end_day:
        day_start = datetime(day.year, day.month, day.day)
        if feed.cadence == "5min":
            steps = [day_start + timedelta(minutes=5 * i) for i in range(288)]
        else:
            # DA/load: one file per day. The ingest target is the day itself.
            steps = [day_start]
        for target in steps:
            try:
                written = ingest(feed_name, target, groups=groups,
                                 members=members, strict=strict, run_id=run_id)
                total += sum(written.values())
            except http.FileNotPublished:
                skipped += 1
            except Exception:
                failures += 1
                log.exception("interval failed",
                              extra={"event": "range.interval_error",
                                     "feed": feed_name,
                                     "interval": target.isoformat()})
        day += timedelta(days=1)

    log.info("range complete",
             extra={"event": "range.ok", "feed": feed_name,
                    "start": start_day.isoformat(), "end": end_day.isoformat(),
                    "rows_written": total, "not_published": skipped,
                    "failed_intervals": failures})
    return total


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def _csv_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_date(value: str) -> date:
    """Accept YYYY-MM-DD or a full ISO datetime; return a date."""
    return datetime.fromisoformat(value).date()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="spp.run")
    ap.add_argument("--feed", required=True, choices=sorted(FEEDS))

    # Date-range model: --start required for a run, --end optional.
    ap.add_argument("--start", type=_parse_date,
                    help="first day to process (YYYY-MM-DD)")
    ap.add_argument("--end", type=_parse_date,
                    help="last day to process; defaults to --start (single day)")

    # Forward-fill: fill gaps for a day/range instead of ingesting.
    ap.add_argument("--fill", action="store_true",
                    help="forward-fill missing intervals instead of ingesting")
    ap.add_argument("--lookback-days", type=int, default=7,
                    help="how far back a fill may reach for a prior value")

    ap.add_argument("--groups", type=_csv_list,
                    help="named groups from the groups file, e.g. nppd,kmea")
    ap.add_argument("--members", type=_csv_list,
                    help="ad-hoc member list, e.g. NPPD_BPWN_NPPD or 1,5,SPP")
    ap.add_argument("--list-groups", action="store_true",
                    help="print the groups available for this feed and exit")
    ap.add_argument("--check-db", action="store_true",
                    help="verify the DSN and schema, then exit")
    ap.add_argument("--no-strict", action="store_true",
                    help="log quality failures instead of raising")
    args = ap.parse_args(argv)

    feed = FEEDS[args.feed]

    if args.check_db:
        print(db.ping())
        return 0

    if args.list_groups:
        from .selection import load_groups

        if not feed.subsettable:
            print(f"{args.feed} is not subsettable")
            return 0
        for name, _ in sorted(load_groups()[feed.dimension].items()):
            resolved = Selection.from_group(feed.dimension, name)
            print(f"{name:<16} {len(resolved):>3} member(s)  "
                  f"{', '.join(resolved.members)}")
        return 0

    if args.start is None:
        ap.error("--start is required (use --start YYYY-MM-DD)")
    start_day = args.start
    end_day = args.end or args.start
    if end_day < start_day:
        ap.error("--end must not be before --start")

    try:
        if args.fill:
            from .fill import forward_fill_range

            filled = forward_fill_range(args.feed, start_day, end_day,
                                        lookback_days=args.lookback_days)
            print(f"{args.feed}: {filled} rows forward-filled "
                  f"({start_day} .. {end_day})")
        else:
            total = run_range(args.feed, start_day, end_day,
                              groups=args.groups, members=args.members,
                              strict=not args.no_strict)
            print(f"{args.feed}: {total} rows ingested ({start_day} .. {end_day})")
    except http.FileNotPublished:
        log.error("upstream file not available",
                  extra={"event": "run.not_published", "feed": args.feed})
        return 2
    except Exception:
        log.exception("run failed", extra={"event": "run.error", "feed": args.feed})
        return 1
    finally:
        http.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
