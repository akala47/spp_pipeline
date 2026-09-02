"""Feed definitions.

One `Feed` object per SPP dataset holds everything that differs between them —
URL shape, CSV parsing, target table, conflict key, which dimension it is
subsettable on — so the runner in `run.py` is identical for all five.

Subsetting
----------
Each feed declares a `dimension` (`settlement_location`, `reserve_zone`, or
None for feeds that aren't subsettable). Parsers take a `Selection` and filter
to its members, so one downloaded payload can be loaded for one group or
several without re-fetching. See `spp/selection.py`.

Timestamp convention
--------------------
SPP publishes `Interval` as the interval *ending* time in Central, and
`GMTIntervalEnd` alongside it. Everything downstream in this warehouse is keyed
on interval *beginning*, so each parser shifts back by one period (1 hour for
hourly feeds, 5 minutes for RT) and renames the GMT column accordingly.

`hourly_load` is the exception, because SPP changed its format. See
`parse_hourly_load` — the older wide files carry a GMT hour-ending column and
no Central column at all, so that branch converts rather than subtracts.

Header drift
------------
SPP ships columns with stray spaces (' PNODE Name'), inconsistent casing
('RegUpMile' vs 'RegUPMile') and inconsistent spacing ('MarketHour' vs
'Market Hour'). `_resolve_columns` matches on a canonical key — no case, no
spaces, no underscores — and renames to the expected spelling, so a cosmetic
change upstream is absorbed instead of raising KeyError.

Numeric coercion
----------------
Parsers return plain Python floats, not the numpy scalars pandas hands back
from `iterrows()`. psycopg2 cannot adapt `numpy.float64`, and `NaN` is not a
legal value for `numeric` — both are turned into `None` / `float` here, at the
edge, rather than being discovered at INSERT time.
"""
from __future__ import annotations

import io
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Sequence
from zoneinfo import ZoneInfo

import pandas as pd

from .config import SPP_PORTAL
from .logging_setup import get_logger
from .selection import RESERVE_ZONE, SETTLEMENT_LOCATION, Selection

log = get_logger("spp.feeds")

Row = dict[str, Any]

CENTRAL = ZoneInfo("America/Chicago")


def _num(value: Any) -> float | None:
    """numpy scalar / NaN / blank -> float | None, so psycopg2 can adapt it."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(out) or math.isinf(out) else out


def _text(value: Any) -> str | None:
    if value is None:
        return None
    out = str(value).strip()
    return out or None


def _read_csv(payload: bytes) -> pd.DataFrame:
    """Parse and strip whitespace from headers.

    Casing and internal spacing are handled downstream by `_resolve_columns`,
    which matches canonically — so there is no alias table to maintain here.
    """
    df = pd.read_csv(io.BytesIO(payload))
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _canon(name: str) -> str:
    """Comparison key for a header: no case, spaces or underscores."""
    return "".join(str(name).split()).replace("_", "").lower()


def _resolve_columns(df: pd.DataFrame, expected: Sequence[str],
                     feed: str) -> pd.DataFrame:
    """Rename columns to the expected spelling, ignoring case and spacing.

    Returns a new frame — callers MUST rebind, e.g.
    `df = _resolve_columns(df, [...], feed)`. Raises KeyError listing every
    genuinely missing column, after logging `parse.schema_drift` with the
    headers that were actually present.
    """
    lookup: dict[str, str] = {}
    for actual in df.columns:
        lookup.setdefault(_canon(actual), actual)

    renames, missing = {}, []
    for want in expected:
        actual = lookup.get(_canon(want))
        if actual is None:
            missing.append(want)
        elif actual != want:
            renames[actual] = want

    if renames:
        df = df.rename(columns=renames)
        log.info("normalised column names",
                 extra={"event": "parse.header_normalised", "feed": feed,
                        "renamed": renames})
    if missing:
        log.error(
            "upstream schema changed",
            extra={"event": "parse.schema_drift", "feed": feed,
                   "missing": missing, "present": list(df.columns)},
        )
        raise KeyError(f"{feed}: missing columns {missing}")
    return df


def _find_column(df: pd.DataFrame, *candidates: str) -> str | None:
    """First candidate present under any casing/spacing, or None."""
    lookup = {_canon(c): c for c in df.columns}
    for cand in candidates:
        hit = lookup.get(_canon(cand))
        if hit is not None:
            return hit
    return None


def _to_interval_begin(df: pd.DataFrame, shift: timedelta) -> pd.DataFrame:
    """Naive-parse the two timestamp columns and shift to interval-beginning.

    Both columns move together: `Interval` is Central wall-clock and
    `GMTIntervalEnd` is UTC, and shifting each by the same period keeps the
    pair consistent without needing a timezone conversion.
    """
    df = df.rename(columns={"GMTIntervalEnd": "gmt_interval_begin"})
    df["interval_central"] = pd.to_datetime(df["Interval"]) - shift
    df["gmt_interval_begin"] = pd.to_datetime(df["gmt_interval_begin"]) - shift
    return df


def _apply_selection(df: pd.DataFrame, column: str, selection: Selection,
                     feed: str) -> pd.DataFrame:
    """Filter to the selection's members and report anything not in the file.

    A requested member absent from the payload is logged loudly rather than
    silently yielding fewer rows — that distinction is the difference between
    "SPP dropped a node" and "someone typo'd a group entry".
    """
    df[column] = df[column].astype(str).str.strip()
    available = set(df[column])
    df = df[df[column].isin(selection.member_set)].copy()

    absent = sorted(selection.member_set - available)
    if absent:
        log.warning(
            "requested members absent from file",
            extra={"event": "parse.member_missing", "feed": feed,
                   "dimension": selection.dimension, "group": selection.label,
                   "missing": absent, "requested": len(selection)},
        )
    return df


def parse_lmp(payload: bytes, ts: datetime, selection: Selection, *,
              shift: timedelta, feed: str) -> list[Row]:
    df = _read_csv(payload)
    df = _resolve_columns(df, ["Interval", "GMTIntervalEnd",
                               "Settlement Location", "LMP", "MLC", "MCC",
                               "MEC"], feed)

    # SPP has shipped this column as both 'Pnode' and 'PNODE Name'.
    pnode_col = _find_column(df, "Pnode", "PNODE Name")
    if pnode_col is None:
        log.error(
            "upstream schema changed",
            extra={"event": "parse.schema_drift", "feed": feed,
                   "missing": ["Pnode"], "present": list(df.columns)},
        )
        raise KeyError(f"{feed}: missing columns ['Pnode']")

    df = _apply_selection(df, "Settlement Location", selection, feed)
    df = _to_interval_begin(df, shift)
    return [
        {
            "interval_central": r["interval_central"].to_pydatetime(),
            "gmt_interval_begin": r["gmt_interval_begin"].to_pydatetime(),
            "settlement_location": _text(r["Settlement Location"]),
            "pnode": _text(r[pnode_col]),
            "lmp": _num(r["LMP"]),
            "mlc": _num(r["MLC"]),
            "mcc": _num(r["MCC"]),
            "mec": _num(r["MEC"]),
        }
        for _, r in df.iterrows()
    ]


def parse_da_mcp(payload: bytes, ts: datetime, selection: Selection) -> list[Row]:
    df = _read_csv(payload)
    df = _resolve_columns(df, ["Interval", "GMTIntervalEnd", "Reserve Zone",
                               "RegUP", "RegDN", "Spin", "Supp", "RampUP",
                               "RampDN", "UncUP"], "da_mcp")
    df = _apply_selection(df, "Reserve Zone", selection, "da_mcp")
    df = _to_interval_begin(df, timedelta(hours=1))
    return [
        {
            "interval_central": r["interval_central"].to_pydatetime(),
            "gmt_interval_begin": r["gmt_interval_begin"].to_pydatetime(),
            "reserve_zone": _text(r["Reserve Zone"]),
            "regup": _num(r["RegUP"]), "regdn": _num(r["RegDN"]),
            "spin": _num(r["Spin"]), "supp": _num(r["Supp"]),
            "rampup": _num(r["RampUP"]), "rampdn": _num(r["RampDN"]),
            "uncup": _num(r["UncUP"]),
        }
        for _, r in df.iterrows()
    ]


def parse_rt_mcp(payload: bytes, ts: datetime, selection: Selection) -> list[Row]:
    df = _read_csv(payload)
    df = _resolve_columns(df, ["Interval", "GMTIntervalEnd", "Reserve Zone",
                               "RegUPService", "RegDNService", "RegUPMile",
                               "RegDNMile", "Spin", "Supp", "RampUP", "RampDN",
                               "UncUP"], "rt_mcp")
    df = _apply_selection(df, "Reserve Zone", selection, "rt_mcp")
    df = _to_interval_begin(df, timedelta(minutes=5))
    return [
        {
            "interval_central": r["interval_central"].to_pydatetime(),
            "gmt_interval_begin": r["gmt_interval_begin"].to_pydatetime(),
            "reserve_zone": _text(r["Reserve Zone"]),
            "regupservice": _num(r["RegUPService"]),
            "regdnservice": _num(r["RegDNService"]),
            "regupmile": _num(r["RegUPMile"]), "regdnmile": _num(r["RegDNMile"]),
            "spin": _num(r["Spin"]), "supp": _num(r["Supp"]),
            "rampup": _num(r["RampUP"]), "rampdn": _num(r["RampDN"]),
            "uncup": _num(r["UncUP"]),
        }
        for _, r in df.iterrows()
    ]


# The 17 original control zones plus the 3 that appeared in the July 2026 files
# (PRPA, WACM, WAUW). They were being pivoted and then silently dropped.
LOAD_UTILITIES = ["CSWS", "EDE", "GRDA", "INDN", "KACY", "KCPL", "LES", "MPS",
                  "NPPD", "OKGE", "OPPD", "PRPA", "SECI", "SPRM", "SPS", "WACM",
                  "WAUE", "WAUW", "WFEC", "WR"]


def parse_hourly_load(payload: bytes, ts: datetime,
                      selection: Selection | None = None) -> list[Row]:
    """Hourly load by utility. Not subsettable (utilities become columns here).

    SPP has published this feed in two shapes, distinguished by whether
    'Control Zone Name' is present:

    LONG (2026-)
        One row per (Market Hour, Control Zone Name, Forecast Area Type)
        carrying a single 'Load MW'. A zone's load is split across
        forecast-area-types (CF conforming / NC non-conforming) and must be
        SUMMED, then pivoted long -> wide. 'Market Hour' is CENTRAL wall-clock,
        hour-ending. No GMT column, so `db.sync_timestamps` computes ts_gmt
        from the America/Chicago rules.

    WIDE (-2025)
        'MarketHour' plus one column per utility. No pivot needed, and earlier
        files carry only 17 of the 20 zones. 'MarketHour' is GMT, hour-ending:
        the Central day starts at 07:00Z under CST and 06:00Z under CDT, so a
        fixed offset is wrong for half the year and this branch converts
        properly. It also emits gmt_interval_begin, which means backfilling
        this feed corrects timestamps.ts_gmt from real published data rather
        than from a computed value.

    Both shapes produce one row per hour with a column per utility, keyed on
    interval-beginning.
    """
    df = _read_csv(payload)
    is_long = _find_column(df, "Control Zone Name") is not None

    if is_long:
        df = _resolve_columns(df, ["Market Hour", "Control Zone Name",
                                   "Load MW"], "hourly_load")
        df["Control Zone Name"] = df["Control Zone Name"].astype(str).str.strip()
        # Sum across forecast-area-type so each (hour, zone) is one number.
        agg = (df.groupby(["Market Hour", "Control Zone Name"], as_index=False)
                 ["Load MW"].sum())
        wide = agg.pivot(index="Market Hour", columns="Control Zone Name",
                         values="Load MW").reset_index()
        hour_col = "Market Hour"
        # Central wall-clock, hour-ending -> hour-beginning.
        wide["interval_central"] = (
            pd.to_datetime(wide[hour_col]) - pd.Timedelta(hours=1)
        )
        has_gmt = False
    else:
        wide = _resolve_columns(df, ["MarketHour"], "hourly_load")
        hour_col = "MarketHour"
        # GMT, hour-ending. Shift as an instant, then convert, so the
        # CST/CDT boundary lands on the right Central hour.
        gmt_end = pd.to_datetime(wide[hour_col], utc=True)
        gmt_begin = gmt_end - pd.Timedelta(hours=1)
        wide["gmt_interval_begin"] = gmt_begin.dt.tz_localize(None)
        wide["interval_central"] = (
            gmt_begin.dt.tz_convert(CENTRAL).dt.tz_localize(None)
        )
        has_gmt = True

    derived = {hour_col, "interval_central", "gmt_interval_begin"}
    present = [u for u in LOAD_UTILITIES if u in wide.columns]
    absent = [u for u in LOAD_UTILITIES if u not in wide.columns]
    unmapped = [c for c in wide.columns
                if c not in derived and c not in LOAD_UTILITIES]
    if absent:
        log.warning(
            "load utilities absent from file",
            extra={"event": "parse.member_missing", "feed": "hourly_load",
                   "missing": absent, "format": "long" if is_long else "wide"},
        )
    if unmapped:
        # A new control zone appeared upstream and has nowhere to land.
        log.error(
            "control zone has no column in hourly_load",
            extra={"event": "parse.schema_drift", "feed": "hourly_load",
                   "unmapped": unmapped},
        )

    rows: list[Row] = []
    for _, r in wide.iterrows():
        row: Row = {"interval_central": r["interval_central"].to_pydatetime()}
        if has_gmt:
            row["gmt_interval_begin"] = r["gmt_interval_begin"].to_pydatetime()
        for u in LOAD_UTILITIES:
            row[u.lower()] = _num(r[u]) if u in present else None
        rows.append(row)
    return rows


# --------------------------------------------------------------------------- #
# feed registry
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Feed:
    name: str
    table: str
    url: Callable[[datetime], str]
    parse: Callable[..., list[Row]]
    columns: Sequence[str]
    conflict_keys: Sequence[str]
    cadence: str                       # "5min" | "hourly"
    periods_per_run: int               # 24 for a daily file, 1 for an RT interval
    dimension: str | None = None       # what this feed can be subset on
    default_group: str = "default"
    partitioned: bool = False          # RANGE-partitioned monthly on interval_central

    @property
    def subsettable(self) -> bool:
        return self.dimension is not None

    def expected_rows(self, selection: Selection | None) -> int:
        members = len(selection) if selection else 1
        return self.periods_per_run * members


def _pad(n: int) -> str:
    return str(n).zfill(2)


def _rt_file_date(end: datetime) -> datetime:
    """Folder date for an RT file, given its interval-ENDING timestamp.

    Midnight-ending lives in the previous day's folder:
    RTBM-LMP-SL-202608130000.csv is under 2026/08/By_Interval/12/.
    """
    return end - timedelta(days=1) if (end.hour == 0 and end.minute == 0) else end


def _rt_path(dataset: str, prefix: str, ts: datetime) -> str:
    """Build an RT file URL from an interval-BEGINNING timestamp.

    Everything in this package keys on interval-beginning; SPP names its RT
    files by interval-ending. The 10:35-beginning interval is published as
    <prefix>-...1040.csv, so shift forward one period here.
    """
    end = ts + timedelta(minutes=5)
    folder = _rt_file_date(end)
    return (
        f"{SPP_PORTAL}/{dataset}"
        f"?path=%2F{folder.year}%2F{_pad(folder.month)}"
        f"%2FBy_Interval%2F{_pad(folder.day)}%2F"
        f"{prefix}-{end:%Y%m%d%H%M}.csv"
    )


DA_LMP = Feed(
    name="da_lmp",
    table="da_lmp",
    url=lambda ts: (
        f"{SPP_PORTAL}/da-lmp-by-settlement-location"
        f"?path=%2F{ts.year}%2F{_pad(ts.month)}%2FBy_Day%2F"
        f"DA-LMP-SL-{ts:%Y%m%d}0100.csv"
    ),
    parse=lambda payload, ts, sel: parse_lmp(payload, ts, sel,
                                             shift=timedelta(hours=1),
                                             feed="da_lmp"),
    columns=("timestamp_id", "interval_central",
             "settlement_location", "pnode", "lmp", "mlc", "mcc", "mec",
             "source", "is_forward_filled"),
    conflict_keys=("timestamp_id", "settlement_location"),
    cadence="hourly",
    periods_per_run=24,
    dimension=SETTLEMENT_LOCATION,
)

DA_MCP = Feed(
    name="da_mcp",
    table="da_mcp",
    url=lambda ts: (
        f"{SPP_PORTAL}/da-mcp"
        f"?path=%2F{ts.year}%2F{_pad(ts.month)}%2FDA-MCP-{ts:%Y%m%d}0100.csv"
    ),
    parse=parse_da_mcp,
    columns=("timestamp_id", "interval_central",
             "reserve_zone", "regup", "regdn", "spin", "supp", "rampup",
             "rampdn", "uncup", "source", "is_forward_filled"),
    conflict_keys=("timestamp_id", "reserve_zone"),
    cadence="hourly",
    periods_per_run=24,
    dimension=RESERVE_ZONE,
)

# NOTE on the RT conflict keys: rt_lmp / rt_mcp are RANGE-partitioned on
# interval_central, and Postgres requires the partition key to be part of any
# unique constraint. The natural key is therefore
# (timestamp_id, dimension, interval_central) and ON CONFLICT must name all
# three — naming only two raises "no unique or exclusion constraint matching
# the ON CONFLICT specification". interval_central is functionally dependent on
# timestamp_id, so this is a Postgres formality, not a wider key.
RT_LMP = Feed(
    name="rt_lmp",
    table="rt_lmp",
    url=lambda ts: _rt_path("rtbm-lmp-by-location", "RTBM-LMP-SL", ts),
    parse=lambda payload, ts, sel: parse_lmp(payload, ts, sel,
                                             shift=timedelta(minutes=5),
                                             feed="rt_lmp"),
    columns=("timestamp_id", "interval_central",
             "settlement_location", "pnode", "lmp", "mlc", "mcc", "mec",
             "source", "is_forward_filled"),
    conflict_keys=("timestamp_id", "settlement_location", "interval_central"),
    cadence="5min",
    periods_per_run=1,
    dimension=SETTLEMENT_LOCATION,
    partitioned=True,
)

RT_MCP = Feed(
    name="rt_mcp",
    table="rt_mcp",
    url=lambda ts: _rt_path("rtbm-mcp", "RTBM-MCP", ts),
    parse=parse_rt_mcp,
    columns=("timestamp_id", "interval_central",
             "reserve_zone", "regupservice", "regdnservice", "regupmile",
             "regdnmile", "spin", "supp", "rampup", "rampdn", "uncup",
             "source", "is_forward_filled"),
    conflict_keys=("timestamp_id", "reserve_zone", "interval_central"),
    cadence="5min",
    periods_per_run=1,
    dimension=RESERVE_ZONE,
    partitioned=True,
)

HOURLY_LOAD = Feed(
    name="hourly_load",
    table="hourly_load",
    url=lambda ts: (
        f"{SPP_PORTAL}/hourly-load"
        f"?path=%2F{ts.year}%2FDAILY_HOURLY_LOAD-{ts:%Y%m%d}.csv"
    ),
    parse=parse_hourly_load,
    columns=("timestamp_id", "interval_central",
             *[u.lower() for u in LOAD_UTILITIES], "source", "is_forward_filled"),
    conflict_keys=("timestamp_id",),
    cadence="hourly",
    periods_per_run=24,
    dimension=None,
)

FEEDS: dict[str, Feed] = {f.name: f for f in
                          (DA_LMP, DA_MCP, RT_LMP, RT_MCP, HOURLY_LOAD)}