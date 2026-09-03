# SPP pipeline — task reference

All schedules are US/Central. Every write is an idempotent upsert, so re-running
any task for any interval is safe.

## Real-time feeds

| DAG | Task | Schedule | What it does |
|---|---|---|---|
| `spp_rt_lmp` | `ingest` | `:02` past each 5-min interval | Sleeps `SPP_START_OFFSET_SECONDS` (default 15) to clear SPP's ~2m15s publication lag, fetches the RT LMP file for the interval that just closed, filters to the configured settlement locations, upserts to `rt_lmp` as `source='published'`. Retries every 10s; skips on the final attempt if still unpublished. |
| `spp_rt_lmp` | `verify` | downstream of `ingest` | Counts rows in `rt_lmp` for that interval and raises if fewer than `periods_per_run × members`. Catches a partial load the ingest reported as success. |
| `spp_rt_mcp` | `ingest` | `:02` past each 5-min interval | Identical, against the RT MCP file and reserve zones, writing to `rt_mcp`. |
| `spp_rt_mcp` | `verify` | downstream of `ingest` | Same coverage check against `rt_mcp`. |

## Daily feeds

| DAG | Task | Schedule | What it does |
|---|---|---|---|
| `spp_day_ahead` | `ingest_da_lmp` | 14:00 daily | Fetches tomorrow's day-ahead LMP file, shifts hour-ending to hour-beginning, upserts 24 hours × members to `da_lmp`. 12 retries at 15 min covers publication drift to ~17:00. |
| `spp_day_ahead` | `ingest_da_mcp` | 14:00 daily | Same for day-ahead marginal clearing prices by reserve zone → `da_mcp`. Runs in parallel with the LMP task. |
| `spp_hourly_load` | `load` | 17:15 daily | Fetches yesterday's hourly load file and writes one row per hour with a column per utility to `hourly_load`. Handles both upstream formats — see below. `catchup=True`, so unpausing backfills from the start date. |

## Manual operations

Not scheduled. Run from `astro dev bash`.

| Command | What it does |
|---|---|
| `python -m spp.run --feed <f> --start <d> [--end <d>]` | Ingest a day or a range at the feed's own cadence. Skips intervals SPP never published. |
| `python -m spp.run --feed <f> --fill --start <d>` | Forward-fill gaps for a day from each dimension's most recent prior value, marked `source='forward_filled'`. |
| `python -m spp.run --feed <f> --check-db` | Verify the DSN and report the spine row count. |
| `python -m spp.run --feed <f> --list-groups` | Print the resolved groups available for that feed. |

## How a target interval is derived

Every task computes its target from the DAG run's own data interval, never from
the wall clock, so retries and backfills hit the same interval regardless of the
host's timezone.

| DAG | Source | Derivation |
|---|---|---|
| `spp_rt_lmp`, `spp_rt_mcp` | `data_interval_end` | Floor to the 5-minute grid. A run firing 10:37 targets interval-beginning 10:35. |
| `spp_day_ahead` | `data_interval_end` | Plus one day, at midnight. A run firing Sept 1 targets operating day Sept 2. |
| `spp_hourly_load` | `data_interval_end` | Minus one day, at midnight. A run firing Sept 2 targets Sept 1. |

`data_interval_end` is the run's execution time for a cron schedule;
`data_interval_start` is one full interval earlier, and using it silently
ingests the wrong day. `logical_date` is `None` on manual and asset-triggered
runs in Airflow 3 and must not be used here.

## Timestamp and format conventions

**Interval-beginning.** SPP publishes `Interval` as the interval *ending* time;
everything in this warehouse keys on interval *beginning*. Each parser shifts
back one period — 5 minutes for RT, 1 hour for DA.

**RT file naming.** The RT URL builder shifts *forward* one period, because
SPP names files by ending time: the 10:35-beginning interval is published as
`...1040.csv`. Midnight-ending lives in the previous day's folder. The DA URLs
need no shift — their hardcoded `0100` suffix is already SPP's first
hour-ending of the operating day.

**`hourly_load` has two upstream formats.** Files from 2026 onward are long —
one row per (Market Hour, Control Zone Name, Forecast Area Type), summed across
forecast-area-type and pivoted wide, with Market Hour in Central wall-clock.
Older files are already wide, with `MarketHour` in **GMT**, and carry only 17 of
the 20 control zones. The wide branch converts through `America/Chicago` rather
than subtracting a fixed offset, since the Central day begins at 07:00Z under
CST and 06:00Z under CDT. It also emits `gmt_interval_begin`, so backfilling
this feed corrects `timestamps.ts_gmt` from real published data.

**Header drift.** Column names are matched canonically — case, spaces and
underscores ignored — so `MarketHour` / `Market Hour` and `RegUpMile` /
`RegUPMile` both resolve. A genuinely missing column logs
`parse.schema_drift` with the headers that were present, then raises.

**Provenance.** Ingest always writes `source='published'`, so re-ingesting real
data over a previously forward-filled row flips it back — published always wins.

## Known gaps in this cut

- No gap sweeper and no scheduled forward fill. Both were dropped to keep the
  first release simple; `spp/fill.py` still works by hand.
- The RT path has an independent `verify` task; the daily path relies only on
  `ingest`'s `strict=True` row-count check.
- DST days produce 23 or 25 hourly rows against an expected 24, so `strict=True`
  fails on them. Use `--no-strict` for historical loads.