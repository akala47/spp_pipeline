# SPP data pipeline

Ingests five Southwest Power Pool datasets into Postgres on a schedule, with
idempotent loads and Airflow orchestration. Replaces a set of Selenium scrapers
driven by crontab.

| Feed | Table | Cadence | Published |
|---|---|---|---|
| RT LMP by settlement location | `rt_lmp` | 5 min | ~2m15s after interval start |
| RT MCP by reserve zone | `rt_mcp` | 5 min | ~2m15s after interval start |
| DA LMP by settlement location | `da_lmp` | daily | early afternoon, for next operating day |
| DA MCP by reserve zone | `da_mcp` | daily | early afternoon, for next operating day |
| Hourly load by utility | `hourly_load` | daily | for the previous operating day |

```
spp/          the package: config, http, selection, feeds, db, fill, run
schema/       010_schema.sql (tables), 011_partitions_and_seed.sql (spine)
config/       groups.json — named node / zone subsets
dags/         Airflow DAGs (spp_realtime.py, spp_daily.py)
tests/        offline parser tests + DB-backed lifecycle tests
```

See `spp_pipeline_tasks.md` for a task-by-task reference.

## Setup

Requires Docker and the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli).
Everything runs in containers, so no local Python or Airflow install is needed.

```bash
git clone <repo-url> && cd spp-data-pipeline
cp .env.example .env          # then edit SPP_DSN
```

### Schema

Any Postgres 14+, including a serverless instance:

```bash
export SPP_DSN='postgresql://user:pass@host:5432/spp?sslmode=require'
psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/010_schema.sql
psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/011_partitions_and_seed.sql
```

`011_partitions_and_seed.sql` seeds ~1.05M spine rows and 216 monthly
partitions. For a small test instance, narrow the two date ranges near the
bottom of that file first — they are commented inline. The loader self-seeds
any interval outside the range anyway.

### Start

```bash
astro dev start
```

Airflow comes up at `localhost:8080`. Verify the package and DSN resolve inside
the container:

```bash
astro dev bash
python -m spp.run --feed rt_lmp --check-db
python -m spp.run --feed rt_lmp --list-groups
```

If Postgres runs on the same host, use `host.docker.internal` in the DSN rather
than `localhost` — the container's `localhost` is itself.

### Resources

The full Astro stack needs headroom. On Linux, Docker uses host resources
directly and 8 GB is comfortable. On macOS via Colima, allocate explicitly:

```bash
colima start --cpu 4 --memory 8 --disk 60
```

Colima ignores resource flags on an existing VM, so `colima list` is worth
checking — under-allocation shows up as API-server crash loops and 30-second
DAG parse times rather than an obvious out-of-memory error.

## Running

Unpause `spp_rt_lmp`, `spp_rt_mcp`, `spp_day_ahead`, and `spp_hourly_load` in
the UI. Note `spp_hourly_load` has `catchup=True` and a start date of
2026-01-01, so unpausing triggers a backfill from that date.

| DAG | Schedule (Central) |
|---|---|
| `spp_rt_lmp`, `spp_rt_mcp` | `2,7,12,...,57 * * * *` — :02 past each interval |
| `spp_day_ahead` | `0 14 * * *` |
| `spp_hourly_load` | `15 17 * * *` |

RT tasks sleep `SPP_START_OFFSET_SECONDS` (default 15) before their first
attempt, landing at ~2m15s past the interval start where SPP publishes. Three
retries at 10s cover late files.

## Backfill

`run_range` handles historical loads from the CLI. It iterates at the feed's own
cadence and skips intervals SPP never published.

```bash
astro dev bash

# daily feeds — one file per day, fast
python -m spp.run --feed da_lmp      --start 2019-01-01 --end 2026-09-01 --no-strict
python -m spp.run --feed da_mcp      --start 2019-01-01 --end 2026-09-01 --no-strict
python -m spp.run --feed hourly_load --start 2019-01-01 --end 2026-09-01 --no-strict

# RT — 288 files per day per feed, see the warning below
python -m spp.run --feed rt_lmp --start 2026-06-01 --end 2026-09-01 --no-strict
```

Use `--no-strict` for history. The strict row-count check raises on any day
SPP published short, and on every DST transition, where an hourly file
legitimately carries 23 or 25 rows against an expected 24.

**RT backfills are expensive.** Each RT feed is 288 requests per day — a full
2019-to-present range is roughly 800,000 requests and 140 GB per feed, about
twelve days serially at observed fetch times. Check the files still exist
before starting:

```bash
curl -sI "https://portal.spp.org/file-browser-api/download/rtbm-lmp-by-location?path=%2F2019%2F01%2FBy_Interval%2F01%2FRTBM-LMP-SL-201901010005.csv" | head -1
```

Backfill RT in bounded windows, and run long jobs detached:

```bash
nohup python -m spp.run --feed rt_lmp --start 2019-01-01 --end 2019-03-31 \
  --no-strict > /tmp/backfill_rt_lmp_2019q1.log 2>&1 &
```

Because every write is an upsert, an interrupted backfill can simply be re-run
over the same range.

Backfilling `hourly_load` has a useful side effect: the older wide-format files
carry SPP's own GMT column, so every hour they touch corrects
`timestamps.ts_gmt` from published data rather than from a computed value.

## How it works

**Extract once, load many.** `run.ingest()` fetches an interval's CSV a single
time, then parses and loads it per selection. Four node groups cost four
parses, not four downloads.

**Every write is an upsert.** `ON CONFLICT (natural key) DO UPDATE` means
running the same interval five times is indistinguishable from running it once,
which is what turns the old defensive double-cron into a plain Airflow retry.

**Selections are config, not code.** `config/groups.json` defines named groups
with transitive `@references`; override per feed with the `SPP_<FEED>_GROUPS`
env var or the matching Airflow Variable. The env var takes effect only when
the Variable is unset.

**Timestamps are interval-beginning.** SPP publishes `Interval` as the interval
*ending* time; every parser shifts back one period (5 min for RT, 1 hour for
DA). The RT URL builder shifts *forward* one period to match SPP's file naming,
where the 10:35-beginning interval is published as `...1040.csv`. The DA URLs
need no shift — their hardcoded `0100` suffix is already SPP's first
hour-ending of the operating day.

**`hourly_load` has two upstream formats.** 2026-onward files are long, keyed
on Central wall-clock, and must be summed across forecast-area-type before
pivoting. Older files are already wide with `MarketHour` in GMT and only 17 of
the 20 control zones. The parser detects which and converts the GMT branch
through `America/Chicago`, since the Central day starts at 07:00Z under CST and
06:00Z under CDT.

**Headers are matched canonically** — case, spaces and underscores ignored — so
`MarketHour` / `Market Hour` and `RegUpMile` / `RegUPMile` both resolve without
an alias table. A genuinely missing column logs `parse.schema_drift` with the
headers that were present, then raises.

**Provenance is first class.** Ingest writes `source='published'`. Filled rows
carry `source='forward_filled'`. Re-ingesting real data over a filled row flips
it back — published always wins.

**Logs are JSON with a stable `event` field** (`quality.short`,
`parse.schema_drift`, `load.unmatched_timestamp`, …), so alerting is a field
match rather than a regex. Set `SPP_LOG_FORMAT=text` while developing.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `SPP_DSN` | — | Postgres DSN; required to write |
| `SPP_LOG_FORMAT` | `json` | `json` or `text` |
| `SPP_LOG_LEVEL` | `INFO` | |
| `SPP_HTTP_TIMEOUT` | `30` | Per-request timeout, seconds |
| `SPP_HTTP_RETRIES` | `3` | urllib3 retries inside one request |
| `SPP_START_OFFSET_SECONDS` | `15` | RT pre-fetch sleep |
| `SPP_TASK_RETRIES` | `3` | Airflow retries on RT ingest |
| `SPP_<FEED>_GROUPS` | — | e.g. `SPP_RT_LMP_GROUPS=nppd,kmea` |

Changes to `.env` require `astro dev restart`. Changes to files under `dags/`
are picked up automatically within ~30 seconds.

## Tests

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e '.[dev]'
pytest -q                                    # offline: no DB, no network
```

DB-backed tests delete rows, so point them at a throwaway database:

```bash
SPP_TEST_DSN='postgresql://user:pass@host:5432/spp_test' pytest -q
```

## Known gaps

- Backfill is serial. A thread pool over intervals in `run_range` would make RT
  history practical; right now it is the single biggest limitation.
- No gap sweeper and no scheduled forward fill in this cut. `spp/fill.py` and
  `python -m spp.run --fill` still work by hand.
- `hourly_load` stays wide (one column per zone). Long format keyed on
  `(timestamp_id, zone)` would make it subsettable like the others and absorb
  new zones without a migration.
- Loads are row-at-a-time so unmatched intervals can be counted individually.
  Fine at a few hundred rows per interval; switch to `execute_values` if you
  start loading the full node list.
- DST fall-back: the repeated 01:00–02:00 Central hour collides on
  `timestamps.ts_central`. SPP's GMT column disambiguates it, so the spine keeps
  the first pass. Storing `ts_central` as `timestamptz` would fix it properly.