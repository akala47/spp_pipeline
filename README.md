# SPP data pipeline

This pipeline loads two types of data into one Postgres database:

- Five Southwest Power Pool (SPP) market datasets.
- Site load readings (MW) from the SCADA meters at Beatrice and Garden City.

Airflow (Astro Runtime 3.3) starts the SPP jobs and the Beatrice job on a
schedule. The Garden City scraper is a standalone script on a different
device, because the Garden City HMI is only available through the site VPN.

All writes are upserts. You can run a job again for the same time and you do
not get duplicate rows.

All tables use one time key: the **spine id**. It is the same Excel-based ID
that our internal files use, so data that you download from the dashboard
agrees with those files. Refer to [Time conventions](#time-conventions).

| Feed | Table | Cadence | Published by the source |
|---|---|---|---|
| RT LMP by settlement location | `rt_lmp` | 5 min | About 2 min 15 s after the interval starts |
| RT MCP by reserve zone | `rt_mcp` | 5 min | About 2 min 15 s after the interval starts |
| DA LMP by settlement location | `da_lmp` | Daily | Early afternoon, for the next operating day |
| DA MCP by reserve zone | `da_mcp` | Daily | Early afternoon, for the next operating day |
| Hourly load by utility | `hourly_load` | Daily | For the previous operating day |
| SCADA site load (Beatrice, Garden City) | `mw_updates` | About 1 min | Read live from the HMI |

Other documents:

- [`DEPLOY_VPS.md`](DEPLOY_VPS.md): how to install and operate the pipeline on a VPS.
- [`PIPELINE_TASKS.md`](PIPELINE_TASKS.md): one entry for each scheduled task and manual command.

## Repository structure

```
spp/            Python package: config, http, feeds, timing, spine, selection,
                db, run, fill, logging_setup
spp/scada/      SCADA scrapers: beatrice, garden_city, common (transform and
                load), import_legacy (old mw_updates dump -> new table)
schema/         01_schema.sql   tables, functions, indexes
                02_seed.sql     spine rows and monthly partitions
                00_reset_dev.sql  DEV ONLY: delete all pipeline tables
config/         groups.json: named node and zone groups
dags/           Airflow DAGs: spp_realtime.py, spp_daily.py, scada.py
scripts/        backfill.sh: load a calendar range, one month at a time
deploy/         launchd unit (Garden City device), systemd unit (VPS)
tests/          offline tests and database tests
docker-compose.override.yml   Astro: host.docker.internal and /dev/shm on Linux
.env.example    all settings, with comments
```

## Time conventions

`spp/timing.py` holds all the rules below. The parsers, the URL builders, the
DAGs and the CLI use these rules. Do not write a time calculation in a
different place.

### 1. The spine id

`timestamps.id`, and the `timestamp_id` column of each fact table, has this
format:

```
<Excel date serial>HB<hour-beginning>MB<minute-beginning>
46268HB17MB45   = 2026-09-03 17:45, Central
```

- The Excel serial is the number of days from 1899-12-30. This is the same
  number that Excel shows for a date.
- The hour and the minute have two digits. Thus all ids have the same length,
  and the text order of the ids is the time order.
- Postgres calculates the id with the SQL function `spine_id()`.
  `spp/spine.py` is the Python copy (`spine_id`, `parse_spine_id`). The two
  must always give the same result. `tests/test_lifecycle.py` compares them.

### 2. Interval-beginning times

SPP publishes interval-ENDING times. This database keeps interval-BEGINNING
times, in Central wall-clock time. Each parser subtracts one period from the
SPP time:

| Feed | Period | SPP time (ending) | Stored time (beginning) | Spine id |
|---|---|---|---|---|
| `rt_lmp`, `rt_mcp` | 5 minutes | 10:40 | 10:35 | `…HB10MB35` |
| `da_lmp`, `da_mcp` | 1 hour | HE 01:00 | 00:00 | `…HB00MB00` |
| `hourly_load` | 1 hour | 06:00 GMT (= HE 01:00 CDT) | 00:00 | `…HB00MB00` |

The `hourly_load` files give `Market Hour` in **GMT**, hour-ending. The file
for October 31, 2026 (CDT) starts at 10/31 06:00 and stops at 11/01 05:00. A
CST day starts at 07:00 GMT. The parser subtracts 1 hour and then converts
the time to `America/Chicago`.

The RT file names also use the ending time. Thus the URL builder adds 5
minutes: the data for the 10:35 interval is in `RTBM-LMP-SL-…1040.csv`. The
file for the interval that ends at midnight is in the folder of the previous
day.

The SCADA readings are point readings. A reading at 10:47:11 goes on the id of
the 5-minute interval that contains it (`…HB10MB45`). With one reading each
minute, each id has about five readings.

### 3. Run day and operating day

The **operating day** is the date of the prints (the data). The **run day** is
the Central date on which the job runs.

| Feed | Operating day | Example |
|---|---|---|
| `da_lmp`, `da_mcp` | Run day + 1 | A run on November 1 loads the November 2 prints |
| `hourly_load` | Run day − 1 | A run on November 1 loads the October 31 prints |
| `rt_lmp`, `rt_mcp` | Same day | Each run loads the interval that just started |

The DAGs calculate the run day from `data_interval_end` (the time at which the
cron run starts). They do not use the wall clock. Thus a retry or a catchup
run loads the same day as the first run.

The CLI (`python -m spp.run`) uses operating days. Add `--by-run-date` if you
want to give run days. Refer to [Load a date range](#load-a-date-range).

## Development setup

Install Docker and the [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli).
Airflow runs in containers. You do not need a local Airflow.

```bash
git clone <repo-url> spp_data_pipeline && cd spp_data_pipeline
cp .env.example .env          # then set SPP_DSN and the SCADA values
```

### Database

You need Postgres 14 or later. There is no migration chain. Two scripts make
a new database:

```bash
export SPP_DSN='postgresql://user:pass@host:5432/spp'
psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/01_schema.sql   # tables, functions, indexes
psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/02_seed.sql     # spine and partitions
```

`02_seed.sql` adds the spine rows and the monthly partitions from 2019-01-01
to 2027-12-31 (about 0.95 million spine rows, a few seconds). For a small test
database, give a smaller range:

```bash
psql "$SPP_DSN" -v ON_ERROR_STOP=1 -v seed_from=2026-01-01 -v seed_to=2026-12-31 \
     -f schema/02_seed.sql
```

You can run `02_seed.sql` again at any time. It only adds what is missing. The
loader also adds spine rows and partitions outside the seeded range when it
needs them.

To start again on a dev database, run the reset script first. **It deletes
all data in the pipeline tables.** It also removes the tables that the old
migration scripts made (for example `mw_updates_legacy`).

```bash
make reset-dev SPP_DSN="$SPP_DSN"                 # 00_reset_dev, 01_schema, 02_seed
make reset-dev SPP_DSN="$SPP_DSN" SEED_FROM=2026-01-01 SEED_TO=2026-12-31
```

### Start Airflow

```bash
astro dev start
```

The Airflow UI is at `localhost:8080`. To make sure that the package and the
DSN are correct, run these commands in the container:

```bash
astro dev bash
python -m spp.run --feed rt_lmp --check-db
python -m spp.run --feed rt_lmp --list-groups
```

If Postgres runs on the same computer, use `host.docker.internal` in the DSN.
In a container, `localhost` is the container. On Linux,
`docker-compose.override.yml` adds `host.docker.internal`.

The Astro stack needs approximately 8 GB of memory. On macOS with Colima, set
the resources when you make the VM (Colima ignores these flags for a VM that
exists):

```bash
colima start --cpu 4 --memory 8 --disk 60
```

## Scheduled runs

Unpause these DAGs in the UI: `spp_rt_lmp`, `spp_rt_mcp`, `spp_day_ahead`,
`spp_hourly_load`, `spp_gap_sweeper` and `scada_beatrice`.

| DAG | Schedule (Central) | Loads |
|---|---|---|
| `spp_rt_lmp`, `spp_rt_mcp` | `2,7,12,…,57 * * * *` | The 5-minute interval that started 2 minutes before |
| `spp_day_ahead` | `0 14 * * *` | Operating day = run day + 1 |
| `spp_hourly_load` | `15 17 * * *` | Operating day = run day − 1 |
| `spp_gap_sweeper` | `30 * * * *` | RT intervals with missing rows in the last 6 hours |
| `scada_beatrice` | `* * * * *` | One Beatrice reading |

`spp_hourly_load` has `catchup=True` and a start date of 2026-01-01. When you
unpause it, Airflow starts one run for each day from that date.

The RT tasks wait `SPP_START_OFFSET_SECONDS` (default 15) before the first
attempt. Thus the first attempt occurs at approximately 2 min 15 s after the
interval starts, when SPP publishes. Three retries at 10 s cover files that
are late.

## Load a date range

Use `python -m spp.run` to load past data (backfill), or to load a day again.
The command uses the cadence of the feed. It skips intervals that SPP did not
publish.

`--start` and `--end` are **operating days** (the dates of the prints). Both
days are included.

```bash
# DA prints for November 2 (the scheduled run on November 1 loads these)
python -m spp.run --feed da_lmp --start 2026-11-02
# the same, with the run day
python -m spp.run --feed da_lmp --start 2026-11-01 --by-run-date

# hourly load prints for October 2026
python -m spp.run --feed hourly_load --start 2026-10-01 --end 2026-10-31 --no-strict

# RT: 288 files for each day for each feed
python -m spp.run --feed rt_lmp --start 2026-10-01 --end 2026-10-31 --no-strict
```

For a long range, use `scripts/backfill.sh`. It divides the range into
calendar months and writes one log file for each month in `logs/backfill/`:

```bash
scripts/backfill.sh da_lmp,da_mcp,hourly_load 2025-01-01 2025-12-31
scripts/backfill.sh rt_lmp,rt_mcp 2026-06-01 2026-10-31
scripts/backfill.sh all 2026-10-01 2026-10-31
```

Use `--no-strict` for past data (`backfill.sh` adds it). Without it, the
row-count check stops on a short day. Each DST change day is a short or long
day: an hourly file has 23 or 25 rows, not 24.

**RT backfills are expensive.** Each RT feed has 288 files for each day. A full
range from 2019 is approximately 800,000 requests and 140 GB for each feed.
This takes approximately twelve days at the fetch times that we measured. Load
RT data in bounded windows, and run long jobs in the background. Refer to
[`DEPLOY_VPS.md`](DEPLOY_VPS.md), step 11.

All writes are upserts. If a backfill stops, run the same range again.

A backfill of `hourly_load` also corrects `timestamps.ts_gmt`: its files
contain the GMT time from SPP.

### Forward fill

`--fill` puts the last known value into missing intervals. It does not get
files from SPP. Filled rows have `source='forward_filled'`.

```bash
python -m spp.run --feed rt_lmp --fill --start 2026-10-01 --end 2026-10-31
scripts/backfill.sh rt_lmp,rt_mcp 2026-10-01 2026-10-31 --fill
```

A fill only uses values from the same node or zone, from an earlier
interval, and not more than `--lookback-days` (default 7) before the day. A
fill never writes an interval after the end of its day. A later load of
published data replaces a filled row.

## How it works

**Extract one time, load many times.** `run.ingest()` gets the CSV file for an
interval one time. Then it parses and loads the file for each selection. Four
node groups cause four parses, but only one download.

**All writes are upserts.** Each load uses `ON CONFLICT (natural key) DO
UPDATE`. If you run the same interval five times, the result is the same as
one run. An Airflow retry is thus always safe.

**The loader keeps the spine complete.** Before it writes facts,
`db.sync_timestamps()` adds the spine row for each interval that it needs. It
also writes `ts_gmt` from the GMT column of SPP, if the file has one. For RT
tables and `mw_updates`, `ensure_month_partition()` makes the monthly
partition if it does not exist.

**Selections are configuration, not code.** `config/groups.json` defines the
node and zone groups. A group can include other groups with `@name`. To
change the groups for one feed, set the Airflow Variable `spp_<feed>_groups`
(for example `spp_rt_lmp_groups = nppd,kmea`). If the Variable is not set,
the environment variable `SPP_<FEED>_GROUPS` applies. If neither is set, the
group `default` applies.

**`hourly_load` has two file formats.** Files from 2026 have one row for each
hour, zone and forecast-area-type. The parser adds the forecast-area-types
and pivots the zones into columns. Older files already have one column for
each zone, and they have only 17 of the 20 zones. In both formats the hour
column is GMT, hour-ending. The parser converts it through `America/Chicago`,
because the Central day starts at 06:00Z (CDT) or 07:00Z (CST). On the DST
fall-back day the file has 25 hours. Two of them become Central 01:00. The
parser keeps the first (CDT) hour and logs `parse.dst_repeat`.

**Header names can change.** The parsers compare header names without case,
spaces or underscores. Thus `MarketHour` and `Market Hour` are the same
column. If a column is missing, the parser logs `parse.schema_drift` with the
headers in the file, and then stops.

**Provenance.** Each row has a `source`: `published` or `forward_filled`. A
load of published data always replaces a filled value.

**Logs are JSON.** Each log line has an `event` field (for example
`quality.short`, `parse.schema_drift`, `parse.member_missing`). An alert can
use the field value. For development, set `SPP_LOG_FORMAT=text`.

## Postgres: partitions and query optimizations

### Partitions

| Table | Partitions | Why |
|---|---|---|
| `rt_lmp`, `rt_mcp` | Monthly, `RANGE (timestamp_id)` | 288 intervals each day. These tables become large. |
| `mw_updates` | Monthly, `RANGE (timestamp_id)` | About 1,440 readings for each site each day. |
| `da_lmp`, `da_mcp`, `hourly_load`, `timestamps` | None | 24 rows each day (or one row for each 5 minutes for the spine). These tables stay small. |

- **Bounds.** A month is the spine id range from the first day at 00:00 to the
  first day of the next month at 00:00. For example, September 2026 is
  `['46266HB00MB00', '46296HB00MB00')`. The ids have a fixed length, so the
  text range is the same as the time range.
- **Names.** `<table>_yYYYYmMM`, for example `rt_lmp_y2026m09`.
- **Creation.** `02_seed.sql` makes the partitions for the seed range.
  `ensure_month_partition(table, month)` makes a missing partition when the
  loader or the SCADA writer needs it. You do not need a cron job.
- **Keys.** Postgres requires the partition key in each unique constraint.
  Thus the primary key of `rt_lmp` and `rt_mcp` is `(id, timestamp_id)`, and
  the natural key starts with `timestamp_id`: `UNIQUE (timestamp_id,
  settlement_location)`. The primary key of `mw_updates` is `(timestamp_id,
  location, reading_timestamp)`.
- **Retention.** To remove a month of data, drop its partition (for example
  `DROP TABLE rt_lmp_y2019m01`). This is much faster than `DELETE`.

### Indexes

| Table | Index | Query that it serves |
|---|---|---|
| All fact tables | `UNIQUE (timestamp_id[, node/zone])` | All rows in an interval or a window. Also the `ON CONFLICT` target. |
| `rt_lmp`, `da_lmp` | `(settlement_location, timestamp_id) INCLUDE (lmp, mlc, mcc, mec)` | One node over time, from the index only |
| `rt_mcp`, `da_mcp` | `(reserve_zone, timestamp_id) INCLUDE (all prices)` | One zone over time, from the index only |
| `mw_updates` | `(location, timestamp_id) INCLUDE (reading_timestamp, mw_reading)` | One site over time, from the index only |
| `timestamps` | Primary key `id`, `UNIQUE (ts_central)`, `(cal_date)`, partial `(ts_central) WHERE is_hour_start` | id → time, time → id, one calendar day, hour starts |

### Design decisions that make queries fast

- **One access path.** Each query in the pipeline filters on `timestamp_id`:
  `verify`, the gap sweeper, forward fill and `db.coverage()`. The
  applications must do the same.
- **`COLLATE "C"` on all spine id columns.** Postgres compares the ids byte
  by byte. This is the fastest text comparison. The order is the same as the
  time order on all servers, for all locales.
- **Covering indexes.** The `INCLUDE` columns let Postgres answer "one node
  over time" with an index-only scan. It does not read the table.
- **No index on `interval_central`.** The column is only for readability. An
  index on it would make each write slower. A filter on it reads the full
  table.
- **Generated columns.** `timestamps.id` is `GENERATED ALWAYS AS
  (spine_id(ts_central)) STORED`, so the id and the time cannot disagree. The
  calendar columns (`cal_date`, `cal_hour`, `cal_minute`, `cal_dow`,
  `is_hour_start`) are stored too. An application can filter on them with no
  date calculation.
- **No lookup on write.** The loader calculates `timestamp_id` in Python
  (`spp/spine.py`). It does not read `timestamps` to find an id.
- **Forward fill uses the index backward.** The fill finds the last value with
  `ORDER BY timestamp_id DESC LIMIT 1` on the `(dimension, timestamp_id)`
  index. It does one set-based statement for each day, not one statement for
  each row.
- **Bulk spine writes.** `sync_timestamps()` uses `execute_values` (500 rows
  for each statement).
- **A CHECK keeps SCADA ids correct.** `mw_updates` has a CHECK that
  `timestamp_id` is the 5-minute interval of `reading_timestamp`.

### Maintenance

- Run `ANALYZE` after a seed. `02_seed.sql` does this for `timestamps`.
- Run `VACUUM (ANALYZE) <table>` after a large backfill. An index-only scan
  needs a current visibility map. Autovacuum does this later, but a manual
  run makes the queries fast immediately.
- To see if a query uses the partitions and the index, use `EXPLAIN`. The plan
  must show only the partitions in your range and an `Index Only Scan` (or
  `Index Scan`).

## Querying

Query the fact tables by `timestamp_id`.

1. **Calculate the id bounds in the application.** You do not need a lookup
   in `timestamps`.

   ```python
   from datetime import datetime
   from spp.spine import spine_id, parse_spine_id
   first = spine_id(datetime(2026, 9, 3, 0, 0))    # '46268HB00MB00'
   last  = spine_id(datetime(2026, 9, 3, 23, 55))  # '46268HB23MB55'
   ```

   In SQL: `spine_id('2026-09-03 00:00'::timestamp)`.

2. **Filter on the column itself.** Add the node or zone if there is one.

   ```sql
   -- one node over time: index-only scan on (settlement_location, timestamp_id)
   SELECT timestamp_id, lmp FROM rt_lmp
    WHERE settlement_location = :node
      AND timestamp_id BETWEEN :first AND :last
    ORDER BY timestamp_id;

   -- all zones in a window: unique (timestamp_id, reserve_zone) index
   SELECT * FROM rt_mcp WHERE timestamp_id BETWEEN :first AND :last;

   -- one hour
   SELECT * FROM hourly_load WHERE timestamp_id = '46268HB17MB00';

   -- one SCADA site over time
   SELECT reading_timestamp, mw_reading FROM mw_updates
    WHERE location = 'BEATRICE' AND timestamp_id BETWEEN :first AND :last
    ORDER BY reading_timestamp;
   ```

   On `rt_lmp`, `rt_mcp` and `mw_updates`, the `timestamp_id` range alone
   selects only the monthly partitions in the range. **Do not** put a function
   or a cast on the column (for example `substring(timestamp_id …)` or
   `timestamp_id::…`). This stops the index and the partition pruning.

3. **Join `timestamps` only for its other columns** (`ts_gmt`, `cal_dow`,
   `is_hour_start`, …). Join on `t.id = f.timestamp_id`. This is a
   primary-key lookup.

4. **Mix 5-minute and hourly data.** An hourly row has the `…MB00` id of its
   hour. Thus one id range selects both types of data.

`ORDER BY timestamp_id` gives the time order. Before you call `spine_id`, move
the bounds from a date picker to the 5-minute grid, or use `…HB00MB00` and
`…HB23MB55` for a full day.

## SCADA readings

`mw_updates` keeps the site load readings:

| Column | Meaning |
|---|---|
| `timestamp_id` | Spine id of the 5-minute interval that contains the reading. A reading at 10:47:11 is `…HB10MB45`. |
| `location` | `BEATRICE` or `GARDEN_CITY` |
| `reading_timestamp` | Central wall-clock time of the reading |
| `mw_reading` | MW, rounded half-up to 3 decimals |

The primary key `(timestamp_id, location, reading_timestamp)` makes a reading
that is sent again a no-op.

**Extract, transform, load.** The extract (scrape) code in
`spp/scada/beatrice.py` and `spp/scada/garden_city.py` is a copy of the old
scripts (`beatrice_mw.py`, `garden_city.py`). It uses the same Chrome options,
login, pages, element ids and waits. Do not change it. Only the transform and
the load are new. They are in `spp/scada/common.py`:

- **Transform.** `parse_mw_text()` reads the number, rounds it half-up to 3
  decimals and checks the range (0 to `BEATRICE_MW_CEIL` or `GC_MW_CEIL`).
  `Reading.from_aware()` changes the read time to Central wall-clock time and
  calculates the spine id.
- **Load.** `record()` adds the spine row and the partition, and inserts the
  row with `ON CONFLICT DO NOTHING`. It uses `SPP_DSN`. The old
  `connection_string` setting is not used.

There are two deployment settings for the Airflow image only:
`SCADA_CHROME_BINARY` and `SCADA_CHROMEDRIVER`. If you do not set them, Chrome
starts exactly as in the old scripts.

**Beatrice** runs on the VPS, in Airflow: DAG `scada_beatrice`, each minute,
live readings only (no catchup, one retry in the same minute). The Beatrice
HMI is available on the internet. It does not need a VPN. `packages.txt`
installs Chromium and chromedriver in the image. To run it manually:

```bash
python -m spp.scada.beatrice             # one reading, written to mw_updates
python -m spp.scada.beatrice --dry-run   # print the reading only
python -m spp.scada.beatrice --headed    # show the browser (development)
```

Each scraper stops before Chrome starts if a setting is missing. Beatrice
needs `BEATRICE_USER`, `BEATRICE_PWD`, `BEATRICE_LOGIN_URL` and
`BEATRICE_SVG_URL`. Garden City needs `GC_HOME_LINK` and `GC_SVG_XPATH`. Both
need `SPP_DSN`, except with `--dry-run`.

**Garden City** runs on the device that has the site VPN, as a standalone
loop. The loop is the same as in the old script:

- It starts a new browser for each reading.
- After a good reading, it waits 60 s.
- After a failure, it waits 5 s to 30 s.
- After 5 failures in a row, it waits 5 minutes.

The device must be able to connect to the Postgres database on the VPS (refer
to `DEPLOY_VPS.md`, step 12).

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[scada]'
.venv/bin/python -m spp.scada.garden_city --once --dry-run   # test
.venv/bin/python -m spp.scada.garden_city                    # loop
```

To keep the loop running on a Mac, install the launchd unit:

```bash
cp deploy/launchd/com.valuehash.scada-garden-city.plist ~/Library/LaunchAgents/
# set the two paths in the file, then:
launchctl load -w ~/Library/LaunchAgents/com.valuehash.scada-garden-city.plist
```

**Old readings.** `spp.scada.import_legacy` loads a CSV dump of the old
`mw_updates` table into the new table. Export the dump from Postgres. Do not
save it again from Excel: Excel shows the time as `46:51.5` and removes the
date.

```bash
python -m spp.scada.import_legacy dump.csv --dry-run   # check each row
python -m spp.scada.import_legacy dump.csv
```

If you run it again, it skips the readings that are already in the table. Add
`--source-tz UTC` if the old times are in UTC.

## Configuration

`.env.example` has all settings with comments.

| Variable | Default | Purpose |
|---|---|---|
| `SPP_DSN` | — | Postgres DSN. Necessary for all writes. |
| `SPP_LOG_FORMAT` | `json` | `json` or `text` |
| `SPP_LOG_LEVEL` | `INFO` | |
| `SPP_HTTP_TIMEOUT` | `30` | Time limit for one request, in seconds |
| `SPP_HTTP_RETRIES` | `3` | Retries in one request (urllib3) |
| `SPP_START_OFFSET_SECONDS` | `15` | Wait before the first RT attempt |
| `SPP_TASK_RETRIES` | `3` | Airflow retries for an RT task |
| `SPP_<FEED>_GROUPS` | — | For example `SPP_RT_LMP_GROUPS=nppd,kmea` |
| `SCADA_CHROME_BINARY` | — | Chromium path. `/usr/bin/chromium` in the Airflow image. |
| `SCADA_CHROMEDRIVER` | — | chromedriver path. `/usr/bin/chromedriver` in the Airflow image. |
| `SCADA_LOG_FILE` | — | Garden City: also write the log to this rotating file |
| `BEATRICE_USER`, `BEATRICE_PWD`, `BEATRICE_LOGIN_URL`, `BEATRICE_SVG_URL` | — | Beatrice HMI login and diagram page |
| `BEATRICE_MW_CEIL` | — | Optional upper limit for a Beatrice value |
| `GC_HOME_LINK`, `GC_SVG_XPATH` | — | Garden City HMI home page and the link to the diagram |
| `GC_USER`, `GC_PWD` | — | Only if the Garden City HMI shows a login form |
| `GC_MW_CEIL` | `10` | Upper limit for a Garden City value |

After you change `.env`, run `astro dev restart`. Airflow reads changes to
files in `dags/` in approximately 30 seconds.

## Tests

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e '.[dev,scada]'
pytest -q                                    # offline: no database, no network
```

The database tests delete rows. Use a database that you do not need:

```bash
createdb spp_test
psql spp_test -f schema/01_schema.sql
psql spp_test -v seed_from=2026-01-01 -v seed_to=2026-12-31 -f schema/02_seed.sql
SPP_TEST_DSN='postgresql://user:pass@localhost:5432/spp_test' pytest -q
```

Some parser tests need sample SPP files in `tests/fixtures/`
(`<feed>_sample.csv`). If a file is not there, pytest skips its tests.

## Known gaps

- Backfill is serial. A thread pool over the intervals in `run_range` would
  make a full RT history practical.
- There is no scheduled forward fill. Run `--fill` manually.
- `hourly_load` has one column for each zone. A long format with the key
  `(timestamp_id, zone)` would accept new zones with no schema change.
- The loads write one row for each statement. This is satisfactory for a few
  hundred rows for each interval. Use `execute_values` if you load the full
  node list.
- When DST ends, the hour 01:00–02:00 Central occurs two times. The times
  (and thus the spine ids) of the second hour are the same as those of the
  first hour. The spine keeps the first hour. This also applies to
  `mw_updates.reading_timestamp`. A `timestamptz` column for `ts_central`
  would correct this.

## VPS configuration

These values are for one VPS that runs the Astro (Airflow) stack, the
Beatrice scraper and the Postgres database. The Garden City scraper runs on
a different device. The data transfer values are estimates. Monitor the
real values after the first month.

| Resource | Minimum | Recommended | Notes |
|---|---|---|---|
| vCPU | 2 | 4 | Airflow scheduler, DAG processor, API server, triggerer, Postgres, and one Chromium start each minute |
| RAM | 8 GB | 16 GB | Astro stack approximately 4–5 GB, Postgres 1–2 GB, Chromium approximately 0.5 GB for each scrape |
| Storage (SSD) | 60 GB | 100 GB | Docker images approximately 6 GB, Airflow task logs (approximately 2,600 task runs each day), Postgres data and WAL. Add space for a full-node-list or full-history RT load. |
| Data transfer (outbound + inbound) | 10 GB/month | 25 GB/month | Scheduled loads approximately 2–5 GB/month. A full RT backfill from 2019 adds approximately 140 GB for each RT feed. |
