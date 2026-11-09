# SPP pipeline: task reference

All schedules are in US Central time. All writes are idempotent upserts. You
can run each task again for each interval, and you do not get duplicate rows.

For the time rules, refer to README → [Time conventions](README.md#time-conventions).
`spp/timing.py` holds these rules in code.

## Real-time feeds

| DAG | Task | Schedule | What it does |
|---|---|---|---|
| `spp_rt_lmp` | `ingest` | `:02` past each 5-minute interval | Waits `SPP_START_OFFSET_SECONDS` (default 15). Gets the RT LMP file for the interval that started 2 minutes before. Keeps the configured settlement locations. Writes them to `rt_lmp` with `source='published'`. Retries each 10 s. On the last attempt, if SPP did not publish the file, the task skips. |
| `spp_rt_lmp` | `verify` | After `ingest` | Counts the rows in `rt_lmp` for the `timestamp_id` of the interval. If the count is less than `periods_per_run × members`, the task fails. This finds a partial load that `ingest` did not report. |
| `spp_rt_mcp` | `ingest` | `:02` past each 5-minute interval | The same as `spp_rt_lmp`, for the RT MCP file and the reserve zones. Writes to `rt_mcp`. |
| `spp_rt_mcp` | `verify` | After `ingest` | The same row-count check, on `rt_mcp`. |
| `spp_gap_sweeper` | `sweep_rt_lmp`, `sweep_rt_mcp` | `:30` each hour | Looks at the last 6 hours. For each interval with fewer rows than the configured members, it loads the interval again. If you add a group to a Variable, the next sweep loads that group for the last 6 hours. |

## Daily feeds

| DAG | Task | Schedule | What it does |
|---|---|---|---|
| `spp_day_ahead` | `ingest_da_lmp` | 14:00 each day | Loads the DA LMP prints for **run day + 1** (a run on November 1 loads November 2). Moves hour-ending times back 1 hour. Writes 24 hours × members to `da_lmp`. 12 retries at 15-minute intervals cover a late publication until approximately 17:00. |
| `spp_day_ahead` | `ingest_da_mcp` | 14:00 each day | The same for DA MCP by reserve zone, into `da_mcp`. Runs in parallel with `ingest_da_lmp`. |
| `spp_hourly_load` | `load` | 17:15 each day | Loads the hourly load prints for **run day − 1** (a run on November 1 loads October 31). Writes one row for each hour, with one column for each utility, to `hourly_load`. Accepts the two file formats (refer to README). `catchup=True`: when you unpause it, Airflow runs it for each day from the start date. |

## SCADA site load

| DAG or process | Task | Schedule | What it does |
|---|---|---|---|
| `scada_beatrice` (VPS, Airflow) | `scrape` | Each minute | Starts headless Chrome, logs in to the Beatrice HMI and reads the MW value from the one-line diagram (old extract code). Writes one row to `mw_updates` with the spine id of the 5-minute interval of the reading. One retry after 10 s. No catchup: a run that occurs late would record the wrong time. |
| `spp.scada.garden_city` (VPN device, launchd) | loop | About each minute | Reads three values from the Garden City load SVG page (`GC_SVG_LINK`): `GC_JAMESON`, `GC_LEWIS` and `GC_TOTAL`, with one read time. The HMI is only available through the site VPN. A value that cannot be read is logged and skipped; the others are still written. Uses the loop of the old script: 60 s wait after a good reading, 5 s to 30 s wait after a failure, 5-minute pause after 5 failures in a row. Rejects values outside 0 to `GC_MW_CEIL` (`GC_TOTAL_MW_CEIL` for the total). |

## Manual operations

These operations do not have a schedule. Run them in the Airflow container
(`astro dev bash`) or in a virtual environment on the VPS (refer to
`DEPLOY_VPS.md`, step 10).

| Command | What it does |
|---|---|
| `python -m spp.run --feed <f> --start <d> [--end <d>]` | Loads one operating day or a range of operating days, at the cadence of the feed. Skips intervals that SPP did not publish. |
| `python -m spp.run --feed <f> --start <d> --by-run-date` | The same, but `<d>` is a run day. `da_*` loads run day + 1, `hourly_load` loads run day − 1. |
| `scripts/backfill.sh <feeds> <start> <end> [options]` | Runs the command above for each calendar month in the range, with one log file for each month in `logs/backfill/`. |
| `python -m spp.run --feed <f> --fill --start <d> [--end <d>]` | Forward fill: puts the last earlier value of the same node or zone into each missing interval. Marks the rows `source='forward_filled'`. |
| `python -m spp.run --feed <f> --check-db` | Makes sure that the DSN is correct and shows the number of spine rows. |
| `python -m spp.run --feed <f> --list-groups` | Shows the groups for the feed. |
| `python -m spp.scada.beatrice [--dry-run] [--headed]` | One Beatrice reading. |
| `python -m spp.scada.garden_city --once [--dry-run] [--headed]` | One Garden City reading of the three values (on the VPN device). |
| `python -m spp.scada.import_legacy dump.csv [--dry-run]` | Loads a CSV dump of the old `mw_updates` table into the new table. |
| `make reset-dev` | DEV ONLY. Deletes all pipeline tables and makes them again (`00_reset_dev.sql`, `01_schema.sql`, `02_seed.sql`). |

## How a task finds its target

Each task calculates its target from `data_interval_end` of its DAG run. For
a cron schedule in Airflow 3, this is the time at which the run starts. A task
never uses the wall clock. Thus a retry or a catchup run loads the same target
as the first run, on all hosts and in all time zones.

| DAG | Calculation | Example |
|---|---|---|
| `spp_rt_lmp`, `spp_rt_mcp` | Move the run time down to the 5-minute grid | A run at 10:37 loads the interval that starts at 10:35 (SPP file `…1040.csv`) |
| `spp_day_ahead` | Central date of the run time + 1 day | A run on November 1 loads operating day November 2 |
| `spp_hourly_load` | Central date of the run time − 1 day | A run on November 1 loads operating day October 31 |

Do not use `logical_date`. In Airflow 3 it is `None` for a manual run.

## Time conventions (summary)

- **Interval-beginning.** SPP publishes interval-ending times. Each parser
  subtracts one period: 5 minutes for RT, 1 hour for DA and hourly load.
- **Spine ids.** `<Excel date serial>HB<hh>MB<mm>` of the interval-beginning
  Central time. `46268HB17MB45` is 2026-09-03 17:45.
- **RT file names.** SPP names an RT file with the interval-ending time. The
  URL builder adds 5 minutes. The file of the interval that ends at midnight
  is in the folder of the previous day.
- **Hourly load times.** `Market Hour` is GMT, hour-ending, in both file
  formats. The parser subtracts 1 hour and converts to Central time.
- **DA file names.** The `0100` suffix is the first hour-ending of the
  operating day. The DA URLs do not need a shift.

## Known gaps

- There is no scheduled forward fill. Run `--fill` manually.
- The daily DAGs do not have a separate `verify` task. They use only the
  row-count check in `ingest` (`strict=True`).
- On a DST change day, an hourly file has 23 or 25 rows, not 24. Thus
  `strict=True` fails on these days. Use `--no-strict` for past data.
