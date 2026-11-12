"""SCADA site load from the vendor HMIs, on the VPS.

scada_beatrice   Beatrice city load, scraped once a minute (below).
scada_sherburne  Sherburne city load: one login every 15 minutes, then a
                 reading of "Demand now" each minute (see the DAG's own doc).

Each run starts headless Chrome, logs in to the Beatrice HMI, reads the MW
value from the one-line diagram and writes one row to `mw_updates` under the
spine id of the 5-minute interval it was read in (about 5 rows per id).

Garden City is NOT here: its HMI is only reachable over the site VPN, so it
runs as a long-lived process on the VPN device (`python -m
spp.scada.garden_city`, see README → SCADA readings).

Live readings only: catchup is off, and a run that misses its minute is not
replayed (the reading would be for the wrong time). Credentials come from the
environment (.env): BEATRICE_USER, BEATRICE_PWD, BEATRICE_LOGIN_URL,
BEATRICE_SVG_URL. Chrome and chromedriver come from packages.txt.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

CENTRAL = pendulum.timezone("US/Central")


@dag(
    dag_id="scada_beatrice",
    description="Beatrice city load (MW) from the SCADA HMI, every minute",
    schedule="* * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=CENTRAL),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        # One quick retry inside the same minute; after that, skip to the
        # next run rather than record a late reading.
        "retries": 1,
        "retry_delay": pendulum.duration(seconds=10),
        "execution_timeout": pendulum.duration(seconds=45),
    },
    tags=["scada", "mw"],
    doc_md=__doc__,
)
def scada_beatrice():
    @task(task_id="scrape")
    def scrape():
        from spp.scada.beatrice import run_once

        reading = run_once()
        return {"timestamp_id": reading.timestamp_id,
                "reading_timestamp": reading.reading_timestamp.isoformat(),
                "mw": str(reading.mw_reading)}

    scrape()


scada_beatrice()


SHERBURNE_DOC = """Sherburne city load (SHERBURNE_CITY), every 15 minutes.

Each run is one cycle of spp/scada/sherburne_city.py: headless Chromium logs
in (SC_LOGIN_LINK, SC_USER, SC_PWD), clicks "View real time graphs" and reads
"Demand now" (kW, divided by 1000) once a minute, 15 times. Each reading is
written to `mw_updates` as soon as it is taken. If the site logs out during
the run, or three readings in a row fail, the task logs in again in the same
browser (up to 3 times) and carries on.

The task takes about 14.5 minutes and stops reading before the next run is
due. No retries: a retry would start late and overlap the next run, which
logs in again anyway. A run is red when no reading was taken or readings
could not be written; the task log shows each reading and each login.

The standalone copy of the script on the device is the standby: keep it
stopped while this DAG is on, or every reading is stored twice.
"""


@dag(
    dag_id="scada_sherburne",
    description="Sherburne city load (kW -> MW): one login, 15 readings, every 15 min",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=CENTRAL),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 0,
        "execution_timeout": pendulum.duration(minutes=16),
    },
    tags=["scada", "mw"],
    doc_md=SHERBURNE_DOC,
)
def scada_sherburne():
    @task(task_id="scrape_cycle")
    def scrape_cycle():
        from spp.scada.sherburne_city import run_scheduled_cycle

        return run_scheduled_cycle()

    scrape_cycle()


scada_sherburne()
