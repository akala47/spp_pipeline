"""SCADA site load: Beatrice city load, scraped once a minute.

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
