"""SCADA site-load scrapers: Beatrice and Garden City MW readings.

Both scrape a vendor HMI page with headless Chrome and write one row per
reading to `mw_updates`. The scraping (extract) code is a copy of the old
scripts; only the transform and load (spp.scada.common) are new.

    timestamp_id       spine id of the 5-minute interval the reading falls in
    location           'BEATRICE' | 'GARDEN_CITY'
    reading_timestamp  Central wall-clock time the reading was taken
    mw_reading         MW, rounded half-up to 3 decimals

A reading taken every minute gives about 5 rows per timestamp_id.

    spp.scada.common       transform (parse, spine id) and load (mw_updates)
    spp.scada.beatrice     one scrape per run (Airflow DAG `scada_beatrice`)
    spp.scada.garden_city  standalone loop on the VPN-connected device
    spp.scada.import_legacy  load a CSV dump of the old mw_updates table

spp.scada.common and import_legacy do not import Selenium, so the parsing and
database code (and its tests) run without it. The two scrapers import it.
"""
