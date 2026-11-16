-- 2026-11-16_sherburne_city_peak.sql — add the Sherburne town peak load as a
-- location. Run after 2026-11-11_sherburne_city.sql. A new database gets the
-- row from 01_schema.sql. Safe to run again.
--
--     psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/patches/2026-11-16_sherburne_city_peak.sql

INSERT INTO scada_locations (location, site, meter, description) VALUES
    ('SHERBURNE_CITY_PEAK', 'Sherburne', 'Peak', 'Sherburne town peak load')
ON CONFLICT (location) DO NOTHING;
