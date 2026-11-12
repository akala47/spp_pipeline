-- 2026-11-11_sherburne_city.sql — add the Sherburne city load as a location.
-- Run after 2026-11-08_gc_total.sql. A new database gets the row from
-- 01_schema.sql. Safe to run again.
--
--     psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/patches/2026-11-11_sherburne_city.sql

INSERT INTO scada_locations (location, site, meter, description) VALUES
    ('SHERBURNE_CITY', 'Sherburne', NULL, 'Sherburne city load (Demand now)')
ON CONFLICT (location) DO NOTHING;
