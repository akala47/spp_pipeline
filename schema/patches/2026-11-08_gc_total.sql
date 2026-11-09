-- 2026-11-08_gc_total.sql — add the Garden City total load as a location.
-- Run after 2026-11-06_scada_locations.sql. A new database gets the row from
-- 01_schema.sql. Safe to run again.
--
--     psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/patches/2026-11-08_gc_total.sql

INSERT INTO scada_locations (location, site, meter, description) VALUES
    ('GC_TOTAL', 'Garden City', 'Total', 'Garden City total load, as the HMI shows it')
ON CONFLICT (location) DO NOTHING;
