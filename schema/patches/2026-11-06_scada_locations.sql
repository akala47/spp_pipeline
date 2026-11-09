-- 2026-11-06_scada_locations.sql — bring a database built from an older
-- 01_schema.sql up to date: add scada_locations and the mw_updates.location
-- foreign key. A new database gets both from 01_schema.sql; do not run this
-- there (it is harmless, but not needed).
--
-- Rows with the old single-site code 'GARDEN_CITY' are the Jameson meter;
-- they are renamed to 'GC_JAMESON' so the history stays one series.
--
-- Safe to run again. It stops (and changes nothing) if mw_updates holds a
-- location that is not in the list below; add that code first.
--
--     psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/patches/2026-11-06_scada_locations.sql

BEGIN;

CREATE TABLE IF NOT EXISTS scada_locations (
    location    text PRIMARY KEY CHECK (location ~ '^[A-Z][A-Z0-9_]*$'),
    site        text NOT NULL,
    meter       text,
    description text
);

INSERT INTO scada_locations (location, site, meter, description) VALUES
    ('BEATRICE',   'Beatrice',    NULL,      'Beatrice city load'),
    ('GC_JAMESON', 'Garden City', 'Jameson', 'Garden City, Jameson meter (old code GARDEN_CITY)'),
    ('GC_LEWIS',   'Garden City', 'Lewis',   'Garden City, Lewis meter')
ON CONFLICT (location) DO NOTHING;

-- Old Jameson code -> new. A reading that exists under both codes is the
-- same reading: keep the GC_JAMESON row.
DELETE FROM mw_updates o
WHERE o.location = 'GARDEN_CITY'
  AND EXISTS (SELECT 1 FROM mw_updates n
              WHERE n.location = 'GC_JAMESON'
                AND n.timestamp_id = o.timestamp_id
                AND n.reading_timestamp = o.reading_timestamp);
UPDATE mw_updates SET location = 'GC_JAMESON' WHERE location = 'GARDEN_CITY';

DO $$
DECLARE v_unknown text;
BEGIN
    SELECT string_agg(DISTINCT m.location, ', ') INTO v_unknown
    FROM mw_updates m
    WHERE NOT EXISTS (SELECT 1 FROM scada_locations s WHERE s.location = m.location);
    IF v_unknown IS NOT NULL THEN
        RAISE EXCEPTION 'mw_updates has locations not in scada_locations: %', v_unknown;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'mw_updates_location_fkey'
                     AND conrelid = 'mw_updates'::regclass) THEN
        ALTER TABLE mw_updates
            ADD CONSTRAINT mw_updates_location_fkey FOREIGN KEY (location)
            REFERENCES scada_locations (location)
            ON UPDATE CASCADE ON DELETE RESTRICT;
    END IF;
END $$;

COMMIT;
