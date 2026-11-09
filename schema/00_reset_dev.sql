-- 00_reset_dev.sql — DEV ONLY. Remove every object the pipeline owns, so that
-- 01_schema.sql and 02_seed.sql can build the database again from zero.
--
-- WARNING: this deletes ALL data in these tables. Do not run it on a
-- database with data that you must keep.
--
--     psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/00_reset_dev.sql
--     psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/01_schema.sql
--     psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/02_seed.sql
--
-- It also removes the tables that the old migration chain (012-014) made,
-- for example mw_updates_legacy.

BEGIN;

DROP TABLE IF EXISTS rt_lmp, rt_mcp, da_lmp, da_mcp, hourly_load,
                     mw_updates, mw_updates_legacy, scada_locations,
                     timestamps CASCADE;

DROP FUNCTION IF EXISTS seed_partitions(date, date);
DROP FUNCTION IF EXISTS seed_timestamps(timestamp, timestamp);
DROP FUNCTION IF EXISTS ensure_month_partition(text, date);
DROP FUNCTION IF EXISTS spine_id(timestamp);
DROP TYPE IF EXISTS value_source;

COMMIT;
