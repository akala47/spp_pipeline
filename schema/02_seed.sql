-- 02_seed.sql — seed the timestamps spine and the monthly partitions.
-- Run after 01_schema.sql. Safe to run again (it only adds what is missing).
--
-- Default range: 2019-01-01 .. 2027-12-31 (about 0.95M spine rows and
-- 108 months x 3 partitioned tables). Set a smaller range for a test database:
--     psql "$SPP_DSN" -v ON_ERROR_STOP=1 \
--          -v seed_from=2026-01-01 -v seed_to=2026-12-31 -f schema/02_seed.sql
-- The loader adds spine rows and partitions outside this range when it needs
-- them, so the range only sets what exists before the first load.

\if :{?seed_from}
\else
    \set seed_from 2019-01-01
\endif
\if :{?seed_to}
\else
    \set seed_to 2027-12-31
\endif

\echo seeding :seed_from .. :seed_to

BEGIN;

SELECT seed_timestamps(:'seed_from'::timestamp,
                       :'seed_to'::date + time '23:55') AS spine_rows_added;

SELECT seed_partitions(:'seed_from'::date, :'seed_to'::date) AS months_seeded;

COMMIT;

ANALYZE timestamps;
