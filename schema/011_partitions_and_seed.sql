-- Partition management for rt_lmp / rt_mcp, and seeding of the timestamps spine.
-- Run after 010_schema.sql.


-- Called by the loader before writing an RT interval, so partitions appear
-- just-in-time and no cron is needed to pre-create them.

CREATE OR REPLACE FUNCTION ensure_month_partition(p_parent text, p_month date)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_start date := date_trunc('month', p_month)::date;
    v_end   date := (date_trunc('month', p_month) + interval '1 month')::date;
    v_part  text := format('%s_y%sm%s', p_parent,
                           to_char(v_start,'YYYY'), to_char(v_start,'MM'));
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_class WHERE relname = v_part) THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
            v_part, p_parent, v_start, v_end);
    END IF;
END $$;


-- seed_timestamps(from_ts, to_ts) — fill the 5-minute spine over a range.
-- Idempotent: ON CONFLICT on the unique ts_central.
--
-- ts_gmt is derived with the real America/Chicago rules rather than a flat
-- +6h, so it is correct on both sides of a DST boundary. The loader still
-- overwrites ts_gmt from SPP's own GMTIntervalEnd when it has the real value;
-- seeding just guarantees every interval has an id and a sane GMT.
CREATE OR REPLACE FUNCTION seed_timestamps(p_from timestamp, p_to timestamp)
RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE v_count bigint;
BEGIN
    INSERT INTO timestamps (ts_central, ts_gmt)
    SELECT g, (g AT TIME ZONE 'America/Chicago') AT TIME ZONE 'UTC'
    FROM generate_series(p_from, p_to, interval '5 minutes') g
    ON CONFLICT (ts_central) DO NOTHING;
    GET DIAGNOSTICS v_count = ROW_COUNT;
    RETURN v_count;
END $$;

-- Seed a sensible initial window (2019-01-01 .. 2028-01-01) to mirror the old
-- spine's coverage. ~1.05M rows; a few seconds. Adjust the range as needed.
-- For a small test database, a single year is plenty:
--   SELECT seed_timestamps('2026-01-01 00:00:00', '2027-01-01 00:00:00');
SELECT seed_timestamps('2019-01-01 00:00:00', '2028-01-01 00:00:00');

-- Pre-create RT partitions for the seeded span so historical backfills don't
-- each pay for a CREATE TABLE. Generates one partition per month.
DO $$
DECLARE m date;
BEGIN
    FOR m IN SELECT generate_series('2019-01-01'::date, '2027-12-01'::date,
                                    interval '1 month')::date LOOP
        PERFORM ensure_month_partition('rt_lmp', m);
        PERFORM ensure_month_partition('rt_mcp', m);
    END LOOP;
END $$;

ANALYZE timestamps;
