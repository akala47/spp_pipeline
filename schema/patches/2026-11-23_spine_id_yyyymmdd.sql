-- 2026-11-23_spine_id_yyyymmdd.sql — change the spine id format of an
-- existing database from the Excel date serial to YYYYMMDD:
--
--     2026-09-03 17:45   '46268HB17MB45'  ->  '20260903HB17MB45'
--
-- A new database gets the new format from 01_schema.sql; do not run this there
-- (it is harmless, it reports "nothing to do").
--
--     psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/patches/2026-11-23_spine_id_yyyymmdd.sql
--
-- Stop every writer first (Airflow, the Garden City and Sherburne scrapers),
-- run this, then deploy the new code and the new standalone scrapers. Old code
-- writes old ids, which no longer exist in `timestamps`, so its inserts fail
-- on the foreign key (and on the mw_updates CHECK).
--
-- WHAT IT DOES, in one transaction (any error rolls back everything):
--   1. Replaces spine_id() with the YYYYMMDD version. The generated column
--      timestamps.id and the mw_updates CHECK call it by reference, so they
--      use the new format from here on.
--   2. Builds a map old id -> new id from timestamps (new id = spine_id(ts_central)).
--   3. Drops every foreign key that references timestamps(id) (saving its
--      definition), because they are NO ACTION on update.
--   4. Detaches every monthly partition of the tables RANGE-partitioned on
--      timestamp_id (their bounds are old ids, and a new id would fit none).
--   5. Rewrites timestamp_id in every table that has that column, through the
--      map, and recomputes timestamps.id (UPDATE ... SET ts_central = ts_central).
--   6. Re-attaches each partition with bounds of the same month in the new
--      format (the month is read from the old lower bound, not from the name).
--   7. Re-creates the foreign keys (Postgres validates them).
--   8. Checks: no old-format id is left, row counts per table are unchanged,
--      every mw_updates row passes its CHECK, every partition bound is new.
--
-- Safe to run again: when spine_id() is already the new version and no
-- old-format id or bound is left, it changes nothing.
--
-- Locks: every touched table is locked ACCESS EXCLUSIVE until COMMIT. The
-- UPDATE rewrites every fact row, so the tables grow until autovacuum (or a
-- manual VACUUM ANALYZE, which cannot run inside this transaction) reclaims
-- the dead rows. Check free disk space: about the size of the fact tables.

\set ON_ERROR_STOP on

BEGIN;

-- Fail fast instead of queueing behind a running writer for minutes.
SET LOCAL lock_timeout = '30s';

-- 1. New spine_id(). Same signature, so dependents keep pointing at it.
CREATE OR REPLACE FUNCTION spine_id(ts timestamp) RETURNS text
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT lpad((extract(year  FROM ts)::int * 10000
               + extract(month FROM ts)::int * 100
               + extract(day   FROM ts)::int)::text, 8, '0')
        || 'HB' || lpad(extract(hour   FROM ts)::int::text, 2, '0')
        || 'MB' || lpad(extract(minute FROM ts)::int::text, 2, '0')
$$;

DO $patch$
DECLARE
    -- Old id: 1-7 digit Excel serial; new id: exactly 8 digits.
    c_old_re CONSTANT text := '^[0-9]{1,7}HB[0-9]{2}MB[0-9]{2}$';
    c_new_re CONSTANT text := '^[0-9]{8}HB[0-9]{2}MB[0-9]{2}$';
    v_tables   regclass[];   -- every table (not partition) with timestamp_id
    v_parents  regclass[];   -- the ones partitioned on timestamp_id
    t          regclass;
    r          record;
    v_n        bigint;
    v_old_ids  bigint;
    v_old_bnds bigint;
    v_lo text; v_hi text; v_month date;
BEGIN
    IF spine_id(TIMESTAMP '2026-09-03 17:45') <> '20260903HB17MB45' THEN
        RAISE EXCEPTION 'spine_id() is not the YYYYMMDD version';
    END IF;

    -- Tables to rewrite: every ordinary or partitioned table in this schema
    -- with a timestamp_id column, excluding partitions (handled via parent).
    SELECT array_agg(c.oid::regclass ORDER BY c.relname),
           array_agg(c.oid::regclass ORDER BY c.relname)
               FILTER (WHERE c.relkind = 'p')
      INTO v_tables, v_parents
      FROM pg_class c
      JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = 'timestamp_id'
                         AND NOT a.attisdropped
     WHERE c.relnamespace = current_schema()::regnamespace
       AND c.relkind IN ('r', 'p')
       AND NOT c.relispartition;
    v_parents := coalesce(v_parents, '{}');

    -- Lock everything up front, in a fixed order.
    LOCK TABLE timestamps IN ACCESS EXCLUSIVE MODE;
    FOREACH t IN ARRAY v_tables LOOP
        EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', t);
    END LOOP;

    -- Anything left to do?
    SELECT count(*) INTO v_old_ids FROM timestamps WHERE id ~ c_old_re;
    SELECT count(*) INTO v_old_bnds
      FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhrelid
     WHERE i.inhparent = ANY (v_parents)
       AND pg_get_expr(p.relpartbound, p.oid) !~ '''[0-9]{8}HB';
    FOREACH t IN ARRAY v_tables LOOP
        EXECUTE format('SELECT count(*) FROM %s WHERE timestamp_id ~ %L', t, c_old_re)
           INTO v_n;
        v_old_ids := v_old_ids + v_n;
    END LOOP;
    IF v_old_ids = 0 AND v_old_bnds = 0 THEN
        RAISE NOTICE 'spine ids are already YYYYMMDD: nothing to do';
        RETURN;
    END IF;
    RAISE NOTICE 'rewriting % old-format ids and % partition bounds',
                 v_old_ids, v_old_bnds;

    -- Row counts before, to compare at the end.
    CREATE TEMP TABLE spine_patch_counts (tbl regclass PRIMARY KEY, before bigint,
                                          after bigint) ON COMMIT DROP;
    INSERT INTO spine_patch_counts (tbl, before)
         SELECT 'timestamps'::regclass, count(*) FROM timestamps;
    FOREACH t IN ARRAY v_tables LOOP
        EXECUTE format('INSERT INTO spine_patch_counts (tbl, before)
                        SELECT %L::regclass, count(*) FROM %s', t, t);
    END LOOP;

    -- 2. old id -> new id.
    CREATE TEMP TABLE spine_id_map ON COMMIT DROP AS
        SELECT id AS old_id, spine_id(ts_central) AS new_id
          FROM timestamps
         WHERE id IS DISTINCT FROM spine_id(ts_central);
    ALTER TABLE spine_id_map ADD PRIMARY KEY (old_id);
    ANALYZE spine_id_map;

    -- A fact id that is old-format but has no spine row cannot be mapped (the
    -- FK makes this impossible, but say so clearly if it happens).
    FOREACH t IN ARRAY v_tables LOOP
        EXECUTE format('SELECT count(*) FROM %s f WHERE f.timestamp_id ~ %L
                          AND NOT EXISTS (SELECT 1 FROM spine_id_map m
                                           WHERE m.old_id = f.timestamp_id)',
                       t, c_old_re) INTO v_n;
        IF v_n > 0 THEN
            RAISE EXCEPTION '% has % old-format timestamp_id values with no spine row',
                            t, v_n;
        END IF;
    END LOOP;

    -- 3. Drop the foreign keys to timestamps(id), top-level ones only (the
    -- partitions' copies go with them and come back with them).
    CREATE TEMP TABLE spine_patch_fks ON COMMIT DROP AS
        SELECT conrelid::regclass AS tbl, conname,
               pg_get_constraintdef(oid) AS def
          FROM pg_constraint
         WHERE contype = 'f' AND confrelid = 'timestamps'::regclass
           AND conparentid = 0;
    FOR r IN SELECT * FROM spine_patch_fks LOOP
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', r.tbl, r.conname);
    END LOOP;

    -- 4. Detach the partitions, remembering each one's month.
    CREATE TEMP TABLE spine_patch_parts (parent regclass, part regclass,
                                         month date) ON COMMIT DROP;
    FOR r IN
        SELECT i.inhparent::regclass AS parent, i.inhrelid::regclass AS part,
               pg_get_expr(p.relpartbound, p.oid) AS bound
          FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhrelid
         WHERE i.inhparent = ANY (v_parents)
    LOOP
        v_lo := substring(r.bound FROM 'FROM \(''([^'']*)''\)');
        v_hi := substring(r.bound FROM 'TO \(''([^'']*)''\)');
        IF v_lo IS NULL OR v_hi IS NULL THEN
            RAISE EXCEPTION 'partition % has an unexpected bound: %', r.part, r.bound;
        END IF;
        IF v_lo ~ c_old_re THEN
            v_month := DATE '1899-12-30' + split_part(v_lo, 'HB', 1)::int;
        ELSIF v_lo ~ c_new_re THEN
            v_month := to_date(left(v_lo, 8), 'YYYYMMDD');
        ELSE
            RAISE EXCEPTION 'partition % has an unexpected bound: %', r.part, r.bound;
        END IF;
        -- Only whole calendar months are expected (ensure_month_partition).
        IF v_month <> date_trunc('month', v_month)::date
           OR right(v_lo, 8) <> 'HB00MB00' OR right(v_hi, 8) <> 'HB00MB00' THEN
            RAISE EXCEPTION 'partition % is not one calendar month: %', r.part, r.bound;
        END IF;
        INSERT INTO spine_patch_parts VALUES (r.parent, r.part, v_month);
        EXECUTE format('ALTER TABLE %s DETACH PARTITION %s', r.parent, r.part);
    END LOOP;

    -- 5. Rewrite. The spine first (recomputes the generated id), then each
    -- ordinary table and each detached partition.
    UPDATE timestamps SET ts_central = ts_central
     WHERE id IS DISTINCT FROM spine_id(ts_central);
    FOR t IN
        SELECT x FROM unnest(v_tables) x WHERE x <> ALL (v_parents)
        UNION ALL
        SELECT part FROM spine_patch_parts
    LOOP
        EXECUTE format('UPDATE %s f SET timestamp_id = m.new_id
                          FROM spine_id_map m WHERE f.timestamp_id = m.old_id', t);
        GET DIAGNOSTICS v_n = ROW_COUNT;
        RAISE NOTICE '%: % rows', t, v_n;
    END LOOP;

    -- 6. Re-attach with new bounds (validated by Postgres against the rows).
    FOR r IN SELECT * FROM spine_patch_parts ORDER BY parent::text, month LOOP
        EXECUTE format('ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (%L) TO (%L)',
                       r.parent, r.part, spine_id(r.month::timestamp),
                       spine_id((r.month + interval '1 month')::timestamp));
    END LOOP;

    -- 7. Foreign keys back (validated).
    FOR r IN SELECT * FROM spine_patch_fks LOOP
        EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s', r.tbl, r.conname, r.def);
    END LOOP;

    -- 8. Checks.
    SELECT count(*) INTO v_n FROM timestamps WHERE id !~ c_new_re
                                                OR id <> spine_id(ts_central);
    IF v_n > 0 THEN
        RAISE EXCEPTION 'timestamps: % ids are not the new spine_id(ts_central)', v_n;
    END IF;
    UPDATE spine_patch_counts SET after = (SELECT count(*) FROM timestamps)
     WHERE tbl = 'timestamps'::regclass;
    FOREACH t IN ARRAY v_tables LOOP
        EXECUTE format('SELECT count(*) FROM %s WHERE timestamp_id !~ %L', t, c_new_re)
           INTO v_n;
        IF v_n > 0 THEN
            RAISE EXCEPTION '%: % timestamp_id values are not YYYYMMDD', t, v_n;
        END IF;
        EXECUTE format('UPDATE spine_patch_counts SET after = (SELECT count(*) FROM %s)
                         WHERE tbl = %L::regclass', t, t);
    END LOOP;
    FOR r IN SELECT * FROM spine_patch_counts WHERE after IS DISTINCT FROM before LOOP
        RAISE EXCEPTION '%: row count changed from % to %', r.tbl, r.before, r.after;
    END LOOP;
    IF to_regclass('mw_updates') IS NOT NULL THEN
        SELECT count(*) INTO v_n FROM mw_updates
         WHERE timestamp_id <> spine_id(date_bin('5 minutes', reading_timestamp,
                                                 TIMESTAMP '2000-01-01'));
        IF v_n > 0 THEN
            RAISE EXCEPTION 'mw_updates: % rows fail mw_updates_id_matches_reading', v_n;
        END IF;
    END IF;
    SELECT count(*) INTO v_n
      FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhrelid
     WHERE i.inhparent = ANY (v_parents)
       AND pg_get_expr(p.relpartbound, p.oid) !~ '''[0-9]{8}HB00MB00''\) TO \(''[0-9]{8}HB00MB00''';
    IF v_n > 0 THEN
        RAISE EXCEPTION '% partitions still have old bounds', v_n;
    END IF;
    RAISE NOTICE 'spine ids rewritten to YYYYMMDD';
END
$patch$;

COMMIT;

ANALYZE timestamps;
