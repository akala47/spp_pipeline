-- 01_schema.sql — SPP data warehouse: types, functions, tables, indexes.
--
-- Run order for a new (empty) database:
--     psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/01_schema.sql
--     psql "$SPP_DSN" -v ON_ERROR_STOP=1 -f schema/02_seed.sql
-- To start again on a dev database, run schema/00_reset_dev.sql first.
-- There is no migration chain: this file is the full schema.
--
-- QUERYING: timestamp_id (the spine id, e.g. '46268HB17MB45') is the access
-- path for every fact table. Apps calculate the id bounds (spp/spine.py, or
-- spine_id() in SQL) and filter
--     WHERE timestamp_id BETWEEN :first AND :last [AND <node/zone/location> = :x]
-- rt_lmp, rt_mcp and mw_updates have monthly RANGE partitions on timestamp_id,
-- so that predicate alone prunes to the months it covers. Each fact table has a
-- (<dimension>, timestamp_id) index that INCLUDEs its measures, so "one node
-- over time" is an index-only scan. interval_central is stored for
-- readability, but it has no index: a filter on it scans.
--
-- All spine id columns use COLLATE "C": byte-wise comparison is the fastest
-- text order and always agrees with the time order of the ids.
--
-- TIME CONVENTION: every timestamp in this database is INTERVAL-BEGINNING,
-- Central wall clock. SPP publishes interval-ENDING times; the loader
-- subtracts one period (5 minutes for RT, 1 hour for DA and hourly load).

BEGIN;

-- Provenance : Published/Forward Filled
CREATE TYPE value_source AS ENUM ('published', 'forward_filled');

-- SPINE_ID — the timestamps key: '<excel date serial>HB<hh>MB<mm>'.
--     2026-09-03 17:45  ->  '46268HB17MB45'
-- The Excel serial is days since 1899-12-30 (Excel's effective epoch). Hour and
-- minute are zero-padded, so ids are fixed-width and sort / BETWEEN-filter in
-- time order as text. IMMUTABLE so it can drive a generated column.
-- Python twin: spp/spine.py — keep the two identical.
CREATE FUNCTION spine_id(ts timestamp) RETURNS text
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT (ts::date - DATE '1899-12-30')::text
        || 'HB' || lpad(extract(hour   FROM ts)::int::text, 2, '0')
        || 'MB' || lpad(extract(minute FROM ts)::int::text, 2, '0')
$$;


-- TIMESTAMPS — 5 minute timestamps index with hourly marker(is_hour_start) for splitting hourly facts
--     ts  -> id : unique btree on ts_central, or compute spine_id(ts) directly
--     id  -> ts : primary key on id
-- id is GENERATED from ts_central, so it can never disagree with it; writers
-- insert (ts_central, ts_gmt) only. The grid CHECK keeps ts_central on whole
-- 5-minute marks, which is what makes the id one-to-one with ts_central.
-- Generated calendar columns let apps filter by date/hour/dow without date math
-- and without a function-based index. STORED so they cost nothing at read time.
CREATE TABLE timestamps (
    id            text COLLATE "C" GENERATED ALWAYS AS (spine_id(ts_central)) STORED PRIMARY KEY,
    ts_central    timestamp   NOT NULL UNIQUE,
    ts_gmt        timestamp   NOT NULL,
    cal_date      date        GENERATED ALWAYS AS (ts_central::date) STORED,
    cal_hour      smallint    GENERATED ALWAYS AS (extract(hour   from ts_central)::smallint) STORED,
    cal_minute    smallint    GENERATED ALWAYS AS (extract(minute from ts_central)::smallint) STORED,
    cal_dow       smallint    GENERATED ALWAYS AS (extract(isodow from ts_central)::smallint) STORED,
    is_hour_start boolean     GENERATED ALWAYS AS (extract(minute from ts_central) = 0) STORED, -- split the index for hourly facts
    CONSTRAINT timestamps_on_5min_grid CHECK (
        date_trunc('minute', ts_central) = ts_central
        AND extract(minute FROM ts_central)::int % 5 = 0
    )
);


CREATE INDEX timestamps_date_idx       ON timestamps (cal_date);
CREATE INDEX timestamps_hour_start_idx ON timestamps (ts_central) WHERE is_hour_start;


-- Fact tables share one layout and one access path:
--   * timestamp_id  text COLLATE "C"  FK to timestamps(id) — the query key
--   * UNIQUE (timestamp_id[, dimension])  — natural key, upsert target, and the
--     index for "everything at an interval / in a window"
--   * (dimension, timestamp_id) INCLUDE (measures)  — "one node or zone over
--     time", answered from the index alone
-- interval_central is kept on every row, unindexed, for readability.


-- RT LMP — real-time locational marginal price, 5-min, by settlement location.
-- RANGE-partitioned monthly on timestamp_id. Ids are fixed-width and
-- time-ordered, so a month is the id range [first day 00:00, next month 00:00),
-- e.g. September 2026 = ['46266HB00MB00', '46296HB00MB00'). Partitions are
-- created just in time by ensure_month_partition() (011).
CREATE TABLE rt_lmp (
    id                  bigint GENERATED ALWAYS AS IDENTITY,
    timestamp_id        text COLLATE "C" NOT NULL REFERENCES timestamps (id) ON DELETE RESTRICT,
    interval_central    timestamp     NOT NULL,
    settlement_location text          NOT NULL,
    pnode               text,
    lmp                 numeric(12,4),
    mlc                 numeric(12,4),
    mcc                 numeric(12,4),
    mec                 numeric(12,4),
    source              value_source  NOT NULL DEFAULT 'published',
    is_forward_filled   boolean       NOT NULL DEFAULT false,
    loaded_at           timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (id, timestamp_id),
    UNIQUE (timestamp_id, settlement_location)
) PARTITION BY RANGE (timestamp_id);

CREATE INDEX rt_lmp_loc_ts_idx ON rt_lmp (settlement_location, timestamp_id)
    INCLUDE (lmp, mlc, mcc, mec);


-- RT MCP — real-time market clearing price for reserve products, 5-min, by zone.
-- Partitioned exactly like rt_lmp.
CREATE TABLE rt_mcp (
    id               bigint GENERATED ALWAYS AS IDENTITY,
    timestamp_id     text COLLATE "C" NOT NULL REFERENCES timestamps (id) ON DELETE RESTRICT,
    interval_central timestamp     NOT NULL,
    reserve_zone     text          NOT NULL,
    regupservice     numeric(12,4),
    regdnservice     numeric(12,4),
    regupmile        numeric(12,4),
    regdnmile        numeric(12,4),
    spin             numeric(12,4),
    supp             numeric(12,4),
    rampup           numeric(12,4),
    rampdn           numeric(12,4),
    uncup            numeric(12,4),
    source           value_source  NOT NULL DEFAULT 'published',
    is_forward_filled boolean      NOT NULL DEFAULT false,
    loaded_at        timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (id, timestamp_id),
    UNIQUE (timestamp_id, reserve_zone)
) PARTITION BY RANGE (timestamp_id);

CREATE INDEX rt_mcp_zone_ts_idx ON rt_mcp (reserve_zone, timestamp_id)
    INCLUDE (regupservice, regdnservice, regupmile, regdnmile,
             spin, supp, rampup, rampdn, uncup);


-- DA LMP — day-ahead LMP, hourly, by settlement location. Not partitioned:
-- 24 rows/day/node stays small for years. References the :00 timestamp_id.
CREATE TABLE da_lmp (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp_id        text COLLATE "C" NOT NULL REFERENCES timestamps (id) ON DELETE RESTRICT,
    interval_central    timestamp     NOT NULL,
    settlement_location text          NOT NULL,
    pnode               text,
    lmp                 numeric(12,4),
    mlc                 numeric(12,4),
    mcc                 numeric(12,4),
    mec                 numeric(12,4),
    source              value_source  NOT NULL DEFAULT 'published',
    is_forward_filled   boolean       NOT NULL DEFAULT false,
    loaded_at           timestamptz   NOT NULL DEFAULT now(),
    UNIQUE (timestamp_id, settlement_location)
);
CREATE INDEX da_lmp_loc_ts_idx ON da_lmp (settlement_location, timestamp_id)
    INCLUDE (lmp, mlc, mcc, mec);


-- DA MCP — day-ahead MCP, hourly, by reserve zone.
CREATE TABLE da_mcp (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp_id       text COLLATE "C" NOT NULL REFERENCES timestamps (id) ON DELETE RESTRICT,
    interval_central   timestamp     NOT NULL,
    reserve_zone       text          NOT NULL,
    regup              numeric(12,4),
    regdn              numeric(12,4),
    spin               numeric(12,4),
    supp               numeric(12,4),
    rampup             numeric(12,4),
    rampdn             numeric(12,4),
    uncup              numeric(12,4),
    source             value_source  NOT NULL DEFAULT 'published',
    is_forward_filled  boolean       NOT NULL DEFAULT false,
    loaded_at          timestamptz   NOT NULL DEFAULT now(),
    UNIQUE (timestamp_id, reserve_zone)
);
CREATE INDEX da_mcp_zone_ts_idx ON da_mcp (reserve_zone, timestamp_id)
    INCLUDE (regup, regdn, spin, supp, rampup, rampdn, uncup);


-- HOURLY LOAD — hourly, one column per control zone. One row per hour.
-- 20 zones: the original 17 plus PRPA, WACM and WAUW, which appeared in the
-- July 2026 files and were being pivoted and then dropped on the floor.
-- No dimension, so UNIQUE (timestamp_id) is the whole access path.
CREATE TABLE hourly_load (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp_id     text COLLATE "C" NOT NULL REFERENCES timestamps (id) ON DELETE RESTRICT,
    interval_central timestamp     NOT NULL,
    csws numeric(12,4), ede  numeric(12,4), grda numeric(12,4), indn numeric(12,4),
    kacy numeric(12,4), kcpl numeric(12,4), les  numeric(12,4), mps  numeric(12,4),
    nppd numeric(12,4), okge numeric(12,4), oppd numeric(12,4), prpa numeric(12,4),
    seci numeric(12,4), sprm numeric(12,4), sps  numeric(12,4), wacm numeric(12,4),
    waue numeric(12,4), wauw numeric(12,4), wfec numeric(12,4), wr   numeric(12,4),
    source           value_source  NOT NULL DEFAULT 'published',
    is_forward_filled boolean      NOT NULL DEFAULT false,
    loaded_at        timestamptz   NOT NULL DEFAULT now(),
    UNIQUE (timestamp_id)
);

-- SCADA LOCATIONS — the meters that write to mw_updates. mw_updates.location
-- must be one of these codes, so a mistyped code in a scraper fails on its
-- first write instead of starting a new series. To add a meter, insert a row
-- here (and in schema/patches for a database that exists).
--   location   code written by the scraper (upper case, A-Z 0-9 _)
--   site       the city / substation that the meter is in
--   meter      the meter within the site; NULL when the site has only one
-- The old single-site scraper wrote Jameson readings as 'GARDEN_CITY'. Those
-- rows are GC_JAMESON here: spp.scada.import_legacy and the 2026-11-06 patch
-- rename them.
CREATE TABLE scada_locations (
    location    text PRIMARY KEY CHECK (location ~ '^[A-Z][A-Z0-9_]*$'),
    site        text NOT NULL,
    meter       text,
    description text
);

INSERT INTO scada_locations (location, site, meter, description) VALUES
    ('BEATRICE',   'Beatrice',    NULL,      'Beatrice city load'),
    ('GC_JAMESON', 'Garden City', 'Jameson', 'Garden City, Jameson meter (old code GARDEN_CITY)'),
    ('GC_LEWIS',   'Garden City', 'Lewis',   'Garden City, Lewis meter'),
    ('GC_TOTAL',   'Garden City', 'Total',   'Garden City total load, as the HMI shows it'),
    ('SHERBURNE_CITY', 'Sherburne', NULL,  'Sherburne city load (Demand now)');

-- MW UPDATES — SCADA site-load readings (Beatrice, Garden City Jameson,
-- Lewis and total, Sherburne), about one per meter per minute. Written by spp/scada (not the SPP
-- feed loader).
--   timestamp_id       spine id of the 5-minute interval the reading falls in,
--                      so ~5 readings share an id; enforced by the CHECK below
--   location           a scada_locations code: 'BEATRICE' | 'GC_JAMESON' |
--                      'GC_LEWIS' | 'GC_TOTAL' | 'SHERBURNE_CITY'
--   reading_timestamp  Central wall-clock time the reading was taken
--   mw_reading         MW, 3 decimals
-- Partitioned monthly on timestamp_id like rt_lmp; the primary key makes a
-- re-sent reading a no-op.
CREATE TABLE mw_updates (
    timestamp_id      text COLLATE "C" NOT NULL REFERENCES timestamps (id) ON DELETE RESTRICT,
    location          text          NOT NULL REFERENCES scada_locations (location)
                                             ON UPDATE CASCADE ON DELETE RESTRICT,
    reading_timestamp timestamp     NOT NULL,
    mw_reading        numeric(10,3) NOT NULL,
    PRIMARY KEY (timestamp_id, location, reading_timestamp),
    CONSTRAINT mw_updates_id_matches_reading CHECK (
        timestamp_id = spine_id(date_bin('5 minutes', reading_timestamp,
                                         TIMESTAMP '2000-01-01'))
    )
) PARTITION BY RANGE (timestamp_id);

CREATE INDEX mw_updates_loc_ts_idx ON mw_updates (location, timestamp_id)
    INCLUDE (reading_timestamp, mw_reading);


-- ENSURE_MONTH_PARTITION(parent, month) — create one monthly partition of a
-- table that is partitioned on timestamp_id, if it does not exist. The loader
-- calls it before each write, so no cron job is necessary. A month is the
-- spine id range [first day 00:00, first day of next month 00:00), e.g.
-- September 2026 = ['46266HB00MB00', '46296HB00MB00'). Name: <parent>_yYYYYmMM.
CREATE FUNCTION ensure_month_partition(p_parent text, p_month date)
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
            v_part, p_parent,
            spine_id(v_start::timestamp), spine_id(v_end::timestamp));
    END IF;
END $$;


-- SEED_TIMESTAMPS(from, to) — add the 5-minute spine rows in [from, to].
-- Idempotent (ON CONFLICT on ts_central). ts_gmt comes from the real
-- America/Chicago rules. The loader replaces ts_gmt with SPP's own GMT value
-- when a feed publishes one.
CREATE FUNCTION seed_timestamps(p_from timestamp, p_to timestamp)
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


-- SEED_PARTITIONS(from_month, to_month) — create the monthly partitions of
-- rt_lmp, rt_mcp and mw_updates for each month in the range.
CREATE FUNCTION seed_partitions(p_from date, p_to date)
RETURNS integer LANGUAGE plpgsql AS $$
DECLARE m date; n integer := 0;
BEGIN
    FOR m IN SELECT generate_series(date_trunc('month', p_from),
                                    date_trunc('month', p_to),
                                    interval '1 month')::date LOOP
        PERFORM ensure_month_partition('rt_lmp', m);
        PERFORM ensure_month_partition('rt_mcp', m);
        PERFORM ensure_month_partition('mw_updates', m);
        n := n + 1;
    END LOOP;
    RETURN n;
END $$;

COMMIT;
