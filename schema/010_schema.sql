-- SPP data warehouse schema
-- Design goals: uniform fact tables, timestamp<->id in both directions O(1),
-- idempotent loads, low read latency for "one dimension over time" app queries,
-- and first-class provenance so forward-filled values are never confused with
-- published ones.

BEGIN;

-- Provenance : Published/Forward Filled
CREATE TYPE value_source AS ENUM ('published', 'forward_filled');

-- TIMESTAMPS — 5 minute timestamps index with hourly marker(is_hour_start) for splitting hourly facts
--     ts  -> id : unique btree on ts_central
--     id  -> ts : primary key on id
-- Generated calendar columns let apps filter by date/hour/dow without date math
-- and without a function-based index. STORED so they cost nothing at read time.
CREATE TABLE timestamps (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts_central    timestamp   NOT NULL UNIQUE,
    ts_gmt        timestamp   NOT NULL,
    cal_date      date        GENERATED ALWAYS AS (ts_central::date) STORED,
    cal_hour      smallint    GENERATED ALWAYS AS (extract(hour   from ts_central)::smallint) STORED,
    cal_minute    smallint    GENERATED ALWAYS AS (extract(minute from ts_central)::smallint) STORED,
    cal_dow       smallint    GENERATED ALWAYS AS (extract(isodow from ts_central)::smallint) STORED,
    is_hour_start boolean     GENERATED ALWAYS AS (extract(minute from ts_central) = 0) STORED -- split the index for hourly facts
);


CREATE INDEX timestamps_date_idx       ON timestamps (cal_date);
CREATE INDEX timestamps_hour_start_idx ON timestamps (ts_central) WHERE is_hour_start;


-- RT LMP — real-time locational marginal price, 5-min, by settlement location.
-- NOTE: the partition key (interval_central) MUST appear in every unique
-- constraint, so the natural key is (timestamp_id, settlement_location,
-- interval_central) and the loader's ON CONFLICT names all three.
CREATE TABLE rt_lmp (
    id                  bigint GENERATED ALWAYS AS IDENTITY,
    timestamp_id        bigint        NOT NULL REFERENCES timestamps (id) ON DELETE RESTRICT,
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
    PRIMARY KEY (id, interval_central),
    UNIQUE (timestamp_id, settlement_location, interval_central)
) PARTITION BY RANGE (interval_central);

CREATE INDEX rt_lmp_loc_time_idx ON rt_lmp (settlement_location, interval_central);
CREATE INDEX rt_lmp_brin         ON rt_lmp USING brin (interval_central);


-- RT MCP — real-time market clearing price for reserve products, 5-min, by zone.
CREATE TABLE rt_mcp (
    id               bigint GENERATED ALWAYS AS IDENTITY,
    timestamp_id     bigint        NOT NULL REFERENCES timestamps (id) ON DELETE RESTRICT,
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
    PRIMARY KEY (id, interval_central),
    UNIQUE (timestamp_id, reserve_zone, interval_central)
) PARTITION BY RANGE (interval_central);

CREATE INDEX rt_mcp_zone_time_idx ON rt_mcp (reserve_zone, interval_central);
CREATE INDEX rt_mcp_brin          ON rt_mcp USING brin (interval_central);


-- DA LMP — day-ahead LMP, hourly, by settlement location. Not partitioned:
-- 24 rows/day/node stays small for years. References the :00 timestamp_id.
CREATE TABLE da_lmp (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp_id        bigint        NOT NULL REFERENCES timestamps (id) ON DELETE RESTRICT,
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
CREATE INDEX da_lmp_loc_time_idx ON da_lmp (settlement_location, interval_central);


-- DA MCP — day-ahead MCP, hourly, by reserve zone.
CREATE TABLE da_mcp (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp_id       bigint        NOT NULL REFERENCES timestamps (id) ON DELETE RESTRICT,
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
CREATE INDEX da_mcp_zone_time_idx ON da_mcp (reserve_zone, interval_central);


-- HOURLY LOAD — hourly, one column per control zone. One row per hour.
-- 20 zones: the original 17 plus PRPA, WACM and WAUW, which appeared in the
-- July 2026 files and were being pivoted and then dropped on the floor.
CREATE TABLE hourly_load (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp_id     bigint        NOT NULL REFERENCES timestamps (id) ON DELETE RESTRICT,
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
CREATE INDEX hourly_load_time_idx ON hourly_load (interval_central);

COMMIT;
