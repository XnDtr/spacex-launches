-- SpaceX API v4 -> SQLite normalized schema
-- Source: https://github.com/r-spacex/SpaceX-API (docs: https://github.com/r-spacex/SpaceX-API/tree/master/docs)
-- Natural keys from the API (24-char hex ids) are used as primary keys so that
-- re-ingestion can UPSERT idempotently instead of inserting duplicates.
--
-- Provenance note: the live API is dead (see README "Known data quality").
-- rockets/launchpads/landpads/capsules/cores/launches/payloads keep the
-- original API's id scheme (real ids, sourced via Wayback Machine snapshot
-- or hand-seeded); `starlink` is now sourced from Celestrak instead, keyed
-- on NORAD catalog id rather than the original API's string id.
--
-- Scope note: the `ships` and `crew` endpoints are deliberately NOT modeled.
-- They're low-value for the analysis questions this project answers (crew
-- data barely exists pre-Commercial-Crew, and ship recovery data duplicates
-- signal already captured via launch_cores/fairings), and including them
-- would mean two more dimension tables and junctions for endpoints nothing
-- downstream uses. Revisit if a future question needs them.

PRAGMA foreign_keys = ON;

-- Each top-level table commits independently (see ingest.py main()), so a run
-- that fails partway leaves some tables fresh and others stale from a prior
-- run -- indistinguishable from a fully consistent snapshot by row counts
-- alone. This single-row table is only written after every table succeeds,
-- so comparing it against each table's last_ingested_at reveals a partial run.
CREATE TABLE IF NOT EXISTS ingest_runs (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    completed_at    TEXT NOT NULL
);

-- ---------------------------------------------------------------------------
-- Reference / dimension tables
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rockets (
    rocket_id           TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    type                TEXT,
    active              INTEGER CHECK (active IN (0, 1)),
    stages              INTEGER,
    boosters            INTEGER,
    cost_per_launch     INTEGER,
    success_rate_pct    INTEGER,
    first_flight        TEXT,
    country             TEXT,
    company             TEXT,
    height_m            REAL,
    diameter_m          REAL,
    mass_kg             REAL,
    description         TEXT,
    last_ingested_at    TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

CREATE TABLE IF NOT EXISTS launchpads (
    launchpad_id        TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    full_name           TEXT,
    locality            TEXT,
    region              TEXT,
    latitude            REAL,
    longitude           REAL,
    launch_attempts     INTEGER,
    launch_successes    INTEGER,
    status              TEXT,
    details             TEXT,
    last_ingested_at    TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

CREATE TABLE IF NOT EXISTS landpads (
    landpad_id          TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    full_name           TEXT,
    type                TEXT,
    locality            TEXT,
    region              TEXT,
    latitude            REAL,
    longitude           REAL,
    landing_attempts    INTEGER,
    landing_successes   INTEGER,
    status              TEXT,
    wikipedia           TEXT,
    details             TEXT,
    last_ingested_at    TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

CREATE TABLE IF NOT EXISTS capsules (
    capsule_id          TEXT PRIMARY KEY,
    serial              TEXT,
    status              TEXT,
    type                TEXT,
    reuse_count         INTEGER,
    water_landings      INTEGER,
    land_landings       INTEGER,
    last_update         TEXT,
    last_ingested_at    TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

-- No live/cached source for core metadata exists (see README) -- rows here
-- are id-only stubs derived from launches, so every column but core_id is
-- expected to be null.
CREATE TABLE IF NOT EXISTS cores (
    core_id             TEXT PRIMARY KEY,
    serial              TEXT,
    block               INTEGER,
    status              TEXT,
    reuse_count         INTEGER,
    rtls_attempts       INTEGER,
    rtls_landings       INTEGER,
    asds_attempts       INTEGER,
    asds_landings       INTEGER,
    last_update         TEXT,
    last_ingested_at    TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

-- ---------------------------------------------------------------------------
-- Launches (fact table) and its child collections
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS launches (
    launch_id            TEXT PRIMARY KEY,
    flight_number         INTEGER,
    name                  TEXT NOT NULL,
    date_utc              TEXT,
    date_unix             INTEGER,
    date_precision        TEXT,
    static_fire_date_utc  TEXT,
    tbd                    INTEGER CHECK (tbd IN (0, 1)),
    net                    INTEGER CHECK (net IN (0, 1)),
    launch_window_sec      INTEGER,
    rocket_id              TEXT REFERENCES rockets(rocket_id),
    launchpad_id           TEXT REFERENCES launchpads(launchpad_id),
    success                INTEGER CHECK (success IN (0, 1)),
    upcoming               INTEGER CHECK (upcoming IN (0, 1)),
    details                TEXT,
    fairings_reused        INTEGER CHECK (fairings_reused IN (0, 1)),
    fairings_recovery_attempt INTEGER CHECK (fairings_recovery_attempt IN (0, 1)),
    fairings_recovered     INTEGER CHECK (fairings_recovered IN (0, 1)),
    patch_small            TEXT,
    patch_large            TEXT,
    webcast_url            TEXT,
    article_url            TEXT,
    wikipedia_url           TEXT,
    last_ingested_at        TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

CREATE TABLE IF NOT EXISTS launch_failures (
    launch_id           TEXT NOT NULL REFERENCES launches(launch_id),
    time_sec            INTEGER,
    altitude_km         REAL,
    reason              TEXT,
    PRIMARY KEY (launch_id, time_sec, reason)
);

-- launch <-> core, one row per booster used on a launch (Falcon Heavy has 3)
CREATE TABLE IF NOT EXISTS launch_cores (
    launch_id           TEXT NOT NULL REFERENCES launches(launch_id),
    core_id             TEXT REFERENCES cores(core_id),
    core_flight_num     INTEGER,
    gridfins            INTEGER CHECK (gridfins IN (0, 1)),
    legs                INTEGER CHECK (legs IN (0, 1)),
    reused              INTEGER CHECK (reused IN (0, 1)),
    landing_attempt     INTEGER CHECK (landing_attempt IN (0, 1)),
    landing_success     INTEGER CHECK (landing_success IN (0, 1)),
    landing_type        TEXT,
    landpad_id          TEXT REFERENCES landpads(landpad_id),
    PRIMARY KEY (launch_id, core_id)
);

-- launch <-> capsule, many-to-many
CREATE TABLE IF NOT EXISTS launch_capsules (
    launch_id           TEXT NOT NULL REFERENCES launches(launch_id),
    capsule_id          TEXT NOT NULL REFERENCES capsules(capsule_id),
    PRIMARY KEY (launch_id, capsule_id)
);

-- ---------------------------------------------------------------------------
-- Payloads
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS payloads (
    payload_id           TEXT PRIMARY KEY,
    name                 TEXT,
    type                 TEXT,
    launch_id            TEXT REFERENCES launches(launch_id),
    reused               INTEGER CHECK (reused IN (0, 1)),
    mass_kg              REAL,
    orbit                TEXT,
    reference_system     TEXT,
    regime               TEXT,
    longitude            REAL,
    semi_major_axis_km   REAL,
    eccentricity         REAL,
    periapsis_km         REAL,
    apoapsis_km          REAL,
    inclination_deg      REAL,
    period_min           REAL,
    lifespan_years       REAL,
    last_ingested_at     TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

CREATE TABLE IF NOT EXISTS payload_customers (
    payload_id           TEXT NOT NULL REFERENCES payloads(payload_id),
    customer             TEXT NOT NULL,
    PRIMARY KEY (payload_id, customer)
);

CREATE TABLE IF NOT EXISTS payload_nationalities (
    payload_id           TEXT NOT NULL REFERENCES payloads(payload_id),
    nationality          TEXT NOT NULL,
    PRIMARY KEY (payload_id, nationality)
);

-- ---------------------------------------------------------------------------
-- Starlink satellites (largest table by row count; drives raw-size requirement)
-- Sourced live from Celestrak (GP elements + SATCAT), not the dead SpaceX
-- API -- launch_id, longitude/latitude, and velocity_kms are unavailable
-- from that source (no SpaceX-internal launch linkage, no SGP4 propagation
-- for live position) and are expected to be null. See README.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS starlink (
    starlink_id          TEXT PRIMARY KEY,
    version              TEXT,
    launch_id            TEXT REFERENCES launches(launch_id),
    longitude            REAL,
    latitude             REAL,
    height_km            REAL,
    velocity_kms         REAL,
    object_name          TEXT,
    object_id            TEXT,
    norad_cat_id         INTEGER,
    launch_date          TEXT,
    country_code         TEXT,
    epoch                TEXT,
    mean_motion          REAL,
    eccentricity         REAL,
    inclination_deg      REAL,
    period_min           REAL,
    semimajor_axis_km    REAL,
    decayed              INTEGER CHECK (decayed IN (0, 1)),
    decay_date           TEXT,
    last_ingested_at     TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

-- ---------------------------------------------------------------------------
-- Starlink unit-economics reference data (Q6) -- hand-curated, not fetched.
-- Bands are defined by orbital inclination, which bounds the max latitude a
-- satellite's ground track reaches, because that's directly computable from
-- starlink.inclination_deg already above -- not by country/continent, which
-- would need a ground-coverage model this project doesn't have. Real prices
-- vary by country, not latitude, so each band's monthly_price_usd is a
-- blended figure across that band's representative markets; throughput is
-- per publicly reported figures for the satellite generation that mostly
-- flies at that inclination. Full sourcing/citations: see README "Starlink
-- unit economics". This is a back-of-envelope model, not verified financials.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS starlink_pricing_bands (
    band_name              TEXT PRIMARY KEY,
    min_inclination_deg    REAL NOT NULL,
    max_inclination_deg    REAL NOT NULL,
    monthly_price_usd      REAL NOT NULL,
    sat_throughput_gbps    REAL NOT NULL,
    notes                  TEXT,
    last_ingested_at       TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

-- ---------------------------------------------------------------------------
-- Indexes for analytical queries
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_launches_rocket        ON launches(rocket_id);
CREATE INDEX IF NOT EXISTS idx_launches_launchpad      ON launches(launchpad_id);
CREATE INDEX IF NOT EXISTS idx_launches_date_utc       ON launches(date_utc);
CREATE INDEX IF NOT EXISTS idx_launches_success        ON launches(success);
-- Q1/Q3 GROUP BY substr(date_utc,1,4), not the raw column -- a plain index on
-- date_utc can't accelerate that (verified via EXPLAIN QUERY PLAN: without
-- this, both queries fall back to a full table scan for the GROUP BY).
CREATE INDEX IF NOT EXISTS idx_launches_year          ON launches(substr(date_utc, 1, 4));

CREATE INDEX IF NOT EXISTS idx_launch_cores_core       ON launch_cores(core_id);
-- Q2 GROUPs BY this column directly.
CREATE INDEX IF NOT EXISTS idx_launch_cores_flight_num ON launch_cores(core_flight_num);
CREATE INDEX IF NOT EXISTS idx_launch_cores_landpad    ON launch_cores(landpad_id);

CREATE INDEX IF NOT EXISTS idx_payloads_launch         ON payloads(launch_id);
CREATE INDEX IF NOT EXISTS idx_payloads_orbit          ON payloads(orbit);

CREATE INDEX IF NOT EXISTS idx_starlink_launch         ON starlink(launch_id);
CREATE INDEX IF NOT EXISTS idx_starlink_launch_date    ON starlink(launch_date);
CREATE INDEX IF NOT EXISTS idx_starlink_decayed        ON starlink(decayed);
