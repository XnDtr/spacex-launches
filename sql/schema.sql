-- SpaceX API v4 -> SQLite normalized schema
-- Source: https://github.com/r-spacex/SpaceX-API (docs: https://github.com/r-spacex/SpaceX-API/tree/master/docs)
-- Natural keys from the API (24-char hex ids) are used as primary keys so that
-- re-ingestion can UPSERT idempotently instead of inserting duplicates.

PRAGMA foreign_keys = ON;

-- ---------------------------------------------------------------------------
-- Reference / dimension tables
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rockets (
    rocket_id           TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    type                TEXT,
    active              INTEGER,
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
    description         TEXT
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
    details             TEXT
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
    details             TEXT
);

CREATE TABLE IF NOT EXISTS capsules (
    capsule_id          TEXT PRIMARY KEY,
    serial              TEXT,
    status              TEXT,
    type                TEXT,
    reuse_count         INTEGER,
    water_landings      INTEGER,
    land_landings       INTEGER,
    last_update         TEXT
);

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
    last_update         TEXT
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
    tbd                    INTEGER,
    net                    INTEGER,
    launch_window_sec      INTEGER,
    rocket_id              TEXT REFERENCES rockets(rocket_id),
    launchpad_id           TEXT REFERENCES launchpads(launchpad_id),
    success                INTEGER,
    upcoming               INTEGER,
    details                TEXT,
    fairings_reused        INTEGER,
    fairings_recovery_attempt INTEGER,
    fairings_recovered     INTEGER,
    patch_small            TEXT,
    patch_large            TEXT,
    webcast_url            TEXT,
    article_url            TEXT,
    wikipedia_url           TEXT
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
    gridfins            INTEGER,
    legs                INTEGER,
    reused              INTEGER,
    landing_attempt     INTEGER,
    landing_success     INTEGER,
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
    reused               INTEGER,
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
    lifespan_years       REAL
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
    decayed              INTEGER,
    decay_date           TEXT
);

-- ---------------------------------------------------------------------------
-- Indexes for analytical queries
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_launches_rocket        ON launches(rocket_id);
CREATE INDEX IF NOT EXISTS idx_launches_launchpad      ON launches(launchpad_id);
CREATE INDEX IF NOT EXISTS idx_launches_date_utc       ON launches(date_utc);
CREATE INDEX IF NOT EXISTS idx_launches_success        ON launches(success);

CREATE INDEX IF NOT EXISTS idx_launch_cores_core       ON launch_cores(core_id);
CREATE INDEX IF NOT EXISTS idx_launch_cores_landpad    ON launch_cores(landpad_id);

CREATE INDEX IF NOT EXISTS idx_payloads_launch         ON payloads(launch_id);
CREATE INDEX IF NOT EXISTS idx_payloads_orbit          ON payloads(orbit);

CREATE INDEX IF NOT EXISTS idx_starlink_launch         ON starlink(launch_id);
CREATE INDEX IF NOT EXISTS idx_starlink_launch_date    ON starlink(launch_date);
CREATE INDEX IF NOT EXISTS idx_starlink_decayed        ON starlink(decayed);
