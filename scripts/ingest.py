"""Download SpaceX API v4 data and load it into a normalized SQLite database.

Usage:
    python scripts/ingest.py [--db spacex.db]

Idempotency strategy
---------------------
Every top-level entity (rockets, launchpads, landpads, capsules, cores,
launches, payloads, starlink) is keyed by its natural API id and loaded with
``INSERT ... ON CONFLICT(id) DO UPDATE``, so re-running the script updates
existing rows in place instead of duplicating them.

Child/junction rows (launch_failures, launch_cores, launch_capsules,
payload_customers, payload_nationalities) have no natural single-column key —
a launch's list of failures or cores can change shape between runs. For those,
each parent's existing child rows are deleted and reinserted from the current
API response inside the same transaction, which is equivalent to an upsert at
the collection level and guarantees no duplicate/stale rows survive a re-run.

Row construction
-----------------
Each `_*_row()` mapper returns a dict keyed by column name, and every INSERT
uses named (`:column`) placeholders rather than positional `?` ones. This
means adding, removing, or reordering a column in schema.sql can't silently
misalign with a hand-maintained positional tuple -- a typo in a dict key
raises immediately instead of writing a value into the wrong column.
"""
import argparse
import logging
import pathlib
import sqlite3
import time
from typing import Any, Callable, Dict, List, Optional

import requests

JsonRecord = Dict[str, Any]
Row = Dict[str, Any]
RowMapper = Callable[[JsonRecord], Row]

BASE_URL = "https://api.spacexdata.com/v4"
SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
SCHEMA_PATH = SCRIPT_DIR.parent / "sql" / "schema.sql"

ENDPOINTS: List[str] = [
    "rockets",
    "launchpads",
    "landpads",
    "capsules",
    "cores",
    "launches",
    "payloads",
    "starlink",
]

# Rough floors used only to flag a suspiciously small response (e.g. an error
# page or an empty result) -- not validated against a live pull, so treat as
# a sanity check to revisit once real counts are known.
MIN_EXPECTED_COUNTS: Dict[str, int] = {
    "rockets": 1,
    "launchpads": 1,
    "landpads": 1,
    "capsules": 1,
    "cores": 1,
    "launches": 100,
    "payloads": 100,
    "starlink": 500,
}

logger = logging.getLogger("spacex_ingest")


def configure_logging(log_file: str) -> None:
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)


def fetch(endpoint: str, retries: int = 3, backoff: float = 2.0) -> requests.Response:
    """GET an endpoint with retries; returns the raw Response (caller reads .content/.json())."""
    url = f"{BASE_URL}/{endpoint}"
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("Fetch attempt %d/%d for %s failed: %s", attempt, retries, url, exc)
            if attempt < retries:
                time.sleep(backoff * attempt)
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts: {last_error}")


def validate_response(endpoint: str, data: Any) -> None:
    """Fail loudly on a shape we don't expect; warn on a suspiciously small payload.

    The v4 GET-all endpoints this script uses return a plain JSON array. If
    that ever changes (e.g. an error page, or the paginated {docs: [...]}
    shape returned by the POST /query variant), we want a clear error instead
    of a confusing downstream KeyError.
    """
    if not isinstance(data, list):
        raise RuntimeError(
            f"{endpoint}: expected a JSON array, got {type(data).__name__}. "
            "The API may have returned an error page or a paginated wrapper "
            "instead of the plain GET-all response this script expects."
        )
    expected_min = MIN_EXPECTED_COUNTS.get(endpoint, 0)
    if len(data) < expected_min:
        logger.warning(
            "%s: got only %d record(s), expected at least ~%d. Response may be "
            "incomplete or the API shape may have changed.",
            endpoint, len(data), expected_min,
        )


def safe_map(rows: List[JsonRecord], mapper: RowMapper, endpoint: str) -> List[Row]:
    """Apply mapper to each row, logging and skipping any record that doesn't
    match our assumptions instead of aborting the whole endpoint's load."""
    mapped: List[Row] = []
    skipped = 0
    for r in rows:
        try:
            mapped.append(mapper(r))
        except Exception:
            skipped += 1
            logger.warning(
                "%s: skipping malformed record id=%s",
                endpoint, r.get("id", "<unknown>") if isinstance(r, dict) else "<unknown>",
                exc_info=True,
            )
    if skipped:
        logger.warning("%s: skipped %d malformed record(s) out of %d", endpoint, skipped, len(rows))
    return mapped


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_PATH.read_text())


def load_rockets(conn: sqlite3.Connection, rows: List[JsonRecord]) -> None:
    conn.executemany(
        """
        INSERT INTO rockets (
            rocket_id, name, type, active, stages, boosters, cost_per_launch,
            success_rate_pct, first_flight, country, company, height_m,
            diameter_m, mass_kg, description
        ) VALUES (
            :rocket_id, :name, :type, :active, :stages, :boosters, :cost_per_launch,
            :success_rate_pct, :first_flight, :country, :company, :height_m,
            :diameter_m, :mass_kg, :description
        )
        ON CONFLICT(rocket_id) DO UPDATE SET
            name=excluded.name, type=excluded.type, active=excluded.active,
            stages=excluded.stages, boosters=excluded.boosters,
            cost_per_launch=excluded.cost_per_launch,
            success_rate_pct=excluded.success_rate_pct,
            first_flight=excluded.first_flight, country=excluded.country,
            company=excluded.company, height_m=excluded.height_m,
            diameter_m=excluded.diameter_m, mass_kg=excluded.mass_kg,
            description=excluded.description, last_ingested_at=CURRENT_TIMESTAMP
        """,
        safe_map(rows, _rocket_row, "rockets"),
    )


def _rocket_row(r: JsonRecord) -> Row:
    return {
        "rocket_id": r["id"], "name": r.get("name"), "type": r.get("type"),
        "active": int(bool(r.get("active"))), "stages": r.get("stages"),
        "boosters": r.get("boosters"), "cost_per_launch": r.get("cost_per_launch"),
        "success_rate_pct": r.get("success_rate_pct"), "first_flight": r.get("first_flight"),
        "country": r.get("country"), "company": r.get("company"),
        "height_m": (r.get("height") or {}).get("meters"),
        "diameter_m": (r.get("diameter") or {}).get("meters"),
        "mass_kg": (r.get("mass") or {}).get("kg"), "description": r.get("description"),
    }


def load_launchpads(conn: sqlite3.Connection, rows: List[JsonRecord]) -> None:
    conn.executemany(
        """
        INSERT INTO launchpads (
            launchpad_id, name, full_name, locality, region, latitude,
            longitude, launch_attempts, launch_successes, status, details
        ) VALUES (
            :launchpad_id, :name, :full_name, :locality, :region, :latitude,
            :longitude, :launch_attempts, :launch_successes, :status, :details
        )
        ON CONFLICT(launchpad_id) DO UPDATE SET
            name=excluded.name, full_name=excluded.full_name,
            locality=excluded.locality, region=excluded.region,
            latitude=excluded.latitude, longitude=excluded.longitude,
            launch_attempts=excluded.launch_attempts,
            launch_successes=excluded.launch_successes,
            status=excluded.status, details=excluded.details,
            last_ingested_at=CURRENT_TIMESTAMP
        """,
        safe_map(rows, _launchpad_row, "launchpads"),
    )


def _launchpad_row(r: JsonRecord) -> Row:
    return {
        "launchpad_id": r["id"], "name": r.get("name"), "full_name": r.get("full_name"),
        "locality": r.get("locality"), "region": r.get("region"),
        "latitude": r.get("latitude"), "longitude": r.get("longitude"),
        "launch_attempts": r.get("launch_attempts"), "launch_successes": r.get("launch_successes"),
        "status": r.get("status"), "details": r.get("details"),
    }


def load_landpads(conn: sqlite3.Connection, rows: List[JsonRecord]) -> None:
    conn.executemany(
        """
        INSERT INTO landpads (
            landpad_id, name, full_name, type, locality, region, latitude,
            longitude, landing_attempts, landing_successes, status,
            wikipedia, details
        ) VALUES (
            :landpad_id, :name, :full_name, :type, :locality, :region, :latitude,
            :longitude, :landing_attempts, :landing_successes, :status,
            :wikipedia, :details
        )
        ON CONFLICT(landpad_id) DO UPDATE SET
            name=excluded.name, full_name=excluded.full_name,
            type=excluded.type, locality=excluded.locality,
            region=excluded.region, latitude=excluded.latitude,
            longitude=excluded.longitude,
            landing_attempts=excluded.landing_attempts,
            landing_successes=excluded.landing_successes,
            status=excluded.status, wikipedia=excluded.wikipedia,
            details=excluded.details, last_ingested_at=CURRENT_TIMESTAMP
        """,
        safe_map(rows, _landpad_row, "landpads"),
    )


def _landpad_row(r: JsonRecord) -> Row:
    return {
        "landpad_id": r["id"], "name": r.get("name"), "full_name": r.get("full_name"),
        "type": r.get("type"), "locality": r.get("locality"), "region": r.get("region"),
        "latitude": r.get("latitude"), "longitude": r.get("longitude"),
        "landing_attempts": r.get("landing_attempts"),
        "landing_successes": r.get("landing_successes"), "status": r.get("status"),
        "wikipedia": r.get("wikipedia"), "details": r.get("details"),
    }


def load_capsules(conn: sqlite3.Connection, rows: List[JsonRecord]) -> None:
    conn.executemany(
        """
        INSERT INTO capsules (
            capsule_id, serial, status, type, reuse_count, water_landings,
            land_landings, last_update
        ) VALUES (
            :capsule_id, :serial, :status, :type, :reuse_count, :water_landings,
            :land_landings, :last_update
        )
        ON CONFLICT(capsule_id) DO UPDATE SET
            serial=excluded.serial, status=excluded.status,
            type=excluded.type, reuse_count=excluded.reuse_count,
            water_landings=excluded.water_landings,
            land_landings=excluded.land_landings,
            last_update=excluded.last_update, last_ingested_at=CURRENT_TIMESTAMP
        """,
        safe_map(rows, _capsule_row, "capsules"),
    )


def _capsule_row(r: JsonRecord) -> Row:
    return {
        "capsule_id": r["id"], "serial": r.get("serial"), "status": r.get("status"),
        "type": r.get("type"), "reuse_count": r.get("reuse_count"),
        "water_landings": r.get("water_landings"), "land_landings": r.get("land_landings"),
        "last_update": r.get("last_update"),
    }


def load_cores(conn: sqlite3.Connection, rows: List[JsonRecord]) -> None:
    conn.executemany(
        """
        INSERT INTO cores (
            core_id, serial, block, status, reuse_count, rtls_attempts,
            rtls_landings, asds_attempts, asds_landings, last_update
        ) VALUES (
            :core_id, :serial, :block, :status, :reuse_count, :rtls_attempts,
            :rtls_landings, :asds_attempts, :asds_landings, :last_update
        )
        ON CONFLICT(core_id) DO UPDATE SET
            serial=excluded.serial, block=excluded.block,
            status=excluded.status, reuse_count=excluded.reuse_count,
            rtls_attempts=excluded.rtls_attempts,
            rtls_landings=excluded.rtls_landings,
            asds_attempts=excluded.asds_attempts,
            asds_landings=excluded.asds_landings,
            last_update=excluded.last_update, last_ingested_at=CURRENT_TIMESTAMP
        """,
        safe_map(rows, _core_row, "cores"),
    )


def _core_row(r: JsonRecord) -> Row:
    return {
        "core_id": r["id"], "serial": r.get("serial"), "block": r.get("block"),
        "status": r.get("status"), "reuse_count": r.get("reuse_count"),
        "rtls_attempts": r.get("rtls_attempts"), "rtls_landings": r.get("rtls_landings"),
        "asds_attempts": r.get("asds_attempts"), "asds_landings": r.get("asds_landings"),
        "last_update": r.get("last_update"),
    }


def load_launches(conn: sqlite3.Connection, rows: List[JsonRecord]) -> None:
    conn.executemany(
        """
        INSERT INTO launches (
            launch_id, flight_number, name, date_utc, date_unix,
            date_precision, static_fire_date_utc, tbd, net,
            launch_window_sec, rocket_id, launchpad_id, success, upcoming,
            details, fairings_reused, fairings_recovery_attempt,
            fairings_recovered, patch_small, patch_large, webcast_url,
            article_url, wikipedia_url
        ) VALUES (
            :launch_id, :flight_number, :name, :date_utc, :date_unix,
            :date_precision, :static_fire_date_utc, :tbd, :net,
            :launch_window_sec, :rocket_id, :launchpad_id, :success, :upcoming,
            :details, :fairings_reused, :fairings_recovery_attempt,
            :fairings_recovered, :patch_small, :patch_large, :webcast_url,
            :article_url, :wikipedia_url
        )
        ON CONFLICT(launch_id) DO UPDATE SET
            flight_number=excluded.flight_number, name=excluded.name,
            date_utc=excluded.date_utc, date_unix=excluded.date_unix,
            date_precision=excluded.date_precision,
            static_fire_date_utc=excluded.static_fire_date_utc,
            tbd=excluded.tbd, net=excluded.net,
            launch_window_sec=excluded.launch_window_sec,
            rocket_id=excluded.rocket_id, launchpad_id=excluded.launchpad_id,
            success=excluded.success, upcoming=excluded.upcoming,
            details=excluded.details, fairings_reused=excluded.fairings_reused,
            fairings_recovery_attempt=excluded.fairings_recovery_attempt,
            fairings_recovered=excluded.fairings_recovered,
            patch_small=excluded.patch_small, patch_large=excluded.patch_large,
            webcast_url=excluded.webcast_url, article_url=excluded.article_url,
            wikipedia_url=excluded.wikipedia_url, last_ingested_at=CURRENT_TIMESTAMP
        """,
        safe_map(rows, _launch_row, "launches"),
    )

    # child collections: delete-then-reinsert per launch for idempotency
    failure_rows: List[Row] = []
    core_rows: List[Row] = []
    capsule_rows: List[Row] = []
    for r in rows:
        lid = r.get("id")
        if lid is None:
            continue  # already logged as skipped by safe_map above
        conn.execute("DELETE FROM launch_failures WHERE launch_id = ?", (lid,))
        conn.execute("DELETE FROM launch_cores WHERE launch_id = ?", (lid,))
        conn.execute("DELETE FROM launch_capsules WHERE launch_id = ?", (lid,))

        for f in r.get("failures") or []:
            try:
                failure_rows.append(
                    {"launch_id": lid, "time_sec": f.get("time"),
                     "altitude_km": f.get("altitude"), "reason": f.get("reason")}
                )
            except AttributeError:
                logger.warning("launches: malformed failure entry on launch_id=%s", lid)

        for c in r.get("cores") or []:
            try:
                core_rows.append({
                    "launch_id": lid, "core_id": c.get("core"), "core_flight_num": c.get("flight"),
                    "gridfins": int(bool(c.get("gridfins"))), "legs": int(bool(c.get("legs"))),
                    "reused": int(bool(c.get("reused"))),
                    "landing_attempt": int(bool(c.get("landing_attempt"))),
                    "landing_success": (
                        int(bool(c.get("landing_success")))
                        if c.get("landing_success") is not None else None
                    ),
                    "landing_type": c.get("landing_type"), "landpad_id": c.get("landpad"),
                })
            except AttributeError:
                logger.warning("launches: malformed core entry on launch_id=%s", lid)

        for cap_id in r.get("capsules") or []:
            capsule_rows.append({"launch_id": lid, "capsule_id": cap_id})

    if failure_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO launch_failures (launch_id, time_sec, altitude_km, reason) "
            "VALUES (:launch_id, :time_sec, :altitude_km, :reason)",
            failure_rows,
        )
    if core_rows:
        conn.executemany(
            """
            INSERT OR IGNORE INTO launch_cores (
                launch_id, core_id, core_flight_num, gridfins, legs, reused,
                landing_attempt, landing_success, landing_type, landpad_id
            ) VALUES (
                :launch_id, :core_id, :core_flight_num, :gridfins, :legs, :reused,
                :landing_attempt, :landing_success, :landing_type, :landpad_id
            )
            """,
            core_rows,
        )
    if capsule_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO launch_capsules (launch_id, capsule_id) VALUES (:launch_id, :capsule_id)",
            capsule_rows,
        )


def _launch_row(r: JsonRecord) -> Row:
    fairings = r.get("fairings") or {}
    links = r.get("links") or {}
    patch = links.get("patch") or {}
    return {
        "launch_id": r["id"], "flight_number": r.get("flight_number"), "name": r.get("name"),
        "date_utc": r.get("date_utc"), "date_unix": r.get("date_unix"),
        "date_precision": r.get("date_precision"),
        "static_fire_date_utc": r.get("static_fire_date_utc"),
        "tbd": int(bool(r.get("tbd"))), "net": int(bool(r.get("net"))),
        "launch_window_sec": r.get("window"), "rocket_id": r.get("rocket"),
        "launchpad_id": r.get("launchpad"),
        "success": None if r.get("success") is None else int(bool(r.get("success"))),
        "upcoming": int(bool(r.get("upcoming"))), "details": r.get("details"),
        "fairings_reused": (
            int(bool(fairings.get("reused"))) if fairings.get("reused") is not None else None
        ),
        "fairings_recovery_attempt": (
            int(bool(fairings.get("recovery_attempt")))
            if fairings.get("recovery_attempt") is not None else None
        ),
        "fairings_recovered": (
            int(bool(fairings.get("recovered"))) if fairings.get("recovered") is not None else None
        ),
        "patch_small": patch.get("small"), "patch_large": patch.get("large"),
        "webcast_url": links.get("webcast"), "article_url": links.get("article"),
        "wikipedia_url": links.get("wikipedia"),
    }


def load_payloads(conn: sqlite3.Connection, rows: List[JsonRecord]) -> None:
    conn.executemany(
        """
        INSERT INTO payloads (
            payload_id, name, type, launch_id, reused, mass_kg, orbit,
            reference_system, regime, longitude, semi_major_axis_km,
            eccentricity, periapsis_km, apoapsis_km, inclination_deg,
            period_min, lifespan_years
        ) VALUES (
            :payload_id, :name, :type, :launch_id, :reused, :mass_kg, :orbit,
            :reference_system, :regime, :longitude, :semi_major_axis_km,
            :eccentricity, :periapsis_km, :apoapsis_km, :inclination_deg,
            :period_min, :lifespan_years
        )
        ON CONFLICT(payload_id) DO UPDATE SET
            name=excluded.name, type=excluded.type, launch_id=excluded.launch_id,
            reused=excluded.reused, mass_kg=excluded.mass_kg,
            orbit=excluded.orbit, reference_system=excluded.reference_system,
            regime=excluded.regime, longitude=excluded.longitude,
            semi_major_axis_km=excluded.semi_major_axis_km,
            eccentricity=excluded.eccentricity, periapsis_km=excluded.periapsis_km,
            apoapsis_km=excluded.apoapsis_km, inclination_deg=excluded.inclination_deg,
            period_min=excluded.period_min, lifespan_years=excluded.lifespan_years,
            last_ingested_at=CURRENT_TIMESTAMP
        """,
        safe_map(rows, _payload_row, "payloads"),
    )

    customer_rows: List[Row] = []
    nationality_rows: List[Row] = []
    for r in rows:
        pid = r.get("id")
        if pid is None:
            continue  # already logged as skipped by safe_map above
        conn.execute("DELETE FROM payload_customers WHERE payload_id = ?", (pid,))
        conn.execute("DELETE FROM payload_nationalities WHERE payload_id = ?", (pid,))
        for c in r.get("customers") or []:
            customer_rows.append({"payload_id": pid, "customer": c})
        for n in r.get("nationalities") or []:
            nationality_rows.append({"payload_id": pid, "nationality": n})

    if customer_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO payload_customers (payload_id, customer) VALUES (:payload_id, :customer)",
            customer_rows,
        )
    if nationality_rows:
        conn.executemany(
            "INSERT OR IGNORE INTO payload_nationalities (payload_id, nationality) "
            "VALUES (:payload_id, :nationality)",
            nationality_rows,
        )


def _payload_row(r: JsonRecord) -> Row:
    orbit_params = r.get("orbit_params") or {}
    return {
        "payload_id": r["id"], "name": r.get("name"), "type": r.get("type"),
        "launch_id": r.get("launch"), "reused": int(bool(r.get("reused"))),
        "mass_kg": r.get("mass_kg"), "orbit": r.get("orbit"),
        "reference_system": orbit_params.get("reference_system"),
        "regime": orbit_params.get("regime"), "longitude": orbit_params.get("longitude"),
        "semi_major_axis_km": orbit_params.get("semi_major_axis_km"),
        "eccentricity": orbit_params.get("eccentricity"),
        "periapsis_km": orbit_params.get("periapsis_km"),
        "apoapsis_km": orbit_params.get("apoapsis_km"),
        "inclination_deg": orbit_params.get("inclination_deg"),
        "period_min": orbit_params.get("period_min"),
        "lifespan_years": orbit_params.get("lifespan_years"),
    }


def load_starlink(conn: sqlite3.Connection, rows: List[JsonRecord]) -> None:
    conn.executemany(
        """
        INSERT INTO starlink (
            starlink_id, version, launch_id, longitude, latitude, height_km,
            velocity_kms, object_name, object_id, norad_cat_id, launch_date,
            country_code, epoch, mean_motion, eccentricity, inclination_deg,
            period_min, semimajor_axis_km, decayed, decay_date
        ) VALUES (
            :starlink_id, :version, :launch_id, :longitude, :latitude, :height_km,
            :velocity_kms, :object_name, :object_id, :norad_cat_id, :launch_date,
            :country_code, :epoch, :mean_motion, :eccentricity, :inclination_deg,
            :period_min, :semimajor_axis_km, :decayed, :decay_date
        )
        ON CONFLICT(starlink_id) DO UPDATE SET
            version=excluded.version, launch_id=excluded.launch_id,
            longitude=excluded.longitude, latitude=excluded.latitude,
            height_km=excluded.height_km, velocity_kms=excluded.velocity_kms,
            object_name=excluded.object_name, object_id=excluded.object_id,
            norad_cat_id=excluded.norad_cat_id, launch_date=excluded.launch_date,
            country_code=excluded.country_code, epoch=excluded.epoch,
            mean_motion=excluded.mean_motion, eccentricity=excluded.eccentricity,
            inclination_deg=excluded.inclination_deg, period_min=excluded.period_min,
            semimajor_axis_km=excluded.semimajor_axis_km, decayed=excluded.decayed,
            decay_date=excluded.decay_date, last_ingested_at=CURRENT_TIMESTAMP
        """,
        safe_map(rows, _starlink_row, "starlink"),
    )


def _starlink_row(r: JsonRecord) -> Row:
    st = r.get("spaceTrack") or {}
    decay_date = st.get("DECAY_DATE")
    return {
        "starlink_id": r["id"], "version": r.get("version"), "launch_id": r.get("launch"),
        "longitude": r.get("longitude"), "latitude": r.get("latitude"),
        "height_km": r.get("height_km"), "velocity_kms": r.get("velocity_kms"),
        "object_name": st.get("OBJECT_NAME"), "object_id": st.get("OBJECT_ID"),
        "norad_cat_id": st.get("NORAD_CAT_ID"), "launch_date": st.get("LAUNCH_DATE"),
        "country_code": st.get("COUNTRY_CODE"), "epoch": st.get("EPOCH"),
        "mean_motion": st.get("MEAN_MOTION"), "eccentricity": st.get("ECCENTRICITY"),
        "inclination_deg": st.get("INCLINATION"), "period_min": st.get("PERIOD"),
        "semimajor_axis_km": st.get("SEMIMAJOR_AXIS"),
        "decayed": int(bool(decay_date)), "decay_date": decay_date,
    }


LOADERS: Dict[str, Callable[[sqlite3.Connection, List[JsonRecord]], None]] = {
    "rockets": load_rockets,
    "launchpads": load_launchpads,
    "landpads": load_landpads,
    "capsules": load_capsules,
    "cores": load_cores,
    "launches": load_launches,
    "payloads": load_payloads,
    "starlink": load_starlink,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="spacex.db", help="path to SQLite database file")
    parser.add_argument(
        "--limit", type=int, default=None,
        help="only load the first N records per endpoint, for a quick smoke test. "
             "Note: the full response is still downloaded and validated -- this "
             "only truncates what gets loaded into SQLite, it does not reduce "
             "network traffic.",
    )
    parser.add_argument("--log-file", default="ingest.log", help="path to write a persistent run log")
    args = parser.parse_args()

    configure_logging(args.log_file)

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys = ON")
    init_schema(conn)

    total_raw_bytes = 0
    with conn:
        for endpoint in ENDPOINTS:
            logger.info("Fetching %s ...", endpoint)
            resp = fetch(endpoint)
            total_raw_bytes += len(resp.content)
            data = resp.json()
            validate_response(endpoint, data)
            if args.limit is not None:
                data = data[: args.limit]
            LOADERS[endpoint](conn, data)
            logger.info("  loaded %d %s record(s)", len(data), endpoint)

    logger.info("Total raw JSON downloaded: %.2f MB", total_raw_bytes / 1_000_000)
    conn.close()


if __name__ == "__main__":
    main()
