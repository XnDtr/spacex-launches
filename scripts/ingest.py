"""Download SpaceX-related data and load it into a normalized SQLite database.

Usage:
    python scripts/ingest.py [--db spacex.db]

Source strategy (see README "Known data quality" for the full story)
----------------------------------------------------------------------
The live SpaceX API (api.spacexdata.com) has been unreachable (Cloudflare 525
-- origin down) since its source repo was archived; the mirror is dead too.
Per table:

- rockets, capsules, launches, payloads: fetched from a Wayback Machine
  snapshot of the real API response (same JSON shape, so every `_*_row()`
  mapper below is unchanged).
- launchpads, landpads: hand-seeded (`LAUNCHPADS_SEED`/`LANDPADS_SEED`). No
  live or archived source exists for these two endpoints; SpaceX has only
  ever used this fixed, tiny set of physical sites, and the id -> site
  mapping was cross-validated against real launch history in the Wayback
  `launches` snapshot (e.g. the earliest launch on one launchpad id is
  FalconSat/2006 -> Kwajalein; on another it's CRS-10/Feb-2017, the
  well-documented first flight from KSC LC-39A after the Sep-2016 pad
  explosion took SLC-40 offline).
- cores: no live or archived source has core metadata either. `id`-only stub
  rows are derived from the core ids referenced inside the ingested
  `launches` data, just to satisfy the `launch_cores` foreign key -- see
  `derive_core_stubs()`.
- starlink: replaced with live data from Celestrak (GP orbital elements +
  the public SATCAT), which is also what drives this project's >=10MB raw
  size target now that the original spaceTrack-shaped source is gone. See
  `_celestrak_to_starlink_records()` -- it reshapes Celestrak's fields into
  the same spaceTrack-nested shape `_starlink_row()` already expects, so
  that mapper (and its tests) are unchanged too.

Idempotency strategy
---------------------
Every top-level entity (rockets, launchpads, landpads, capsules, cores,
launches, payloads, starlink) is keyed by its natural id and loaded with
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
import csv
import io
import logging
import math
import pathlib
import sqlite3
import time
from collections.abc import Callable
from typing import Any

import requests

JsonRecord = dict[str, Any]
Row = dict[str, Any]
RowMapper = Callable[[JsonRecord], Row]

BASE_URL = "https://api.spacexdata.com/v4"
SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
SCHEMA_PATH = SCRIPT_DIR.parent / "sql" / "schema.sql"

# Wayback Machine snapshot timestamps of the real (now-dead) API responses --
# see module docstring. Format matches web.archive.org's /web/<timestamp>/ path.
WAYBACK_SNAPSHOTS: dict[str, str] = {
    "rockets": "20260206123459",
    "capsules": "20260206123459",
    "launches": "20260206123625",
    "payloads": "20240802054003",
}

CELESTRAK_GP_URL = "https://celestrak.org/NORAD/elements/gp.php?GROUP=starlink&FORMAT=json"
CELESTRAK_SATCAT_URL = "https://celestrak.org/pub/satcat.csv"

EARTH_MU_KM3_S2 = 398600.4418  # Earth's standard gravitational parameter
EARTH_MEAN_RADIUS_KM = 6371.0

# Real, hand-verified SpaceX launch/landing sites -- see module docstring.
LAUNCHPADS_SEED: list[JsonRecord] = [
    {
        "id": "5e9e4502f5090995de566f86", "name": "Kwajalein Atoll", "full_name": "Omelek Island",
        "locality": "Kwajalein Atoll", "region": "Marshall Islands", "latitude": 9.047721,
        "longitude": 167.743129, "launch_attempts": 5, "launch_successes": 2, "status": "retired",
        "details": "SpaceX's first launch site, used only for Falcon 1 (2006-2009).",
    },
    {
        "id": "5e9e4501f509094ba4566f84", "name": "CCSFS SLC-40",
        "full_name": "Cape Canaveral Space Force Station Space Launch Complex 40",
        "locality": "Cape Canaveral", "region": "Florida", "latitude": 28.561857,
        "longitude": -80.577366, "launch_attempts": None, "launch_successes": None, "status": "active",
        "details": "Primary East Coast Falcon 9 pad since the Falcon 9 debut in June 2010.",
    },
    {
        "id": "5e9e4502f509092b78566f87", "name": "VAFB SLC-4E",
        "full_name": "Vandenberg Space Force Base Space Launch Complex 4E",
        "locality": "Vandenberg Space Force Base", "region": "California", "latitude": 34.632093,
        "longitude": -120.610829, "launch_attempts": 15, "launch_successes": 15, "status": "active",
        "details": "West Coast Falcon 9 pad. Name/location/counts confirmed via the archived "
                   "API repo's own doc examples (docs/launchpads/v4/all.md).",
    },
    {
        "id": "5e9e4502f509094188566f88", "name": "KSC LC-39A",
        "full_name": "Kennedy Space Center Launch Complex 39A",
        "locality": "Kennedy Space Center", "region": "Florida", "latitude": 28.608389,
        "longitude": -80.604333, "launch_attempts": None, "launch_successes": None, "status": "active",
        "details": "Former Apollo/Shuttle pad; SpaceX's Falcon Heavy and Crew Dragon pad since 2017.",
    },
]

LANDPADS_SEED: list[JsonRecord] = [
    {
        "id": "5e9e3032383ecb267a34e7c7", "name": "LZ-1", "full_name": "Landing Zone 1",
        "type": "RTLS", "locality": "Cape Canaveral", "region": "Florida", "latitude": 28.485833,
        "longitude": -80.544444, "landing_attempts": None, "landing_successes": None, "status": "active",
        "wikipedia": "https://en.wikipedia.org/wiki/Landing_Zones_1_and_2",
        "details": "Site of the first-ever orbital-class rocket landing (Orbcomm-2, Dec 2015).",
    },
    {
        "id": "5e9e3032383ecb554034e7c9", "name": "LZ-4", "full_name": "Landing Zone 4",
        "type": "RTLS", "locality": "Vandenberg Space Force Base", "region": "California",
        "latitude": 34.6321, "longitude": -120.6110, "landing_attempts": None,
        "landing_successes": None, "status": "active",
        "wikipedia": "https://en.wikipedia.org/wiki/Autonomous_spaceport_drone_ship",
        "details": "West Coast RTLS pad; first landing was SAOCOM 1A, Oct 2018.",
    },
    {
        "id": "5e9e3032383ecb90a834e7c8", "name": "LZ-2", "full_name": "Landing Zone 2",
        "type": "RTLS", "locality": "Cape Canaveral", "region": "Florida", "latitude": 28.485833,
        "longitude": -80.544444, "landing_attempts": 3, "landing_successes": 3, "status": "active",
        "wikipedia": "https://en.wikipedia.org/wiki/Landing_Zones_1_and_2",
        "details": "Falcon Heavy side-booster RTLS pad. Name/counts confirmed via the archived "
                   "API repo's own doc examples (docs/landpads/v4/all.md).",
    },
    {
        "id": "5e9e3032383ecb761634e7cb", "name": "JRTI (2015)",
        "full_name": "Just Read the Instructions (original hull)",
        "type": "ASDS", "locality": "Atlantic Ocean", "region": None, "latitude": None,
        "longitude": None, "landing_attempts": None, "landing_successes": None, "status": "retired",
        "wikipedia": "https://en.wikipedia.org/wiki/Autonomous_spaceport_drone_ship",
        "details": "First-generation droneship; both attempts (CRS-5, CRS-6, early 2015) tipped "
                   "over on landing. Retired later in 2015.",
    },
    {
        "id": "5e9e3032383ecb6bb234e7ca", "name": "OCISLY", "full_name": "Of Course I Still Love You",
        "type": "ASDS", "locality": "Atlantic Ocean", "region": None, "latitude": None,
        "longitude": None, "landing_attempts": None, "landing_successes": None, "status": "active",
        "wikipedia": "https://en.wikipedia.org/wiki/Autonomous_spaceport_drone_ship",
        "details": "SpaceX's primary East Coast droneship, in service since 2015.",
    },
    {
        "id": "5e9e3033383ecbb9e534e7cc", "name": "JRTI",
        "full_name": "Just Read the Instructions (2016 hull)",
        "type": "ASDS", "locality": "Pacific / Atlantic Ocean", "region": None, "latitude": None,
        "longitude": None, "landing_attempts": None, "landing_successes": None, "status": "active",
        "wikipedia": "https://en.wikipedia.org/wiki/Autonomous_spaceport_drone_ship",
        "details": "Replacement droneship carrying the JRTI name since 2016; relocated between "
                   "coasts over its service life.",
    },
    {
        "id": "5e9e3033383ecb075134e7cd", "name": "ASOG", "full_name": "A Shortfall of Gravitas",
        "type": "ASDS", "locality": "Atlantic Ocean", "region": None, "latitude": None,
        "longitude": None, "landing_attempts": None, "landing_successes": None, "status": "active",
        "wikipedia": "https://en.wikipedia.org/wiki/Autonomous_spaceport_drone_ship",
        "details": "SpaceX's newest droneship, in service since 2021.",
    },
]

# Rough floors used only to flag a suspiciously small response (e.g. an error
# page or an empty result) -- not validated against a live pull, so treat as
# a sanity check to revisit once real counts are known.
MIN_EXPECTED_COUNTS: dict[str, int] = {
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


def _get_with_retries(
    url: str, retries: int = 3, backoff: float = 2.0, timeout: int = 30
) -> requests.Response:
    """GET a URL with retries; returns the raw Response (caller reads .content/.json()/.text)."""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_error = exc
            logger.warning("Fetch attempt %d/%d for %s failed: %s", attempt, retries, url, exc)
            if attempt < retries:
                time.sleep(backoff * attempt)
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts: {last_error}")


def fetch_wayback(endpoint: str) -> tuple[list[JsonRecord], int]:
    """Fetch a Wayback Machine snapshot of the real (now-dead) API response.

    Wayback serves the original bytes verbatim, so this is the same JSON
    shape the live endpoint used to return -- every `_*_row()` mapper below
    needs no changes to consume it.
    """
    timestamp = WAYBACK_SNAPSHOTS[endpoint]
    url = f"https://web.archive.org/web/{timestamp}if_/{BASE_URL}/{endpoint}"
    resp = _get_with_retries(url)
    return resp.json(), len(resp.content)


def fetch_celestrak_gp_starlink() -> tuple[list[JsonRecord], int]:
    """Live current orbital elements for active Starlink satellites."""
    resp = _get_with_retries(CELESTRAK_GP_URL, timeout=60)
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Celestrak GP starlink: expected a JSON array, got {type(data).__name__}")
    return data, len(resp.content)


SATCAT_EXPECTED_COLUMNS = {"OBJECT_NAME", "OBJECT_ID", "LAUNCH_DATE", "DECAY_DATE", "OWNER"}


def _parse_satcat_csv(text: str) -> list[dict[str, str]]:
    """Parse SATCAT CSV text down to Starlink rows.

    Unlike the GP feed (JSON, checked for list-shape in
    `fetch_celestrak_gp_starlink`), a non-CSV response here (e.g. an HTML
    error page served with a 200) wouldn't raise -- DictReader would just
    silently yield zero matching rows, masking the real cause. Fail loudly
    on a missing/wrong header instead.
    """
    reader = csv.DictReader(io.StringIO(text))
    fieldnames = set(reader.fieldnames or [])
    if not SATCAT_EXPECTED_COLUMNS.issubset(fieldnames):
        raise RuntimeError(
            f"Celestrak SATCAT: expected columns {sorted(SATCAT_EXPECTED_COLUMNS)}, got "
            f"{sorted(fieldnames)}. The response may be an error page rather than real CSV."
        )
    return [row for row in reader if row.get("OBJECT_NAME", "").upper().startswith("STARLINK")]


def fetch_celestrak_satcat_starlink() -> tuple[list[dict[str, str]], int]:
    """Live satellite catalog rows (launch/decay history) for Starlink, active + decayed."""
    resp = _get_with_retries(CELESTRAK_SATCAT_URL, timeout=120)
    return _parse_satcat_csv(resp.text), len(resp.content)


def derive_core_stubs(launches_data: list[JsonRecord]) -> list[JsonRecord]:
    """No live or cached source has cores metadata (serial/block/landing counts)
    -- see module docstring. Stub just the id for every core referenced by a
    launch so the launch_cores foreign key resolves; every other column stays
    null."""
    core_ids: set[str] = set()
    for launch in launches_data:
        for c in launch.get("cores") or []:
            cid = c.get("core")
            if cid:
                core_ids.add(cid)
    return [{"id": cid} for cid in sorted(core_ids)]


def _semimajor_axis_km(mean_motion_rev_per_day: float | None) -> float | None:
    """Kepler's third law: a = (mu / n^2)^(1/3), with n converted to rad/s."""
    if not mean_motion_rev_per_day:
        return None
    n_rad_s = mean_motion_rev_per_day * 2 * math.pi / 86400.0
    return (EARTH_MU_KM3_S2 / (n_rad_s**2)) ** (1 / 3)


def _to_float(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _celestrak_to_starlink_records(
    gp_rows: list[JsonRecord], satcat_rows: list[dict[str, str]]
) -> list[JsonRecord]:
    """Adapt live Celestrak data into the spaceTrack-nested shape `_starlink_row()`
    already expects from the (now-dead) SpaceX API's `starlink` endpoint, so
    that mapper (and its tests) don't need to change -- see module docstring.
    """
    satcat_by_object_id = {row["OBJECT_ID"]: row for row in satcat_rows if row.get("OBJECT_ID")}
    gp_by_object_id = {row["OBJECT_ID"]: row for row in gp_rows if row.get("OBJECT_ID")}

    records: list[JsonRecord] = []
    skipped = 0
    for object_id, gp in gp_by_object_id.items():
        try:
            sat = satcat_by_object_id.get(object_id, {})
            mean_motion = gp.get("MEAN_MOTION")
            semimajor = _semimajor_axis_km(mean_motion)
            records.append({
                "id": str(gp.get("NORAD_CAT_ID") or object_id),
                "version": None, "launch": None, "longitude": None, "latitude": None,
                "height_km": (semimajor - EARTH_MEAN_RADIUS_KM) if semimajor is not None else None,
                "velocity_kms": None,
                "spaceTrack": {
                    "OBJECT_NAME": gp.get("OBJECT_NAME"), "OBJECT_ID": object_id,
                    "NORAD_CAT_ID": gp.get("NORAD_CAT_ID"),
                    "LAUNCH_DATE": sat.get("LAUNCH_DATE") or None,
                    "COUNTRY_CODE": sat.get("OWNER") or None,
                    "EPOCH": gp.get("EPOCH"), "MEAN_MOTION": mean_motion,
                    "ECCENTRICITY": gp.get("ECCENTRICITY"), "INCLINATION": gp.get("INCLINATION"),
                    "PERIOD": (1440.0 / mean_motion) if mean_motion else None,
                    "SEMIMAJOR_AXIS": semimajor,
                    "DECAY_DATE": sat.get("DECAY_DATE") or None,
                },
            })
        except Exception:
            skipped += 1
            logger.warning(
                "starlink: skipping malformed Celestrak GP record object_id=%s", object_id, exc_info=True
            )

    # Decayed/no-longer-tracked satellites have no current GP element, but
    # SATCAT still has their launch/decay history and last-known orbit --
    # keep them (with coarser, non-propagated orbital fields) rather than
    # silently dropping all decay history.
    for object_id, sat in satcat_by_object_id.items():
        if object_id in gp_by_object_id:
            continue
        try:
            apogee, perigee = _to_float(sat.get("APOGEE")), _to_float(sat.get("PERIGEE"))
            records.append({
                "id": str(sat.get("NORAD_CAT_ID") or object_id),
                "version": None, "launch": None, "longitude": None, "latitude": None,
                "height_km": (
                    (apogee + perigee) / 2 if apogee is not None and perigee is not None else None
                ),
                "velocity_kms": None,
                "spaceTrack": {
                    "OBJECT_NAME": sat.get("OBJECT_NAME"), "OBJECT_ID": object_id,
                    "NORAD_CAT_ID": sat.get("NORAD_CAT_ID"),
                    "LAUNCH_DATE": sat.get("LAUNCH_DATE") or None,
                    "COUNTRY_CODE": sat.get("OWNER") or None,
                    "EPOCH": None, "MEAN_MOTION": None, "ECCENTRICITY": None,
                    "INCLINATION": _to_float(sat.get("INCLINATION")),
                    "PERIOD": _to_float(sat.get("PERIOD")),
                    "SEMIMAJOR_AXIS": None,
                    "DECAY_DATE": sat.get("DECAY_DATE") or None,
                },
            })
        except Exception:
            skipped += 1
            logger.warning(
                "starlink: skipping malformed SATCAT record object_id=%s", object_id, exc_info=True
            )

    if skipped:
        logger.warning(
            "starlink: skipped %d malformed record(s) out of %d", skipped, len(records) + skipped
        )
    return records


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


def safe_map(rows: list[JsonRecord], mapper: RowMapper, endpoint: str) -> list[Row]:
    """Apply mapper to each row, logging and skipping any record that doesn't
    match our assumptions instead of aborting the whole endpoint's load."""
    mapped: list[Row] = []
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


def _fk_mismatch_reason(truncated: bool) -> str:
    """rockets/capsules/launches/payloads are independently-dated Wayback
    snapshots (see README "Known data quality"), so a child row can
    reference a parent id that exists in its own snapshot but not in a
    differently-dated one. `truncated` distinguishes that genuine
    cross-snapshot mismatch from expected `--limit` truncation (which
    produces the exact same symptom -- a missing parent id -- for an
    unrelated reason), so callers' log messages don't misdiagnose the cause.
    """
    return "likely --limit truncation, not a data issue" if truncated else "a cross-snapshot mismatch"


def _known_ids(conn: sqlite3.Connection, table: str, id_column: str) -> set[Any]:
    """table/id_column are always internal literals (never derived from
    fetched data), so building the query with an f-string here is safe."""
    return {r[0] for r in conn.execute(f"SELECT {id_column} FROM {table}")}


def _null_dangling_fk(
    conn: sqlite3.Connection,
    rows: list[Row],
    fk_column: str,
    parent_table: str,
    parent_id_column: str,
    *,
    id_column: str,
    endpoint: str,
    truncated: bool,
) -> None:
    """Null out fk_column on any row whose value isn't currently in
    parent_table, rather than letting the FK constraint abort the whole
    insert. See `_fk_mismatch_reason` for what `truncated` is for.
    """
    known_ids = _known_ids(conn, parent_table, parent_id_column)
    reason = _fk_mismatch_reason(truncated)
    for row in rows:
        value = row.get(fk_column)
        if value is not None and value not in known_ids:
            logger.warning(
                "%s: %s=%s references %s=%s, absent from its table (%s) -- nulling the FK",
                endpoint, id_column, row.get(id_column), fk_column, value, reason,
            )
            row[fk_column] = None


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_PATH.read_text())


def load_rockets(conn: sqlite3.Connection, rows: list[JsonRecord]) -> None:
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


def load_launchpads(conn: sqlite3.Connection, rows: list[JsonRecord]) -> None:
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


def load_landpads(conn: sqlite3.Connection, rows: list[JsonRecord]) -> None:
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


def load_capsules(conn: sqlite3.Connection, rows: list[JsonRecord]) -> None:
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


def load_cores(conn: sqlite3.Connection, rows: list[JsonRecord]) -> None:
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


def load_launches(conn: sqlite3.Connection, rows: list[JsonRecord], truncated: bool = False) -> None:
    # rockets/launchpads/launches can be independently-dated sources (see
    # README "Known data quality") -- null a dangling rocket_id/launchpad_id
    # rather than let the FK constraint abort the whole insert.
    mapped = safe_map(rows, _launch_row, "launches")
    _null_dangling_fk(
        conn, mapped, "rocket_id", "rockets", "rocket_id",
        id_column="launch_id", endpoint="launches", truncated=truncated,
    )
    _null_dangling_fk(
        conn, mapped, "launchpad_id", "launchpads", "launchpad_id",
        id_column="launch_id", endpoint="launches", truncated=truncated,
    )

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
        mapped,
    )

    # child collections: delete-then-reinsert per launch for idempotency
    failure_rows: list[Row] = []
    core_rows: list[Row] = []
    capsule_rows: list[Row] = []
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
        # landpad_id comes from the hand-seeded LANDPADS_SEED (a different
        # source than the launches snapshot) -- null a dangling reference
        # rather than let the FK constraint abort the whole insert.
        _null_dangling_fk(
            conn, core_rows, "landpad_id", "landpads", "landpad_id",
            id_column="launch_id", endpoint="launch_cores", truncated=truncated,
        )
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
        # capsule_id has the same cross-snapshot risk as rocket_id/launchpad_id/
        # landpad_id above (capsules is an independently-dated Wayback snapshot)
        # -- but unlike those, capsule_id is NOT NULL here (part of the
        # composite PK), so an unresolvable reference means dropping the
        # junction row entirely rather than nulling a mandatory column.
        known_capsule_ids = _known_ids(conn, "capsules", "capsule_id")
        reason = _fk_mismatch_reason(truncated)
        resolvable_capsule_rows = []
        for row in capsule_rows:
            if row["capsule_id"] in known_capsule_ids:
                resolvable_capsule_rows.append(row)
            else:
                logger.warning(
                    "launch_capsules: launch_id=%s references capsule_id=%s, absent from "
                    "its table (%s) -- dropping the junction row",
                    row["launch_id"], row["capsule_id"], reason,
                )
        conn.executemany(
            "INSERT OR IGNORE INTO launch_capsules (launch_id, capsule_id) VALUES (:launch_id, :capsule_id)",
            resolvable_capsule_rows,
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


def load_payloads(conn: sqlite3.Connection, rows: list[JsonRecord], truncated: bool = False) -> None:
    # rockets/capsules/launches/payloads are independently-dated Wayback
    # snapshots (see README), so a payload can reference a launch_id that
    # existed when the payloads snapshot was taken but is absent from the
    # (differently-dated) launches snapshot -- e.g. a placeholder combined
    # launch record later split/removed upstream. Null the FK rather than
    # fail the whole load.
    mapped = safe_map(rows, _payload_row, "payloads")
    _null_dangling_fk(
        conn, mapped, "launch_id", "launches", "launch_id",
        id_column="payload_id", endpoint="payloads", truncated=truncated,
    )

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
        mapped,
    )

    customer_rows: list[Row] = []
    nationality_rows: list[Row] = []
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


def load_starlink(conn: sqlite3.Connection, rows: list[JsonRecord]) -> None:
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


LOADERS: dict[str, Callable[[sqlite3.Connection, list[JsonRecord]], None]] = {
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

    def limited(rows: list[JsonRecord]) -> list[JsonRecord]:
        return rows if args.limit is None else rows[: args.limit]

    # Distinguishes a genuine cross-snapshot FK mismatch from expected --limit
    # truncation in _null_dangling_fk()'s log messages (they produce the same
    # symptom -- a missing parent id -- for very different reasons).
    truncated = args.limit is not None

    # Each table commits independently (rather than one all-or-nothing
    # transaction for the whole run) -- sources are now heterogeneous
    # (Wayback/hand-seeded/derived/live Celestrak), so a transient failure
    # in one (e.g. Celestrak's throttle, see below) shouldn't force
    # re-fetching/re-loading tables that already succeeded on retry.
    total_raw_bytes = 0

    try:
        total_raw_bytes = _run_ingest(conn, truncated, limited, total_raw_bytes)
    except Exception as exc:
        # Tables already committed above (each is its own `with conn` block)
        # are unaffected -- only ingest_runs, written last, is left stale,
        # which is exactly the partial-run signal described in schema.sql.
        # Logging cleanly + a plain exit code beats an unhandled traceback,
        # since this is routinely a known, actionable failure (e.g.
        # Celestrak's throttle below) rather than a bug.
        logger.error("Ingestion aborted: %s", exc)
        raise SystemExit(1) from None
    finally:
        conn.close()

    logger.info(
        "Total raw bytes downloaded: %.2f MB (Wayback Machine + Celestrak; excludes the "
        "hand-seeded launchpads/landpads and derived cores stubs, which aren't network-fetched)",
        total_raw_bytes / 1_000_000,
    )


def _run_ingest(
    conn: sqlite3.Connection,
    truncated: bool,
    limited: Callable[[list[JsonRecord]], list[JsonRecord]],
    total_raw_bytes: int,
) -> int:
    with conn:
        logger.info("Fetching rockets (Wayback Machine snapshot -- live API is down, see README) ...")
        rockets, n = fetch_wayback("rockets")
        total_raw_bytes += n
        validate_response("rockets", rockets)
        rockets = limited(rockets)
        LOADERS["rockets"](conn, rockets)
        logger.info("  loaded %d rockets record(s)", len(rockets))

    with conn:
        logger.info("Loading launchpads (hand-seeded, no live/cached source exists -- see README) ...")
        validate_response("launchpads", LAUNCHPADS_SEED)
        launchpads = limited(LAUNCHPADS_SEED)
        LOADERS["launchpads"](conn, launchpads)
        logger.info("  loaded %d launchpads record(s)", len(launchpads))

    with conn:
        logger.info("Loading landpads (hand-seeded, no live/cached source exists -- see README) ...")
        validate_response("landpads", LANDPADS_SEED)
        landpads = limited(LANDPADS_SEED)
        LOADERS["landpads"](conn, landpads)
        logger.info("  loaded %d landpads record(s)", len(landpads))

    with conn:
        logger.info("Fetching capsules (Wayback Machine snapshot) ...")
        capsules, n = fetch_wayback("capsules")
        total_raw_bytes += n
        validate_response("capsules", capsules)
        capsules = limited(capsules)
        LOADERS["capsules"](conn, capsules)
        logger.info("  loaded %d capsules record(s)", len(capsules))

    with conn:
        logger.info("Fetching launches (Wayback Machine snapshot) ...")
        launches, n = fetch_wayback("launches")
        total_raw_bytes += n
        validate_response("launches", launches)
        launches = limited(launches)

        logger.info("Deriving cores stubs from launches (no live/cached cores source -- see README) ...")
        cores = derive_core_stubs(launches)
        validate_response("cores", cores)
        LOADERS["cores"](conn, cores)
        logger.info("  loaded %d cores record(s) (id only -- no metadata source available)", len(cores))

        load_launches(conn, launches, truncated=truncated)
        logger.info("  loaded %d launches record(s)", len(launches))

    with conn:
        logger.info("Fetching payloads (Wayback Machine snapshot) ...")
        payloads, n = fetch_wayback("payloads")
        total_raw_bytes += n
        validate_response("payloads", payloads)
        payloads = limited(payloads)
        load_payloads(conn, payloads, truncated=truncated)
        logger.info("  loaded %d payloads record(s)", len(payloads))

    with conn:
        logger.info("Fetching starlink (live Celestrak GP elements + SATCAT -- see README) ...")
        try:
            gp_rows, gp_bytes = fetch_celestrak_gp_starlink()
            satcat_rows, satcat_bytes = fetch_celestrak_satcat_starlink()
        except RuntimeError as exc:
            raise RuntimeError(
                f"{exc}\nNote: celestrak.org enforces a per-IP courtesy throttle on repeat "
                "GP downloads of the same group within its ~2h refresh window (a 403 with a "
                "'has not updated since your last successful download' body is that throttle, "
                "not a real outage) -- wait for the window to pass, or retry from a different "
                "network."
            ) from exc
        total_raw_bytes += gp_bytes + satcat_bytes
        starlink = _celestrak_to_starlink_records(gp_rows, satcat_rows)
        validate_response("starlink", starlink)
        starlink = limited(starlink)
        LOADERS["starlink"](conn, starlink)
        logger.info("  loaded %d starlink record(s)", len(starlink))

    # Only reached if every table above committed -- see the ingest_runs
    # comment in schema.sql for why this matters.
    with conn:
        conn.execute(
            "INSERT INTO ingest_runs (id, completed_at) VALUES (1, CURRENT_TIMESTAMP) "
            "ON CONFLICT(id) DO UPDATE SET completed_at = excluded.completed_at"
        )

    return total_raw_bytes


if __name__ == "__main__":
    main()
