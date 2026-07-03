"""Synthetic records shaped like real SpaceX API v4 responses, used by the
test suite. Kept deliberately small -- these exist to validate parsing,
loading, and idempotency logic, not to simulate production data volume."""

ROCKETS = [{
    "id": "5e9d0d95eda69955f709d1eb", "name": "Falcon 9", "type": "rocket",
    "active": True, "stages": 2, "boosters": 0, "cost_per_launch": 50000000,
    "success_rate_pct": 98, "first_flight": "2010-06-04", "country": "United States",
    "company": "SpaceX", "height": {"meters": 70.0}, "diameter": {"meters": 3.7},
    "mass": {"kg": 549054}, "description": "Two-stage rocket",
}]

LAUNCHPADS = [{
    "id": "5e9e4501f509092b78566f87", "name": "CCSFS SLC 40", "full_name": "Cape Canaveral SFS",
    "locality": "Cape Canaveral", "region": "Florida", "latitude": 28.56, "longitude": -80.57,
    "launch_attempts": 100, "launch_successes": 98, "status": "active", "details": "test",
}]

LANDPADS = [{
    "id": "5e9e3032383ecb267a34e7c7", "name": "OCISLY", "full_name": "Of Course I Still Love You",
    "type": "ASDS", "locality": "Port Canaveral", "region": "Florida", "latitude": 28.4,
    "longitude": -80.6, "landing_attempts": 50, "landing_successes": 45, "status": "active",
    "wikipedia": "https://en.wikipedia.org/x", "details": "droneship",
}]

CAPSULES = [{
    "id": "5e9e2c5bf3591817f23b2663", "serial": "C101", "status": "active", "type": "Dragon 1.0",
    "reuse_count": 3, "water_landings": 3, "land_landings": 0, "last_update": None,
}]

CORES = [{
    "id": "5e9e289df35918033d3b2623", "serial": "B1058", "block": 5, "status": "active",
    "reuse_count": 5, "rtls_attempts": 3, "rtls_landings": 3, "asds_attempts": 2,
    "asds_landings": 2, "last_update": None,
}]

LAUNCHES = [{
    "id": "5eb87d46ffd86e000604b384", "flight_number": 1, "name": "FalconSat",
    "date_utc": "2006-03-24T22:30:00.000Z", "date_unix": 1143239400,
    "date_precision": "hour", "static_fire_date_utc": None, "tbd": False, "net": False,
    "window": 0, "rocket": "5e9d0d95eda69955f709d1eb", "launchpad": "5e9e4501f509092b78566f87",
    "success": False, "upcoming": False, "details": "Engine failure at 33 seconds",
    "fairings": {"reused": False, "recovery_attempt": False, "recovered": False},
    "failures": [{"time": 33, "altitude": None, "reason": "merlin engine failure"}],
    "cores": [{
        "core": "5e9e289df35918033d3b2623", "flight": 1, "gridfins": False, "legs": False,
        "reused": False, "landing_attempt": False, "landing_success": None,
        "landing_type": None, "landpad": None,
    }],
    "capsules": ["5e9e2c5bf3591817f23b2663"],
    "links": {
        "patch": {"small": "http://x/small.png", "large": "http://x/large.png"},
        "webcast": "http://youtube/x", "article": "http://x", "wikipedia": "http://wiki/x",
    },
}]

PAYLOADS = [{
    "id": "5eb0e4b5b6c3bb0006eeb1e1", "name": "FalconSAT-2", "type": "Satellite",
    "launch": "5eb87d46ffd86e000604b384", "reused": False, "mass_kg": 20,
    "orbit": "LEO", "customers": ["DARPA", "USAF"], "nationalities": ["United States"],
    "orbit_params": {
        "reference_system": "geocentric", "regime": "low-earth", "longitude": None,
        "semi_major_axis_km": 6971.79, "eccentricity": 0.0001, "periapsis_km": 400,
        "apoapsis_km": 600, "inclination_deg": 39, "period_min": 90, "lifespan_years": 3,
    },
}]

STARLINK = [{
    "id": "5eed7714096e59000698560d", "version": "v0.9", "launch": "5eb87d46ffd86e000604b384",
    "longitude": -95.1, "latitude": 40.2, "height_km": 550.2, "velocity_kms": 7.6,
    "spaceTrack": {
        "OBJECT_NAME": "STARLINK-1", "OBJECT_ID": "2019-029A", "NORAD_CAT_ID": 44235,
        "LAUNCH_DATE": "2019-05-24", "COUNTRY_CODE": "US", "EPOCH": "2022-01-01T00:00:00.000000",
        "MEAN_MOTION": 15.05, "ECCENTRICITY": 0.0001, "INCLINATION": 53.0,
        "PERIOD": 95.6, "SEMIMAJOR_AXIS": 6931.0, "DECAY_DATE": None,
    },
}]

FIXTURES = {
    "rockets": ROCKETS, "launchpads": LAUNCHPADS, "landpads": LANDPADS,
    "capsules": CAPSULES, "cores": CORES, "launches": LAUNCHES,
    "payloads": PAYLOADS, "starlink": STARLINK,
}
