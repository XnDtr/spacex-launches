"""Tests for scripts/ingest.py.

These run entirely offline against synthetic fixtures shaped like real API
responses -- they exercise the transform/load/idempotency logic without
depending on api.spacexdata.com being reachable, so they run the same in CI
as on a laptop.
"""
import copy
import sqlite3

import ingest
import pytest
from fixtures import FIXTURES


def make_db():
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    ingest.init_schema(conn)
    return conn


def load_all(conn, fixtures):
    with conn:
        for name in ingest.ENDPOINTS:
            ingest.LOADERS[name](conn, fixtures[name])


def counts(conn):
    tables = [
        "rockets", "launchpads", "landpads", "capsules", "cores", "launches",
        "launch_failures", "launch_cores", "launch_capsules", "payloads",
        "payload_customers", "payload_nationalities", "starlink",
    ]
    return {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in tables}


def test_initial_load_populates_every_table():
    conn = make_db()
    load_all(conn, FIXTURES)
    c = counts(conn)
    assert c["rockets"] == 1
    assert c["launches"] == 1
    assert c["launch_failures"] == 1
    assert c["launch_cores"] == 1
    assert c["launch_capsules"] == 1
    assert c["payload_customers"] == 2
    assert c["payload_nationalities"] == 1
    assert c["starlink"] == 1


def test_reingest_is_idempotent():
    conn = make_db()
    load_all(conn, FIXTURES)
    before = counts(conn)
    load_all(conn, FIXTURES)  # re-run against identical data
    after = counts(conn)
    assert before == after


def test_upstream_field_change_updates_in_place_not_as_new_row():
    conn = make_db()
    load_all(conn, FIXTURES)

    changed = copy.deepcopy(FIXTURES)
    changed["rockets"][0]["success_rate_pct"] = 42
    load_all(conn, changed)

    c = counts(conn)
    assert c["rockets"] == 1  # still one row, not two
    value = conn.execute(
        "SELECT success_rate_pct FROM rockets WHERE rocket_id = ?",
        (FIXTURES["rockets"][0]["id"],),
    ).fetchone()[0]
    assert value == 42


def test_removed_child_item_is_not_left_behind():
    """A launch's cores/failures/capsules should reflect the *current* API
    response -- if a failure entry disappears upstream, the stale row must
    not survive a re-ingest (this is why launch_cores/failures/capsules use
    delete-then-reinsert rather than INSERT OR IGNORE alone)."""
    conn = make_db()
    load_all(conn, FIXTURES)
    assert counts(conn)["launch_failures"] == 1

    changed = copy.deepcopy(FIXTURES)
    changed["launches"][0]["failures"] = []
    load_all(conn, changed)

    assert counts(conn)["launch_failures"] == 0


def test_malformed_record_is_skipped_not_fatal():
    """One bad record (missing required 'id') must not take down the whole
    endpoint's load -- safe_map should log and skip it, leaving good records
    intact."""
    conn = make_db()
    broken = copy.deepcopy(FIXTURES)
    broken["rockets"].append({"name": "No ID Rocket"})  # missing "id" -> KeyError in mapper

    load_all(conn, broken)  # must not raise

    c = counts(conn)
    assert c["rockets"] == 1  # only the well-formed rocket made it in


def test_joins_resolve_across_foreign_keys():
    conn = make_db()
    load_all(conn, FIXTURES)
    row = conn.execute(
        "SELECT l.name, r.name, lp.name FROM launches l "
        "JOIN rockets r ON l.rocket_id = r.rocket_id "
        "JOIN launchpads lp ON l.launchpad_id = lp.launchpad_id"
    ).fetchone()
    assert row == ("FalconSat", "Falcon 9", "CCSFS SLC 40")


def test_validate_response_rejects_non_list_payload():
    with pytest.raises(RuntimeError):
        ingest.validate_response("launches", {"docs": [], "totalDocs": 0})


def test_validate_response_accepts_list_and_warns_on_low_count(caplog):
    with caplog.at_level("WARNING", logger="spacex_ingest"):
        ingest.validate_response("launches", [{"id": "1"}])  # below MIN_EXPECTED_COUNTS
    assert any("expected at least" in message for message in caplog.messages)
