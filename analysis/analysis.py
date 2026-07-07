"""Analysis questions over the SpaceX SQLite database built by scripts/ingest.py.

Run after ingestion:
    python analysis/analysis.py [--db spacex.db] [--out analysis/output]

Produces printed tables for the SQL questions and PNG charts for the pandas
questions in --out (default: analysis/output/).
"""
import argparse
import pathlib
import sqlite3

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

# Q6 unit-economics assumptions -- no API publishes Starlink subscription
# pricing or per-satellite economics, so these are documented, cited
# estimates, not measured data. See README "Starlink unit economics" for
# full sourcing. Fleet-wide (not band-dependent): the same cost is compared
# against every band's estimated revenue/satellite.
BUILD_COST_USD_PER_SAT = 500_000.0  # publicly reported v1.5/v2-Mini build cost estimate
LAUNCH_COST_USD = 20_000_000.0      # SpaceX-internal reused-booster marginal launch cost estimate
SATS_PER_LAUNCH = 22                # typical v2 Mini batch size (reported range: 20-24)
COST_USD_PER_SAT = BUILD_COST_USD_PER_SAT + LAUNCH_COST_USD / SATS_PER_LAUNCH

# Assumed average concurrent per-user bandwidth allocation, used to convert a
# satellite's raw throughput into a max-customers estimate. Real usage varies
# a lot by time of day/congestion; this is a simplifying constant applied
# uniformly across bands.
AVG_USER_MBPS = 50.0


def q1_success_rate_by_year(conn: sqlite3.Connection) -> pd.DataFrame:
    """Q1 (pure SQL) -- Has SpaceX's launch success rate improved over time?

    Why interesting: reliability trend is the headline metric for a launch
    provider -- customers and insurers price risk off it. Excludes upcoming
    (not-yet-flown) launches so 'success' is only computed over resolved
    outcomes.
    """
    sql = """
        SELECT
            substr(date_utc, 1, 4)                          AS year,
            COUNT(*)                                         AS total_launches,
            SUM(success)                                     AS successful,
            ROUND(100.0 * SUM(success) / COUNT(*), 1)        AS success_rate_pct
        FROM launches
        WHERE upcoming = 0 AND success IS NOT NULL
        GROUP BY year
        ORDER BY year;
    """
    df = pd.read_sql_query(sql, conn)
    print("\n=== Q1: Launch success rate by year ===")
    print(sql)
    print(df.to_string(index=False))
    return df


def q2_core_reuse_landing_success(conn: sqlite3.Connection) -> pd.DataFrame:
    """Q2 (pure SQL) -- Do more-reused boosters land as reliably as new ones?

    Why interesting: core reuse is the core (pun intended) of SpaceX's cost
    model. If landing success dropped off with reuse count, that would be a
    real engineering red flag -- this checks it directly from the data.
    """
    sql = """
        SELECT
            core_flight_num,
            COUNT(*)                                              AS attempts,
            SUM(landing_attempt)                                  AS landing_attempts,
            SUM(landing_success)                                  AS landing_successes,
            ROUND(100.0 * SUM(landing_success) /
                  NULLIF(SUM(landing_attempt), 0), 1)             AS landing_success_pct
        FROM launch_cores
        WHERE core_flight_num IS NOT NULL
        GROUP BY core_flight_num
        HAVING attempts >= 3
        ORDER BY core_flight_num;
    """
    df = pd.read_sql_query(sql, conn)
    print("\n=== Q2: Landing success rate by core flight number (1st use, 2nd use, ...) ===")
    print(sql)
    print(df.to_string(index=False))
    return df


def q3_launchpad_success_trend(conn: sqlite3.Connection) -> pd.DataFrame:
    """Q3 (pure SQL, CTE + JOIN + window function) -- Which launchpads are
    trending better or worse year over year, and by how much?

    Why interesting: Q1's year-over-year rate is fleet-wide and can hide a
    launchpad-specific story (a bad year at one pad dragging down the
    average while others improve, or a newly commissioned pad still working
    out kinks). Joining launches to launchpads and using LAG() to compute
    each pad's change vs. its own prior year isolates that signal instead of
    just restating the aggregate from Q1.
    """
    sql = """
        WITH yearly AS (
            SELECT
                lp.name                                          AS launchpad,
                substr(l.date_utc, 1, 4)                         AS year,
                COUNT(*)                                         AS total_launches,
                ROUND(100.0 * SUM(l.success) / COUNT(*), 1)      AS success_rate_pct
            FROM launches l
            JOIN launchpads lp ON l.launchpad_id = lp.launchpad_id
            WHERE l.upcoming = 0 AND l.success IS NOT NULL
            GROUP BY lp.name, year
        )
        SELECT
            launchpad, year, total_launches, success_rate_pct,
            ROUND(
                success_rate_pct
                - LAG(success_rate_pct) OVER (PARTITION BY launchpad ORDER BY year),
                1
            ) AS pct_pt_change_vs_prev_year
        FROM yearly
        ORDER BY launchpad, year;
    """
    df = pd.read_sql_query(sql, conn)
    print("\n=== Q3: Launchpad success-rate trend, year over year (CTE + JOIN + LAG window function) ===")
    print(sql)
    print(df.to_string(index=False))
    return df


def q4_payload_mass_by_orbit(conn: sqlite3.Connection, out_dir: pathlib.Path) -> pd.DataFrame:
    """Q4 (pandas + matplotlib) -- How has payload mass to each orbit class evolved?

    Why interesting: rising mass-to-orbit for LEO/GTO over time is a direct
    signal of Falcon 9's growing capacity (block upgrades, reuse efficiency)
    and of the Starlink-driven shift in SpaceX's own manifest.
    """
    sql = """
        SELECT l.date_utc, p.orbit, p.mass_kg
        FROM payloads p
        JOIN launches l ON p.launch_id = l.launch_id
        WHERE p.mass_kg IS NOT NULL AND p.orbit IS NOT NULL AND l.upcoming = 0;
    """
    df = pd.read_sql_query(sql, conn, parse_dates=["date_utc"])
    print("\n=== Q4: Payload mass by orbit over time (pandas) ===")
    print(sql)
    print(df.describe(include="all").to_string())

    top_orbits = df["orbit"].value_counts().head(5).index
    fig, ax = plt.subplots(figsize=(9, 5))
    for orbit in top_orbits:
        sub = df[df["orbit"] == orbit].sort_values("date_utc")
        ax.scatter(sub["date_utc"], sub["mass_kg"], label=orbit, alpha=0.6, s=15)
    ax.set_xlabel("Launch date")
    ax.set_ylabel("Payload mass (kg)")
    ax.set_title("Payload mass to orbit over time, by orbit class")
    ax.legend()
    fig.autofmt_xdate()
    fig.tight_layout()
    out_path = out_dir / "q4_payload_mass_by_orbit.png"
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    print(f"Saved chart: {out_path}")
    return df


def q5_starlink_altitude_and_cadence(conn: sqlite3.Connection, out_dir: pathlib.Path) -> pd.DataFrame:
    """Q5 (pandas + matplotlib) -- What does the Starlink constellation's
    altitude distribution look like, and how fast is it being launched?

    Why interesting: Starlink is now the majority of SpaceX's launch manifest
    and satellite count by a wide margin. Altitude clustering reflects
    deliberate shell design (orbital-debris mitigation, coverage bands), and
    the cadence chart shows how aggressively the constellation is being built
    out launch-over-launch.
    """
    sql = """
        SELECT s.height_km, s.launch_date, l.name AS launch_name
        FROM starlink s
        LEFT JOIN launches l ON s.launch_id = l.launch_id
        WHERE s.height_km IS NOT NULL;
    """
    df = pd.read_sql_query(sql, conn, parse_dates=["launch_date"])
    print("\n=== Q5: Starlink altitude distribution & launch cadence (pandas) ===")
    print(sql)
    print(df["height_km"].describe().to_string())

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    axes[0].hist(df["height_km"].dropna(), bins=40, color="steelblue")
    axes[0].set_xlabel("Altitude (km)")
    axes[0].set_ylabel("Satellite count")
    axes[0].set_title("Starlink altitude distribution")

    cadence = (
        df.dropna(subset=["launch_date"])
        .groupby(df["launch_date"].dt.to_period("M"))
        .size()
    )
    cadence.index = cadence.index.to_timestamp()
    axes[1].plot(cadence.index, cadence.values, marker="o", markersize=3)
    axes[1].set_xlabel("Month")
    axes[1].set_ylabel("Satellites launched")
    axes[1].set_title("Starlink deployment cadence")
    fig.autofmt_xdate()

    fig.tight_layout()
    out_path = out_dir / "q5_starlink_altitude_cadence.png"
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    print(f"Saved chart: {out_path}")
    return df


def q6_starlink_unit_economics(conn: sqlite3.Connection, out_dir: pathlib.Path) -> pd.DataFrame:
    """Q6 (SQL + pandas) -- Which orbital bands generate the best revenue per
    satellite relative to the (fixed) cost of building and launching one?

    Why interesting: Starlink's margin isn't uniform across the constellation
    -- a satellite serving a premium/low-competition market recoups its
    build+launch cost far faster than one serving a low-price market, even
    though every satellite costs about the same to build and launch. This is
    a back-of-envelope model, not verified financials: no API publishes
    Starlink subscription pricing or per-satellite economics, so both the
    regional prices and the per-satellite customer capacity are documented
    assumptions (see README "Starlink unit economics" for full sourcing).
    "Customers per satellite" is estimated as raw throughput / an assumed
    average per-user bandwidth, held constant per band. "Regional flyover
    band" is defined by orbital inclination (the max latitude a satellite's
    ground track reaches), not by country, since that's directly computable
    from the ingested starlink.inclination_deg column -- real subscription
    prices vary by country, so each band's price is a blended estimate
    across its representative markets (see starlink_pricing_bands.notes).
    """
    sql = """
        SELECT
            b.band_name,
            b.min_inclination_deg,
            b.max_inclination_deg,
            b.monthly_price_usd,
            b.sat_throughput_gbps,
            COUNT(s.starlink_id) AS active_satellites
        FROM starlink_pricing_bands b
        LEFT JOIN starlink s
            ON s.inclination_deg BETWEEN b.min_inclination_deg AND b.max_inclination_deg
            AND s.decayed = 0
        GROUP BY b.band_name, b.min_inclination_deg, b.max_inclination_deg,
                 b.monthly_price_usd, b.sat_throughput_gbps
        ORDER BY b.min_inclination_deg;
    """
    df = pd.read_sql_query(sql, conn)
    df["avg_users_per_sat"] = (df["sat_throughput_gbps"] * 1000 / AVG_USER_MBPS).round(0)
    df["monthly_rev_per_sat_usd"] = (df["avg_users_per_sat"] * df["monthly_price_usd"]).round(0)
    df["payback_months"] = (COST_USD_PER_SAT / df["monthly_rev_per_sat_usd"]).round(1)

    print("\n=== Q6: Starlink unit economics by orbital band (SQL + pandas) ===")
    print(sql)
    print(
        f"Assumptions: ${COST_USD_PER_SAT:,.0f}/satellite build+launch cost, "
        f"{AVG_USER_MBPS:.0f} Mbps/user -- see README for sourcing."
    )
    print(df.to_string(index=False))

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    axes[0].bar(df["band_name"], df["monthly_rev_per_sat_usd"], color="steelblue")
    axes[0].set_ylabel("Monthly revenue / satellite (USD, est.)")
    axes[0].set_title("Estimated revenue per satellite, by orbital band")
    plt.setp(axes[0].get_xticklabels(), rotation=25, ha="right")

    axes[1].bar(df["band_name"], df["payback_months"], color="darkorange")
    axes[1].set_ylabel("Months to recoup build + launch cost")
    axes[1].set_title(f"Payback period (cost/sat = ${COST_USD_PER_SAT:,.0f}, est.)")
    plt.setp(axes[1].get_xticklabels(), rotation=25, ha="right")

    fig.tight_layout()
    out_path = out_dir / "q6_starlink_unit_economics.png"
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    print(f"Saved chart: {out_path}")
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="spacex.db")
    parser.add_argument("--out", default="analysis/output")
    args = parser.parse_args()

    out_dir = pathlib.Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(args.db)
    q1_success_rate_by_year(conn)
    q2_core_reuse_landing_success(conn)
    q3_launchpad_success_trend(conn)
    q4_payload_mass_by_orbit(conn, out_dir)
    q5_starlink_altitude_and_cadence(conn, out_dir)
    q6_starlink_unit_economics(conn, out_dir)
    conn.close()


if __name__ == "__main__":
    main()
