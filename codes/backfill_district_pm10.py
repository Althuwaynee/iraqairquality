#!/usr/bin/env python3
"""
backfill_district_pm10.py
Backfill district-level PM10 for past forecast hours
(using the same constrained IDW logic as realtime)
"""
import sqlite3
import pandas as pd
import argparse
from datetime import datetime, timedelta
import sys
import os
import sqlite3
import pandas as pd
import argparse
from datetime import datetime, timedelta
current_dir = os.path.dirname(os.path.abspath(__file__))
# Assuming your realtime_dust_current.py is in the same directory
# If it's in a different directory, adjust the path accordingly
sys.path.insert(0, current_dir)
# ---- import from your realtime script ----
from realtime_IRQ_csv_json_dust import (
    load_iraq_districts,
    interpolate_all_districts,
    store_hourly_results_sqlite
)


# ============================================================
# LOAD MULTIPLE FORECAST TIMESTAMPS
# ============================================================
def get_past_forecast_timestamps(db_path, hours):
    conn = sqlite3.connect(db_path)

    cutoff = datetime.utcnow() - timedelta(hours=hours)

    ts_df = pd.read_sql_query(
        """
        SELECT DISTINCT timestamp_utc
        FROM dust_measurements_realtime
        WHERE timestamp_utc >= ?
        ORDER BY timestamp_utc ASC
        """,
        conn,
        params=(cutoff.strftime('%Y-%m-%d %H:%M:%S'),)
    )

    conn.close()
    return ts_df["timestamp_utc"].tolist()


# ============================================================
# LOAD GRID DATA FOR ONE TIMESTAMP
# ============================================================
def load_grid_at_timestamp(db_path, timestamp_utc):
    conn = sqlite3.connect(db_path)

    df = pd.read_sql_query(
        """
        SELECT latitude, longitude, dust_concentration_ugm3
        FROM dust_measurements_realtime
        WHERE timestamp_utc = ?
        """,
        conn,
        params=(timestamp_utc,)
    )

    conn.close()
    return df


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dust-db", required=True)
    parser.add_argument("--shapefile", required=True)
    parser.add_argument("--store-db", required=True)
    parser.add_argument("--hours", type=int, default=72)
    args = parser.parse_args()

    print("=" * 60)
    print("BACKFILL DISTRICT PM10")
    print("=" * 60)

    districts = load_iraq_districts(args.shapefile)
    timestamps = get_past_forecast_timestamps(args.dust_db, args.hours)
##
    print(f"Timestamps requested: {args.hours} hours")
    print(f"Timestamps available: {len(timestamps)}")
    
    if len(timestamps) == 0:
        print("ERROR: No forecast data found!")
        return
    
    # Calculate actual hours covered
    if len(timestamps) > 1:
        from dateutil.parser import parse
        hours_covered = (parse(timestamps[-1]) - parse(timestamps[0])).total_seconds() / 3600
        print(f"Actual forecast horizon: {hours_covered:.1f} hours")

###


    print(f"Timestamps found: {len(timestamps)}")

    for ts in timestamps:
        print(f"Processing {ts} ...")
        dust_data = load_grid_at_timestamp(args.dust_db, ts)

        if dust_data.empty:
            continue

        results = interpolate_all_districts(districts, dust_data)
        store_hourly_results_sqlite(results, ts, args.store_db)

    print("Backfill complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
