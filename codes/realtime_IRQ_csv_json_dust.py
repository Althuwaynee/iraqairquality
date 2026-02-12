#!/usr/bin/env python3
"""
realtime_dust_current.py
Get current-time dust concentration for Iraq districts
(Time-locked, grid-aware, constrained IDW)
+ Store hourly PM10 for rolling means & alerts
"""
from datetime import datetime, UTC
import sqlite3
import pandas as pd
import geopandas as gpd
import numpy as np
from scipy.spatial import cKDTree
from datetime import datetime
import argparse
import json
import os

# ============================================================
# CONFIG
# ============================================================
MAX_DISTANCE_KM = 55.0
K_NEAREST = 4
IDW_POWER = 2


# ============================================================
# TIME-LOCKED DATA RETRIEVAL
# ============================================================
def get_current_time_data(db_path):
    print(f"Reading dust data from {db_path} ...")
    conn = sqlite3.connect(db_path)

    current_utc = datetime.now(UTC)
    print(f"Current UTC time: {current_utc}")

    query_ts = """
    SELECT timestamp_utc,
           ABS(strftime('%s', timestamp_utc) - strftime('%s', ?)) AS diff
    FROM dust_measurements_realtime
    ORDER BY diff ASC
    LIMIT 1
    """
    ts_df = pd.read_sql_query(
        query_ts,
        conn,
        params=(current_utc.strftime('%Y-%m-%d %H:%M:%S'),)
    )

    if ts_df.empty:
        return None, None, None

    locked_timestamp = ts_df.iloc[0]["timestamp_utc"]
    print(f"Locked forecast timestamp: {locked_timestamp}")

    query_data = """
    SELECT latitude, longitude, dust_concentration_ugm3, timestamp_utc
    FROM dust_measurements_realtime
    WHERE timestamp_utc = ?
    """
    dust_data = pd.read_sql_query(query_data, conn, params=(locked_timestamp,))
    conn.close()

    print(f"Loaded {len(dust_data)} grid points")
    return dust_data, locked_timestamp, current_utc


# ============================================================
# LOAD IRAQ DISTRICTS
# ============================================================
def load_iraq_districts(shapefile_path):
    print(f"Loading districts from {shapefile_path} ...")
    gdf = gpd.read_file(shapefile_path)

    gdf["centroid"] = gdf.geometry.centroid
    gdf["centroid_lat"] = gdf.centroid.y
    gdf["centroid_lon"] = gdf.centroid.x

    iraq_bbox = (29, 38, 39, 49)
    gdf = gdf[
        (gdf["centroid_lat"] >= iraq_bbox[0]) &
        (gdf["centroid_lat"] <= iraq_bbox[1]) &
        (gdf["centroid_lon"] >= iraq_bbox[2]) &
        (gdf["centroid_lon"] <= iraq_bbox[3])
    ].copy()

    gdf["district_id"] = (
        gdf["ID_2"].astype(str) if "ID_2" in gdf.columns else gdf.index.astype(str)
    )
    gdf["district_name"] = gdf["NAME_2"] if "NAME_2" in gdf.columns else "Unknown"
    gdf["province_name"] = gdf["NAME_1"] if "NAME_1" in gdf.columns else "Unknown"

    print(f"Districts loaded: {len(gdf)}")
    return gdf


# ============================================================
# CONSTRAINED IDW
# ============================================================
def constrained_idw(district_point, dust_data, tree):
    lat, lon = district_point
    point = np.array([lon, lat])

    distances, indices = tree.query(point, k=K_NEAREST)
    distances_km = distances * 111.0

    mask = distances_km <= MAX_DISTANCE_KM
    indices = indices[mask]
    distances_km = distances_km[mask]

    if len(indices) == 0:
        return None, 0, None

    distances_km[distances_km == 0] = 0.001
    weights = 1.0 / (distances_km ** IDW_POWER)
    weights /= weights.sum()

    values = dust_data.iloc[indices]["dust_concentration_ugm3"].values
    dust_value = np.sum(values * weights)

    return float(dust_value), int(len(indices)), float(np.mean(distances_km))


# ============================================================
# INTERPOLATE ALL DISTRICTS
# ============================================================
def interpolate_all_districts(districts, dust_data):
    dust_coords = np.column_stack([
        dust_data["longitude"].values,
        dust_data["latitude"].values
    ])
    tree = cKDTree(dust_coords)

    rows = []

    for _, d in districts.iterrows():
        value, npts, avgdist = constrained_idw(
            (d["centroid_lat"], d["centroid_lon"]),
            dust_data,
            tree
        )

        rows.append({
            "district_id": d["district_id"],
            "district_name": d["district_name"],
            "province_name": d["province_name"],
            "latitude": float(d["centroid_lat"]),
            "longitude": float(d["centroid_lon"]),
            "dust_final": value,
            "points_used": npts,
            "avg_distance_km": avgdist,
            "method": "constrained_idw"
        })

    return pd.DataFrame(rows)


# ============================================================
# STORE HOURLY RESULTS (NEW)
# ============================================================
# ============================================================
# STORE HOURLY RESULTS (NEW)
# ============================================================
def store_hourly_results_sqlite(results, timestamp_utc, db_path):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS district_pm10_hourly (
        district_id TEXT,
        district_name TEXT,
        province_name TEXT,
        latitude REAL,
        longitude REAL,
        timestamp_utc TEXT,
        pm10 REAL,
        points_used INTEGER,
        avg_distance_km REAL,
        method TEXT,
        PRIMARY KEY (district_id, timestamp_utc)
    )
    """)

    # Ensure timestamp is in consistent format: YYYY-MM-DD HH:MM:SS
    if isinstance(timestamp_utc, str):
        # Remove 'T' if present
        clean_timestamp = timestamp_utc.replace('T', ' ')
        # Remove any timezone suffix
        if '+' in clean_timestamp:
            clean_timestamp = clean_timestamp.split('+')[0].strip()
        if 'Z' in clean_timestamp:
            clean_timestamp = clean_timestamp.replace('Z', '').strip()
    else:
        # If it's a datetime object
        clean_timestamp = timestamp_utc.strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"DEBUG: Storing timestamp in database as: '{clean_timestamp}'")

    for _, r in results.iterrows():
        cur.execute("""
        INSERT OR REPLACE INTO district_pm10_hourly
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            r["district_id"],
            r["district_name"],
            r["province_name"],
            r["latitude"],
            r["longitude"],
            clean_timestamp,  # Use cleaned timestamp
            r["dust_final"],
            r["points_used"],
            r["avg_distance_km"],
            r["method"]
        ))

    conn.commit()
    conn.close()
    print(f"Stored hourly PM10 in {db_path}")


# ============================================================
# EXPORT FILES
# ============================================================
def export_results(results, data_timestamp, output_file):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Format timestamps consistently
    from datetime import UTC
    
    # Convert data_timestamp string to datetime if needed
    if isinstance(data_timestamp, str):
        # Handle both formats: '2026-02-05 15:00:00' and '2026-02-05T12:00:00'
        data_timestamp = data_timestamp.replace('T', ' ')
        forecast_dt = datetime.strptime(data_timestamp, '%Y-%m-%d %H:%M:%S')
        forecast_dt = forecast_dt.replace(tzinfo=UTC)
    else:
        forecast_dt = data_timestamp
    
    current_dt = datetime.now(UTC)

    json_out = {
        "metadata": {
            "generated_at": current_dt.isoformat().replace('+00:00', 'Z'),
            "forecast_timestamp": forecast_dt.isoformat().replace('+00:00', 'Z'),
            "max_distance_km": MAX_DISTANCE_KM,
            "method": "constrained_idw"
        },
        "districts": results.to_dict(orient="records")
    }

    with open(output_file, "w") as f:
        json.dump(json_out, f, indent=2)

    print(f"Saved JSON: {output_file}")


# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dust-db", required=True)
    parser.add_argument("--shapefile", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--store-db", required=True)
    args = parser.parse_args()

    print("=" * 60)
    print("IRAQ REAL-TIME PM10 (GRID-AWARE)")
    print("=" * 60)

    dust_data, ts, _ = get_current_time_data(args.dust_db)
    if dust_data is None:
        return

    districts = load_iraq_districts(args.shapefile)
    results = interpolate_all_districts(districts, dust_data)

    export_results(results, ts, args.output)
    store_hourly_results_sqlite(results, ts, args.store_db)

    valid = results["dust_final"].dropna()
    print(f"Districts with data: {len(valid)}")
    print(f"PM10 range: {valid.min():.1f} – {valid.max():.1f} µg/m³")
    print("=" * 60)


if __name__ == "__main__":
    main()
