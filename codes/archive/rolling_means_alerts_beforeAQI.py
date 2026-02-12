
#!/usr/bin/env python3
"""
rolling_means_alerts_fixed_rounding.py
Compute rolling PM10 means + alert levels for Iraq districts
FIXED VERSION with consistent time rounding
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta, UTC
import argparse
import json
import os

# ============================================================
# CONFIG
# ============================================================
DATA_RESOLUTION_HOURS = 3  # Data is at 3-hour intervals
ROLLING_WINDOWS = [6, 12, 24]  # Rolling windows in hours

PM10_THRESHOLDS = {
    "good": 0,
    "moderate": 50,
    "unhealthy": 100,
    "very_unhealthy": 150,
    "hazardous": 300
}

# ============================================================
# ALERT CLASSIFICATION
# ============================================================
def classify_pm10(value):
    if value is None:
        return "no_data"
    
    value = float(value)
    
    if value < PM10_THRESHOLDS["moderate"]:
        return "good"
    elif value < PM10_THRESHOLDS["unhealthy"]:
        return "moderate"
    elif value < PM10_THRESHOLDS["very_unhealthy"]:
        return "unhealthy"
    elif value < PM10_THRESHOLDS["hazardous"]:
        return "very_unhealthy"
    else:
        return "hazardous"

# ============================================================
# FORMAT TIMESTAMP CONSISTENTLY
# ============================================================
def format_timestamp_iso(dt):
    """Format datetime to ISO format with Z timezone"""
    if pd.isna(dt) or dt is None:
        return None
    
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()
    
    if isinstance(dt, datetime):
        # Ensure UTC timezone
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        elif dt.tzinfo != UTC:
            dt = dt.astimezone(UTC)
        
        # Format as ISO with Z suffix
        return dt.isoformat().replace('+00:00', 'Z')
    
    return str(dt)

# ============================================================
# ROUND DOWN TO 3-HOUR INTERVAL
# ============================================================
# ============================================================
# ROUND TO NEAREST 3-HOUR INTERVAL
# ============================================================
def round_to_nearest_3hour(dt):
    """Round datetime to NEAREST 3-hour interval (00:00, 03:00, etc.)"""
    hour = dt.hour
    minute = dt.minute
    
    # Calculate which 3-hour block we're in
    remainder = hour % DATA_RESOLUTION_HOURS
    
    # Calculate total minutes into the 3-hour block
    minutes_into_block = remainder * 60 + minute
    
    # If we're in the second half of the 3-hour block (>= 90 minutes),
    # round UP to next 3-hour mark
    if minutes_into_block >= 90:
        # Round up
        rounded_hour = hour + (DATA_RESOLUTION_HOURS - remainder)
    else:
        # Round down
        rounded_hour = hour - remainder
    
    # Handle hour overflow (if rounding up past 24:00)
    if rounded_hour >= 24:
        dt = dt + timedelta(days=1)
        rounded_hour = rounded_hour - 24
    
    return dt.replace(hour=rounded_hour, minute=0, second=0, microsecond=0)

# ============================================================
# GET REFERENCE TIME (ROUND TO NEAREST 3-HOUR MARK)
# ============================================================
def get_reference_time():
    """Get the reference time - round to NEAREST 3-hour mark"""
    now = datetime.now(UTC)
    ref_time = round_to_nearest_3hour(now)
    
    print(f"Current time: {now}")
    print(f"Reference time (rounded to NEAREST 3-hour interval): {ref_time}")
    return ref_time

# ============================================================
# CLEAN AND PREPARE DATA
# ============================================================
def clean_and_prepare_data(df):
    """Clean and prepare data with consistent timestamps"""
    print(f"DEBUG: Raw data shape: {df.shape}")
    
    # Convert timestamps
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    
    # Round timestamps DOWN to 3-hour intervals
    df["timestamp_rounded"] = df["timestamp_utc"].apply(round_to_nearest_3hour)
    
    # Group by district and rounded timestamp, take the mean if multiple points
    df = df.groupby(["district_id", "district_name", "province_name", 
                     "latitude", "longitude", "timestamp_rounded"]).agg({
        "pm10": "mean",
        "points_used": "first",
        "avg_distance_km": "first",
        "method": "first"
    }).reset_index()
    
    df = df.rename(columns={"timestamp_rounded": "timestamp_utc"})
    
    # Sort by district and timestamp
    df = df.sort_values(["district_id", "timestamp_utc"])
    
    print(f"DEBUG: After 3-hour grouping, shape: {df.shape}")
    print(f"DEBUG: Timestamp range: {df['timestamp_utc'].min()} to {df['timestamp_utc'].max()}")
    
    return df

# ============================================================
# COMPUTE METRICS FOR 3-HOUR DATA
# ============================================================
def compute_metrics(df, reference_time):
    """Compute metrics for 3-hour interval data"""
    print(f"\nDEBUG: Reference time for calculations: {reference_time}")
    
    districts = []
    
    for did, g in df.groupby("district_id"):
        g = g.sort_values("timestamp_utc")
        
        # Find data point at the reference time (exact match)
        current_data = g[g["timestamp_utc"] == reference_time]
        
        if len(current_data) == 0:
            # If no exact match, find closest data point within ±1.5 hours
            g['time_diff'] = abs(g["timestamp_utc"] - reference_time)
            min_diff = g['time_diff'].min()
            
            if min_diff <= timedelta(hours=1.5):
                # Use closest point within tolerance
                current_row = g.loc[g['time_diff'].idxmin()]
                print(f"  District {did}: Using closest timestamp {current_row['timestamp_utc']} (diff: {min_diff})")
            else:
                # Use most recent past data
                past_data = g[g["timestamp_utc"] < reference_time]
                if len(past_data) > 0:
                    current_row = past_data.iloc[-1]
                    print(f"  District {did}: Using most recent past timestamp {current_row['timestamp_utc']}")
                else:
                    # No past data, skip this district
                    print(f"  District {did}: No suitable data")
                    continue
        else:
            current_row = current_data.iloc[0]
            print(f"  District {did}: Found exact match at {current_row['timestamp_utc']}")
        
        # Current PM10
        current_pm10 = float(current_row["pm10"])
        
        pm = {
            "now": current_pm10,
            "timestamp": format_timestamp_iso(current_row["timestamp_utc"])
        }
        
        # Compute rolling means for each window
        for w in ROLLING_WINDOWS:
            # Calculate cutoff time
            cutoff_time = reference_time - timedelta(hours=w)
            
            # Get data within the window
            window_data = g[
                (g["timestamp_utc"] >= cutoff_time) & 
                (g["timestamp_utc"] <= reference_time)
            ]
            
            if len(window_data) > 0:
                mean_value = float(window_data["pm10"].mean())
                pm[f"mean_{w}h"] = mean_value
                pm[f"mean_{w}h_points"] = len(window_data)
                pm[f"mean_{w}h_start"] = format_timestamp_iso(window_data["timestamp_utc"].min())
            else:
                pm[f"mean_{w}h"] = None
                pm[f"mean_{w}h_points"] = 0
                pm[f"mean_{w}h_start"] = None
        
        # Alert logic - use longest available mean
        alert_value = None
        alert_basis = "now"
        
        for w in sorted(ROLLING_WINDOWS, reverse=True):
            if pm.get(f"mean_{w}h") is not None and pm.get(f"mean_{w}h_points", 0) > 0:
                alert_value = pm[f"mean_{w}h"]
                alert_basis = f"mean_{w}h"
                break
        
        if alert_value is None:
            alert_value = current_pm10
            alert_basis = "now"
        
        alert_level = classify_pm10(alert_value)
        
        districts.append({
            "district_id": int(current_row["district_id"]),
            "district_name": current_row["district_name"],
            "province_name": current_row["province_name"],
            "latitude": float(current_row["latitude"]),
            "longitude": float(current_row["longitude"]),
            "pm10": pm,
            "alert": {
                "level": alert_level,
                "based_on": alert_basis,
                "value": alert_value
            }
        })
    
    return districts

# ============================================================
# EXPORT
# ============================================================
def export_json(districts, output_file, reference_time):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    current_time = datetime.now(UTC)
    
    out = {
        "metadata": {
            "generated_at": format_timestamp_iso(current_time),
            "reference_time": format_timestamp_iso(reference_time),
            "rolling_windows_hours": ROLLING_WINDOWS,
            "data_resolution_hours": DATA_RESOLUTION_HOURS,
            "note": "Data is at 3-hour resolution. Means use available data points within window."
        },
        "districts": districts
    }
    
    with open(output_file, "w") as f:
        json.dump(out, f, indent=2)
    
    print(f"\nSaved alerts JSON: {output_file}")

# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--store-db", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    
    print("=" * 60)
    print("IRAQ PM10 ROLLING MEANS & ALERTS (3-HOUR DATA)")
    print("=" * 60)
    
    # Load data from database
    conn = sqlite3.connect(args.store_db)
    df = pd.read_sql_query("SELECT * FROM district_pm10_hourly", conn)
    conn.close()
    
    if df.empty:
        print("No data found")
        return
    
    # Clean and prepare data (round to 3-hour intervals)
    df = clean_and_prepare_data(df)
    
    if df.empty:
        print("No valid data after cleaning")
        return
    
    # Get reference time (always round DOWN)
    reference_time = get_reference_time()
    
    # Compute metrics
    districts = compute_metrics(df, reference_time)
    
    # Export JSON
    export_json(districts, args.output, reference_time)
    
    # Print summary
    print(f"\nSummary:")
    print(f"Districts processed: {len(districts)}")
    print(f"Reference time: {format_timestamp_iso(reference_time)}")
    print(f"Alert distribution:")
    
    alert_counts = {}
    for d in districts:
        level = d["alert"]["level"]
        alert_counts[level] = alert_counts.get(level, 0) + 1
    
    for level, count in alert_counts.items():
        print(f"  {level}: {count} districts")
    
    print("=" * 60)

if __name__ == "__main__":
    main()
