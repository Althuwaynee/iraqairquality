#!/usr/bin/env python3
"""
rolling_means_alerts_with_forecast.py
Compute rolling PM10 means + alert levels for Iraq districts
WITH FORECAST SUPPORT (3h to 24h)
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta, UTC
import argparse
import json
import os
import math

# ============================================================
# CONFIG
# ============================================================
DATA_RESOLUTION_HOURS = 3  # Data is at 3-hour intervals
ROLLING_WINDOWS = [6, 12, 24]  # Rolling windows in hours
FORECAST_WINDOWS = [3, 6, 9, 12, 15, 18, 21, 24]  # Forecast windows in hours (3-hour increments up to 24h)

PM10_THRESHOLDS = {
    "good": 0,
    "moderate": 50,
    "unhealthy": 100,
    "very_unhealthy": 150,
    "hazardous": 300
}

# ============================================================
# AQI CALCULATION FUNCTIONS
# ============================================================
def calculate_pm10_aqi(concentration):
    """Calculate PM10 AQI based on EPA standards"""
    if concentration is None:
        return None
    
    # Round to nearest integer as per EPA standards for PM10
    c = round(float(concentration))
    
    # Calculate AQI based on EPA breakpoints
    if 0 <= c <= 54:
        return round(((50 - 0) / (54 - 0)) * (c - 0) + 0)
    elif 55 <= c <= 154:
        return round(((100 - 51) / (154 - 55)) * (c - 55) + 51)
    elif 155 <= c <= 254:
        return round(((150 - 101) / (254 - 155)) * (c - 155) + 101)
    elif 255 <= c <= 354:
        return round(((200 - 151) / (354 - 255)) * (c - 255) + 151)
    elif 355 <= c <= 424:
        return round(((300 - 201) / (424 - 355)) * (c - 355) + 201)
    elif 425 <= c <= 504:
        return round(((400 - 301) / (504 - 425)) * (c - 425) + 301)
    elif 505 <= c <= 604:
        return round(((500 - 401) / (604 - 505)) * (c - 505) + 401)
    else:
        return 501  # Beyond the scale

def classify_aqi(aqi_value):
    """Classify AQI value into categories"""
    if aqi_value is None:
        return "no_data"
    
    aqi = int(aqi_value)
    
    if aqi <= 50:
        return "good"
    elif aqi <= 100:
        return "moderate"
    elif aqi <= 150:
        return "unhealthy_for_sensitive_groups"
    elif aqi <= 200:
        return "unhealthy"
    elif aqi <= 300:
        return "very_unhealthy"
    elif aqi <= 500:
        return "hazardous"
    else:
        return "beyond_index"

# ============================================================
# GOVERNMENT COMPLIANCE CHECK
# ============================================================
def check_government_compliance(pm10_mean_24h):
    """Check if PM10 24-hour mean complies with Iraq Government limit (100 μg/m³)"""
    if pm10_mean_24h is None:
        return "no_data"
    
    mean_24h = float(pm10_mean_24h)
    
    # Iraq Government daily limit for PM10: 100 μg/m³
    if mean_24h > 100:
        return "EXCEEDS_LIMIT"
    else:
        return "WITHIN_LIMIT"

# ============================================================
# ALERT CLASSIFICATION (Original PM10-based)
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
    print(f"Current time (rounded to NEAREST 3-hour interval): {ref_time}")
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
# GET FORECAST DATA FOR DISTRICT
# ============================================================
def get_forecast_for_district(g, reference_time, district_id):
    """Get forecast values for each forecast window (3h to 24h)"""
    forecast_data = {}
    
    for forecast_hours in FORECAST_WINDOWS:
        # Calculate forecast timestamp
        forecast_time = reference_time + timedelta(hours=forecast_hours)
        
        # Round forecast time to nearest 3-hour interval
        forecast_time_rounded = round_to_nearest_3hour(forecast_time)
        
        # Find exact forecast data at this timestamp
        forecast_row = g[g["timestamp_utc"] == forecast_time_rounded]
        
        if len(forecast_row) > 0:
            # Found exact forecast
            forecast_value = float(forecast_row.iloc[0]["pm10"])
            forecast_timestamp = forecast_row.iloc[0]["timestamp_utc"]
            data_source = "exact"
        else:
            # Try to find nearest forecast within ±1.5 hours
            g['time_diff'] = abs(g["timestamp_utc"] - forecast_time_rounded)
            min_diff = g['time_diff'].min()
            
            if min_diff <= timedelta(hours=1.5):
                # Use nearest forecast within tolerance
                forecast_row = g.loc[g['time_diff'].idxmin()]
                forecast_value = float(forecast_row["pm10"])
                forecast_timestamp = forecast_row["timestamp_utc"]
                data_source = "nearest"
            else:
                # No forecast data available for this window
                forecast_value = None
                forecast_timestamp = None
                data_source = "none"
        
        # Calculate AQI for forecast if available
        forecast_aqi = calculate_pm10_aqi(forecast_value) if forecast_value is not None else None
        forecast_aqi_level = classify_aqi(forecast_aqi) if forecast_aqi is not None else None
        
        forecast_data[f"pm10_forecast_{forecast_hours}h"] = {
            "value": forecast_value,
            "aqi": forecast_aqi,
            "aqi_level": forecast_aqi_level,
            "timestamp": format_timestamp_iso(forecast_timestamp),
            "data_source": data_source,
            "forecast_hours": forecast_hours
        }
    
    return forecast_data

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
        
        # Calculate AQI based on current PM10
        aqi_value = calculate_pm10_aqi(current_pm10)
        aqi_level = classify_aqi(aqi_value)
        
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
        
        # Get forecast data for this district
        forecast_data = get_forecast_for_district(g, reference_time, did)
        
        # Check government compliance using 24-hour mean
        gov_compliance = check_government_compliance(pm.get("mean_24h"))
        
        # Alert logic - now based on AQI
        alert_value = aqi_value if aqi_value is not None else current_pm10
        alert_basis = "AQI" if aqi_value is not None else "now"
        alert_level = aqi_level if aqi_value is not None else classify_pm10(current_pm10)
        
        # Create district record with forecast data
        district_record = {
            "district_id": int(current_row["district_id"]),
            "district_name": current_row["district_name"],
            "province_name": current_row["province_name"],
            "latitude": float(current_row["latitude"]),
            "longitude": float(current_row["longitude"]),
            "pm10": pm,
            "aqi": {
                "value": aqi_value,
                "level": aqi_level,
                "based_on": "pm10_now"
            },
            "government_compliance": {
                "status": gov_compliance,
                "limit_24h_ug_m3": 100,
                "pm10_mean_24h": pm.get("mean_24h")
            },
            "alert": {
                "level": alert_level,
                "based_on": alert_basis,
                "value": alert_value
            }
        }
        
        # Add forecast data to district record
        district_record.update(forecast_data)
        
        districts.append(district_record)
    
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
            "forecast_windows_hours": FORECAST_WINDOWS,
            "data_resolution_hours": DATA_RESOLUTION_HOURS,
            "note": "Data is at 3-hour resolution.",
            "aqi_calculation": "Based on EPA PM10 AQI formula",
            "government_compliance_limit": "Iraq Government daily PM10 limit: 100 μg/m³",
            "forecast_note": "Forecast values are for PM10 at specified hours from reference time"
        },
        "districts": districts
    }
    
    with open(output_file, "w") as f:
        json.dump(out, f, indent=2)
    
    print(f"\nSaved alerts JSON with forecasts: {output_file}")

# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--store-db", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    
    print("=" * 60)
    print("IRAQ PM10 ROLLING MEANS & ALERTS WITH FORECAST")
    print("3-Hour Data with Forecasts up to 24h")
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
    print(f"Forecast windows: {FORECAST_WINDOWS}")
    
    print(f"\nAlert distribution (based on AQI):")
    alert_counts = {}
    for d in districts:
        level = d["alert"]["level"]
        alert_counts[level] = alert_counts.get(level, 0) + 1
    
    for level, count in sorted(alert_counts.items()):
        print(f"  {level}: {count} districts")
    
    print(f"\nGovernment Compliance:")
    gov_counts = {}
    for d in districts:
        status = d["government_compliance"]["status"]
        gov_counts[status] = gov_counts.get(status, 0) + 1
    
    for status, count in sorted(gov_counts.items()):
        print(f"  {status}: {count} districts")
    
    # Print forecast availability summary
    print(f"\nForecast Availability Summary:")
    forecast_stats = {}
    for forecast_hours in FORECAST_WINDOWS:
        forecast_key = f"pm10_forecast_{forecast_hours}h"
        available = sum(1 for d in districts if d.get(forecast_key, {}).get("value") is not None)
        total = len(districts)
        forecast_stats[forecast_hours] = (available, total)
    
    for forecast_hours, (available, total) in forecast_stats.items():
        percentage = (available / total * 100) if total > 0 else 0
        print(f"  {forecast_hours}h forecast: {available}/{total} districts ({percentage:.1f}%)")
    
    print("=" * 60)

if __name__ == "__main__":
    main()