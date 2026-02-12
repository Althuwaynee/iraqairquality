# Get data for specific date
#		python query_dust_database.py --date 2025-09-15

# Get time series for Baghdad (33.3°N, 44.4°E)
#		python query_dust_database.py --location 33.3 44.4 --timeseries --start 2025-09-01 --end 2025-09-30

# Show database statistics
#		python query_dust_database.py --stats





#!/usr/bin/env python3
"""
query_dust_database.py
Query and analyze dust concentration database
"""

import sqlite3
import pandas as pd
import argparse
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import numpy as np

def connect_db(db_path='databases/2026_dust.db'):
    """Connect to database"""
    return sqlite3.connect(db_path)

def query_by_date(db_conn, date_str, hour=None):
    """Query data for specific date"""
    if hour is not None:
        date_filter = f"timestamp_utc LIKE '{date_str} {hour:02d}:%'"
    else:
        date_filter = f"timestamp_utc LIKE '{date_str}%'"
    
    query = f'''
    SELECT timestamp_utc, latitude, longitude, dust_concentration_ugm3
    FROM dust_measurements
    WHERE {date_filter}
    ORDER BY timestamp_utc, latitude, longitude
    '''
    
    return pd.read_sql_query(query, db_conn)

def query_by_location(db_conn, lat, lon, radius_km=50):
    """Query data for specific location (approximate)"""
    # Approximate conversion: 1 degree ≈ 111 km
    radius_deg = radius_km / 111.0
    
    query = '''
    SELECT timestamp_utc, latitude, longitude, dust_concentration_ugm3
    FROM dust_measurements
    WHERE latitude BETWEEN ? AND ?
      AND longitude BETWEEN ? AND ?
    ORDER BY timestamp_utc
    '''
    
    params = (
        lat - radius_deg,
        lat + radius_deg,
        lon - radius_deg,
        lon + radius_deg
    )
    
    return pd.read_sql_query(query, db_conn, params=params)

def query_time_series(db_conn, lat, lon, start_date, end_date):
    """Get time series for specific location"""
    query = '''
    SELECT timestamp_utc, dust_concentration_ugm3
    FROM dust_measurements
    WHERE ABS(latitude - ?) < 0.1
      AND ABS(longitude - ?) < 0.1
      AND timestamp_utc BETWEEN ? AND ?
    ORDER BY timestamp_utc
    '''
    
    params = (lat, lon, start_date, end_date)
    return pd.read_sql_query(query, db_conn, params=params)

def get_statistics(db_conn):
    """Get overall statistics"""
    query = '''
    SELECT 
        MIN(dust_concentration_ugm3) as min_val,
        MAX(dust_concentration_ugm3) as max_val,
        AVG(dust_concentration_ugm3) as avg_val,
        COUNT(*) as total_measurements
    FROM dust_measurements
    '''
    
    return pd.read_sql_query(query, db_conn)

def plot_time_series(df, title="Dust Concentration Time Series"):
    """Plot time series data"""
    if df.empty:
        print("No data to plot")
        return
    
    df['timestamp'] = pd.to_datetime(df['timestamp_utc'])
    df.set_index('timestamp', inplace=True)
    
    plt.figure(figsize=(12, 6))
    plt.plot(df.index, df['dust_concentration_ugm3'], 'b-', linewidth=1)
    plt.fill_between(df.index, df['dust_concentration_ugm3'], alpha=0.3)
    
    plt.title(title)
    plt.xlabel('Time (UTC)')
    plt.ylabel('Dust Concentration (µg/m³)')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

def main():
    parser = argparse.ArgumentParser(description='Query dust concentration database')
    parser.add_argument('--db', type=str, default='databases/2026_dust.db', help='Database file')
    parser.add_argument('--date', type=str, help='Query by date (YYYY-MM-DD)')
    parser.add_argument('--hour', type=int, help='Specific hour (0-23)')
    parser.add_argument('--location', type=float, nargs=2, metavar=('LAT', 'LON'), 
                       help='Query by location (latitude longitude)')
    parser.add_argument('--radius', type=float, default=50, help='Radius in km for location query')
    parser.add_argument('--timeseries', action='store_true', help='Get time series')
    parser.add_argument('--start', type=str, help='Start date for time series (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='End date for time series (YYYY-MM-DD)')
    parser.add_argument('--stats', action='store_true', help='Show statistics')
    parser.add_argument('--export', type=str, help='Export query results to CSV')
    
    args = parser.parse_args()
    
    conn = connect_db(args.db)
    
    try:
        if args.date:
            df = query_by_date(conn, args.date, args.hour)
            print(f"Found {len(df)} records for date {args.date}")
            if not df.empty:
                print(df.head())
                
                if args.export:
                    df.to_csv(args.export, index=False)
                    print(f"Exported to {args.export}")
        
        elif args.location and args.timeseries and args.start and args.end:
            lat, lon = args.location
            df = query_time_series(conn, lat, lon, args.start, args.end)
            print(f"Time series for location ({lat}, {lon}): {len(df)} records")
            if not df.empty:
                plot_time_series(df, f"Dust Concentration at ({lat}, {lon})")
        
        elif args.location:
            lat, lon = args.location
            df = query_by_location(conn, lat, lon, args.radius)
            print(f"Found {len(df)} records near ({lat}, {lon}) within {args.radius}km")
            if not df.empty:
                print(df.head())
        
        elif args.stats:
            df = get_statistics(conn)
            print("\nDatabase Statistics:")
            print("="*40)
            print(df.to_string(index=False))
            
            # Additional stats
            query = "SELECT COUNT(DISTINCT timestamp_utc) as unique_timestamps FROM dust_measurements"
            unique_ts = pd.read_sql_query(query, conn).iloc[0,0]
            print(f"\nUnique timestamps: {unique_ts}")
        
        else:
            print("No query specified. Use --help for options.")
    
    finally:
        conn.close()

if __name__ == '__main__':
    main()
