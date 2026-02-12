#!/usr/bin/env python3
"""
query_realtime_database.py
Query the real-time dust concentration database
"""

import sqlite3
import pandas as pd
import argparse
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

def connect_db(db_path='databases/dust_realtime.db'):
    """Connect to real-time database"""
    return sqlite3.connect(db_path)

def get_latest_data(db_conn, limit=1000):
    """Get latest dust measurements"""
    query = '''
    SELECT timestamp_utc, latitude, longitude, dust_concentration_ugm3
    FROM dust_measurements_realtime
    ORDER BY timestamp_utc DESC
    LIMIT ?
    '''
    
    df = pd.read_sql_query(query, db_conn, params=(limit,))
    
    if not df.empty:
        df['timestamp'] = pd.to_datetime(df['timestamp_utc'])
        df = df.sort_values('timestamp')
    
    return df

def get_region_summary(db_conn, lat_min, lat_max, lon_min, lon_max):
    """Get summary for a specific region"""
    query = '''
    SELECT 
        timestamp_utc,
        COUNT(*) as point_count,
        AVG(dust_concentration_ugm3) as avg_dust,
        MIN(dust_concentration_ugm3) as min_dust,
        MAX(dust_concentration_ugm3) as max_dust
    FROM dust_measurements_realtime
    WHERE latitude BETWEEN ? AND ?
      AND longitude BETWEEN ? AND ?
      AND timestamp_utc >= datetime('now', '-6 hours')
    GROUP BY timestamp_utc
    ORDER BY timestamp_utc DESC
    '''
    
    params = (lat_min, lat_max, lon_min, lon_max)
    return pd.read_sql_query(query, db_conn, params=params)

def get_spatial_coverage(db_conn):
    """Get current spatial coverage"""
    query = '''
    SELECT * FROM spatial_coverage
    ORDER BY update_id DESC
    LIMIT 1
    '''
    
    return pd.read_sql_query(query, db_conn)

def export_for_leaflet(db_conn, output_file='leaflet_data.json'):
    """Export data in Leaflet-friendly format"""
    # Get latest data
    query = '''
    SELECT latitude, longitude, dust_concentration_ugm3
    FROM dust_measurements_realtime
    WHERE timestamp_utc >= datetime('now', '-1 hour')
    '''
    
    df = pd.read_sql_query(query, db_conn)
    
    if df.empty:
        print("No recent data found")
        return None
    
    # Create GeoJSON format
    features = []
    for _, row in df.iterrows():
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row['longitude'], row['latitude']]
            },
            "properties": {
                "dust_ugm3": float(row['dust_concentration_ugm3']),
                "popup": f"Dust: {row['dust_concentration_ugm3']:.1f} µg/m³"
            }
        }
        features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "timestamp": datetime.utcnow().isoformat(),
            "total_points": len(features),
            "avg_dust": float(df['dust_concentration_ugm3'].mean())
        }
    }
    
    # Save to file
    import json
    with open(output_file, 'w') as f:
        json.dump(geojson, f, indent=2)
    
    print(f"Exported {len(features)} points to {output_file}")
    return geojson

def main():
    parser = argparse.ArgumentParser(description='Query real-time dust database')
    parser.add_argument('--db', type=str, default='databases/dust_realtime.db',
                       help='Database file')
    parser.add_argument('--latest', action='store_true',
                       help='Show latest data')
    parser.add_argument('--region', type=float, nargs=4,
                       metavar=('LAT_MIN', 'LAT_MAX', 'LON_MIN', 'LON_MAX'),
                       help='Get region summary')
    parser.add_argument('--coverage', action='store_true',
                       help='Show spatial coverage')
    parser.add_argument('--export-leaflet', type=str,
                       help='Export for Leaflet (specify output file)')
    parser.add_argument('--limit', type=int, default=100,
                       help='Limit results')
    
    args = parser.parse_args()
    
    conn = connect_db(args.db)
    
    try:
        if args.latest:
            df = get_latest_data(conn, args.limit)
            print(f"Latest {len(df)} measurements:")
            print(df.head())
            
            if not df.empty:
                print(f"\nTime range: {df['timestamp'].min()} to {df['timestamp'].max()}")
                print(f"Dust range: {df['dust_concentration_ugm3'].min():.1f} to {df['dust_concentration_ugm3'].max():.1f} µg/m³")
        
        elif args.region:
            lat_min, lat_max, lon_min, lon_max = args.region
            df = get_region_summary(conn, lat_min, lat_max, lon_min, lon_max)
            print(f"Region summary ({lat_min}°-{lat_max}°N, {lon_min}°-{lon_max}°E):")
            print(df.head())
            
            if not df.empty:
                print(f"\nLatest average: {df.iloc[0]['avg_dust']:.1f} µg/m³")
                print(f"Data points: {df.iloc[0]['point_count']}")
        
        elif args.coverage:
            df = get_spatial_coverage(conn)
            print("Current spatial coverage:")
            print(df.to_string(index=False))
        
        elif args.export_leaflet:
            result = export_for_leaflet(conn, args.export_leaflet)
            if result:
                print(f"Data ready for Leaflet. Load with:")
                print(f"  fetch('{args.export_leaflet}').then(...)")
        
        else:
            # Show basic stats
            query = "SELECT COUNT(*) as total_points FROM dust_measurements_realtime"
            total = pd.read_sql_query(query, conn).iloc[0,0]
            
            query = "SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM dust_measurements_realtime"
            min_time, max_time = conn.execute(query).fetchone()
            
            print(f"Real-time database: {total:,} points")
            print(f"Time range: {min_time} to {max_time}")
            print("\nAvailable options:")
            print("  --latest         Show latest measurements")
            print("  --region         Get region summary (lat_min lat_max lon_min lon_max)")
            print("  --coverage       Show spatial coverage")
            print("  --export-leaflet Export for Leaflet visualization")
    
    finally:
        conn.close()

if __name__ == '__main__':
    main()
