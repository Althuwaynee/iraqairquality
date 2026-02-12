#!/usr/bin/env python3
"""
analyze_dust_districts_final.py
Final optimized analysis of dust data for Iraq districts
"""

import sqlite3
import pandas as pd
import geopandas as gpd
import numpy as np
from shapely.geometry import Point, box
from scipy.spatial import cKDTree
from datetime import datetime, timedelta
import argparse
import json
import os
import warnings
warnings.filterwarnings('ignore')
from tqdm import tqdm
import multiprocessing as mp
from functools import partial

# Iraq bounding box with buffer
IRAQ_BBOX = {
    'lat_min': 28.0,
    'lat_max': 39.0,
    'lon_min': 38.0,
    'lon_max': 50.0
}

# AQI Classification (PM10/Dust)
AQI_THRESHOLDS = {
    'Good': (0, 54),
    'Moderate': (55, 154),
    'Unhealthy for Sensitive Groups': (155, 254),
    'Unhealthy': (255, 354),
    'Very Unhealthy': (355, 424),
    'Hazardous': (425, 504),
    'Extreme': (505, float('inf'))
}

AQI_COLORS = {
    'Good': '#00e400',
    'Moderate': '#ffff00',
    'Unhealthy for Sensitive Groups': '#ff7e00',
    'Unhealthy': '#ff0000',
    'Very Unhealthy': '#8f3f97',
    'Hazardous': '#7e0023',
    'Extreme': '#660000'
}

def create_district_databases():
    """Create databases for district analysis"""
    
    # Main database
    conn = sqlite3.connect('databases/realtime_iraq_dust.db')
    
    conn.execute('DROP TABLE IF EXISTS district_results')
    conn.execute('DROP TABLE IF EXISTS analysis_log')
    
    # Main results table
    conn.execute('''
    CREATE TABLE district_results (
        district_id TEXT PRIMARY KEY,
        district_name TEXT,
        province_name TEXT,
        centroid_lat REAL,
        centroid_lon REAL,
        
        -- Dust values from different methods
        dust_voronoi REAL,
        dust_idw REAL,
        dust_avg REAL,
        
        -- Method comparison
        method_difference REAL,
        uncertainty_score REAL,
        
        -- AQI Classification
        aqi_category TEXT,
        aqi_color TEXT,
        
        -- Spatial statistics
        num_gridpoints_used INTEGER,
        avg_distance_km REAL,
        
        -- Timestamps
        analysis_time TIMESTAMP,
        data_timestamp TIMESTAMP,
        latest_data_timestamp TIMESTAMP,
        
        -- Data source info
        dust_source_file TEXT,
        interpolation_method TEXT
    )
    ''')
    
    # Analysis log table
    conn.execute('''
    CREATE TABLE analysis_log (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
        analysis_time TIMESTAMP,
        num_districts INTEGER,
        num_dust_points INTEGER,
        avg_dust REAL,
        min_dust REAL,
        max_dust REAL,
        data_timestamp TIMESTAMP,
        processing_time_seconds REAL,
        status TEXT
    )
    ''')
    
    conn.commit()
    conn.close()
    print("Created district analysis database")

def get_latest_dust_timestamp(db_path):
    """Get the latest timestamp from dust data"""
    conn = sqlite3.connect(db_path)
    query = '''
    SELECT MAX(timestamp_utc) as latest_timestamp
    FROM dust_measurements_realtime
    '''
    result = pd.read_sql_query(query, conn)
    conn.close()
    
    if not result.empty and result.iloc[0]['latest_timestamp']:
        return result.iloc[0]['latest_timestamp']
    return datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

def get_latest_dust_data(db_path, hours_back=6, bbox=None):
    """Get latest dust data with proper timestamp tracking"""
    print(f"Loading dust data from {db_path}...")
    
    conn = sqlite3.connect(db_path)
    
    # First, get the latest timestamp
    timestamp_query = '''
    SELECT MAX(timestamp_utc) as latest_time,
           COUNT(*) as total_count
    FROM dust_measurements_realtime
    '''
    timestamp_info = pd.read_sql_query(timestamp_query, conn).iloc[0]
    latest_time = timestamp_info['latest_time']
    total_count = timestamp_info['total_count']
    
    print(f"Latest dust data timestamp: {latest_time}")
    print(f"Total dust measurements in DB: {total_count:,}")
    
    # Calculate time filter
    if hours_back > 0:
        try:
            latest_dt = datetime.strptime(latest_time, '%Y-%m-%d %H:%M:%S')
            time_filter = latest_dt - timedelta(hours=hours_back)
            time_filter_str = time_filter.strftime('%Y-%m-%d %H:%M:%S')
        except:
            time_filter_str = latest_time
    else:
        time_filter_str = latest_time
    
    # Build query
    if bbox:
        query = '''
        SELECT 
            latitude, 
            longitude, 
            dust_concentration_ugm3,
            timestamp_utc,
            source_file
        FROM dust_measurements_realtime
        WHERE timestamp_utc >= ?
          AND latitude BETWEEN ? AND ?
          AND longitude BETWEEN ? AND ?
        ORDER BY timestamp_utc DESC
        '''
        params = (
            time_filter_str,
            bbox['lat_min'], bbox['lat_max'],
            bbox['lon_min'], bbox['lon_max']
        )
    else:
        query = '''
        SELECT 
            latitude, 
            longitude, 
            dust_concentration_ugm3,
            timestamp_utc,
            source_file
        FROM dust_measurements_realtime
        WHERE timestamp_utc >= ?
        ORDER BY timestamp_utc DESC
        LIMIT 50000
        '''
        params = (time_filter_str,)
    
    # Load data
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    if df.empty:
        print("Warning: No dust data found with time filter, trying without...")
        conn = sqlite3.connect(db_path)
        if bbox:
            query = '''
            SELECT latitude, longitude, dust_concentration_ugm3,
                   timestamp_utc, source_file
            FROM dust_measurements_realtime
            WHERE latitude BETWEEN ? AND ?
              AND longitude BETWEEN ? AND ?
            LIMIT 30000
            '''
            params = (bbox['lat_min'], bbox['lat_max'], 
                     bbox['lon_min'], bbox['lon_max'])
        else:
            query = '''
            SELECT latitude, longitude, dust_concentration_ugm3,
                   timestamp_utc, source_file
            FROM dust_measurements_realtime
            LIMIT 30000
            '''
            params = ()
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
    
    # Get actual data timestamp (most recent in our selection)
    if 'timestamp_utc' in df.columns and not df.empty:
        actual_timestamp = df['timestamp_utc'].max()
        source_files = df['source_file'].dropna().unique()[:3]  # First 3 files
    else:
        actual_timestamp = latest_time
        source_files = []
    
    print(f"Loaded {len(df):,} dust measurements")
    print(f"Data timestamp range: {df['timestamp_utc'].min()[:16]} to {df['timestamp_utc'].max()[:16]}")
    print(f"Source files: {', '.join([os.path.basename(f) for f in source_files])}")
    
    # Check for unrealistic values
    if 'dust_concentration_ugm3' in df.columns:
        dust_stats = df['dust_concentration_ugm3'].describe()
        print(f"Dust concentration stats:")
        print(f"  Min: {dust_stats['min']:.2f}, Max: {dust_stats['max']:.2f}, Mean: {dust_stats['mean']:.2f}")
        
        # Warn if values seem too low
        if dust_stats['max'] < 10:
            print("WARNING: Dust values seem very low (<10 µg/m³). Check data units.")
    
    return df, actual_timestamp, source_files

def load_districts(shapefile_path):
    """Load Iraq districts shapefile"""
    print(f"Loading districts from {shapefile_path}...")
    gdf = gpd.read_file(shapefile_path)
    
    # Standardize column names
    col_map = {}
    for col in gdf.columns:
        col_lower = col.lower()
        if 'id_2' in col_lower or 'adm2' in col_lower:
            col_map[col] = 'district_id'
        elif 'name_2' in col_lower:
            col_map[col] = 'district_name'
        elif 'id_1' in col_lower or 'adm1' in col_lower:
            col_map[col] = 'province_id'
        elif 'name_1' in col_lower:
            col_map[col] = 'province_name'
    
    if col_map:
        gdf = gdf.rename(columns=col_map)
    
    # Ensure required columns exist
    if 'district_id' not in gdf.columns:
        gdf['district_id'] = [f'DIST_{i:03d}' for i in range(len(gdf))]
    if 'district_name' not in gdf.columns:
        gdf['district_name'] = ['Unknown' for _ in range(len(gdf))]
    if 'province_name' not in gdf.columns:
        gdf['province_name'] = ['Unknown' for _ in range(len(gdf))]
    
    # Calculate centroids
    gdf['centroid'] = gdf.geometry.centroid
    gdf['centroid_lat'] = gdf.centroid.y
    gdf['centroid_lon'] = gdf.centroid.x
    
    # Filter to Iraq bounding box
    gdf = gdf[
        (gdf['centroid_lat'] >= IRAQ_BBOX['lat_min']) &
        (gdf['centroid_lat'] <= IRAQ_BBOX['lat_max']) &
        (gdf['centroid_lon'] >= IRAQ_BBOX['lon_min']) &
        (gdf['centroid_lon'] <= IRAQ_BBOX['lon_max'])
    ]
    
    print(f"Loaded {len(gdf)} districts within Iraq")
    return gdf

def interpolate_idw_single(args):
    """IDW interpolation for single point"""
    lat, lon, grid_points, tree, max_radius_km, power = args
    
    # Convert radius from km to approximate degrees
    max_radius_deg = max_radius_km / 111.0
    
    # Find points within radius
    point_coord = np.array([lon, lat])
    indices = tree.query_ball_point(point_coord, max_radius_deg)
    
    if not indices:
        # No points within radius
        return None, [], []
    
    # Get nearby points
    nearby_points = grid_points.iloc[indices]
    
    # Calculate distances
    distances = []
    for _, point in nearby_points.iterrows():
        dist = haversine_distance(lat, lon, point['latitude'], point['longitude'])
        distances.append(dist)
    
    distances = np.array(distances)
    
    # Remove points too far away
    valid_mask = distances <= max_radius_km
    if not np.any(valid_mask):
        return None, [], []
    
    indices = [indices[i] for i in range(len(indices)) if valid_mask[i]]
    nearby_points = nearby_points[valid_mask]
    distances = distances[valid_mask]
    
    # Avoid division by zero
    distances[distances == 0] = 0.001
    
    # Calculate weights
    weights = 1.0 / (distances ** power)
    weights = weights / weights.sum()
    
    # Weighted average
    dust_value = np.sum(nearby_points['dust_concentration_ugm3'].values * weights)
    
    return float(dust_value), indices, weights.tolist()

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate great-circle distance in kilometers"""
    # Convert to radians
    lat1, lon1, lat2, lon2 = np.radians([lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    
    # Earth radius in kilometers
    return 6371 * 2 * np.arcsin(np.sqrt(a))

def interpolate_voronoi_single(args):
    """Voronoi interpolation for single point"""
    lat, lon, grid_points, tree = args
    
    # Find 3 nearest neighbors for triangle interpolation
    distances, indices = tree.query([lon, lat], k=3)
    
    if len(indices) < 3:
        return None, [], []
    
    # Get triangle points
    triangle_points = grid_points.iloc[indices]
    
    # Get coordinates
    A = np.array([triangle_points.iloc[0]['longitude'], triangle_points.iloc[0]['latitude']])
    B = np.array([triangle_points.iloc[1]['longitude'], triangle_points.iloc[1]['latitude']])
    C = np.array([triangle_points.iloc[2]['longitude'], triangle_points.iloc[2]['latitude']])
    P = np.array([lon, lat])
    
    # Calculate barycentric coordinates
    v0 = B - A
    v1 = C - A
    v2 = P - A
    
    d00 = np.dot(v0, v0)
    d01 = np.dot(v0, v1)
    d11 = np.dot(v1, v1)
    d20 = np.dot(v2, v0)
    d21 = np.dot(v2, v1)
    
    denom = d00 * d11 - d01 * d01
    if abs(denom) < 1e-10:
        return None, [], []
    
    v = (d11 * d20 - d01 * d21) / denom
    w = (d00 * d21 - d01 * d20) / denom
    u = 1.0 - v - w
    
    # Check if point is inside triangle
    if u < 0 or v < 0 or w < 0 or u > 1 or v > 1 or w > 1:
        # Point outside triangle, use IDW instead
        return None, [], []
    
    # Get dust values
    values = triangle_points['dust_concentration_ugm3'].values
    
    # Weighted average
    dust_value = u * values[0] + v * values[1] + w * values[2]
    
    return float(dust_value), indices.tolist(), [float(u), float(v), float(w)]

def get_aqi_category(dust_value):
    """Convert dust concentration to AQI category"""
    if dust_value is None or np.isnan(dust_value):
        return 'Unknown'
    
    for category, (low, high) in AQI_THRESHOLDS.items():
        if low <= dust_value <= high:
            return category
    
    return 'Unknown'

def save_results(districts, idw_results, voronoi_results, dust_data, 
                data_timestamp, source_files, analysis_time):
    """Save analysis results to database"""
    print("Saving results to database...")
    
    conn = sqlite3.connect('databases/realtime_iraq_dust.db')
    
    records = []
    for idx, district in tqdm(districts.iterrows(), total=len(districts), desc="Processing districts"):
        district_id = district['district_id']
        
        # Get IDW results
        idw_match = idw_results.get(district_id)
        if idw_match:
            dust_idw, idw_indices, idw_weights = idw_match
        else:
            dust_idw, idw_indices, idw_weights = None, [], []
        
        # Get Voronoi results
        voronoi_match = voronoi_results.get(district_id)
        if voronoi_match:
            dust_voronoi, voronoi_indices, voronoi_weights = voronoi_match
        else:
            dust_voronoi, voronoi_indices, voronoi_weights = None, [], []
        
        # Determine which value to use
        if dust_idw is not None and dust_voronoi is not None:
            # Both methods worked, use average
            dust_avg = (dust_idw + dust_voronoi) / 2
            method_diff = abs(dust_idw - dust_voronoi)
            uncertainty = min(method_diff / max(dust_avg, 1), 1.0)
            interpolation_method = 'both'
            
        elif dust_idw is not None:
            # Only IDW worked
            dust_avg = dust_idw
            method_diff = None
            uncertainty = 0.2  # Medium uncertainty
            interpolation_method = 'idw_only'
            
        elif dust_voronoi is not None:
            # Only Voronoi worked
            dust_avg = dust_voronoi
            method_diff = None
            uncertainty = 0.3  # Higher uncertainty
            interpolation_method = 'voronoi_only'
            
        else:
            # Both methods failed
            dust_avg = None
            method_diff = None
            uncertainty = 1.0
            interpolation_method = 'failed'
        
        # Calculate average distance to used points
        all_indices = list(set(idw_indices + voronoi_indices))
        distances = []
        
        if all_indices and len(dust_data) > 0:
            for point_idx in all_indices:
                if point_idx < len(dust_data):
                    point = dust_data.iloc[point_idx]
                    dist = haversine_distance(
                        district['centroid_lat'], district['centroid_lon'],
                        point['latitude'], point['longitude']
                    )
                    distances.append(dist)
        
        avg_distance = np.mean(distances) if distances else 0
        
        # AQI classification
        if dust_avg is not None:
            aqi_category = get_aqi_category(dust_avg)
            aqi_color = AQI_COLORS.get(aqi_category, '#CCCCCC')
        else:
            aqi_category = 'Unknown'
            aqi_color = '#CCCCCC'
        
        # Prepare record
        record = (
            district_id,
            district['district_name'][:50],
            district['province_name'][:50],
            float(district['centroid_lat']),
            float(district['centroid_lon']),
            float(dust_voronoi) if dust_voronoi is not None else None,
            float(dust_idw) if dust_idw is not None else None,
            float(dust_avg) if dust_avg is not None else None,
            float(method_diff) if method_diff is not None else None,
            float(uncertainty),
            aqi_category,
            aqi_color,
            len(all_indices),
            float(avg_distance),
            analysis_time.isoformat(),
            data_timestamp,
            data_timestamp,  # latest_data_timestamp same as data_timestamp
            ','.join([os.path.basename(f) for f in source_files[:2]]),
            interpolation_method
        )
        
        records.append(record)
    
    # Batch insert
    if records:
        conn.executemany('''
        INSERT OR REPLACE INTO district_results VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ''', records)
        conn.commit()
        print(f"Saved {len(records)} district records")
    
    # Log analysis
    if records:
        dust_values = [r[7] for r in records if r[7] is not None]  # dust_avg values
        if dust_values:
            conn.execute('''
            INSERT INTO analysis_log 
            (analysis_time, num_districts, num_dust_points, avg_dust, min_dust, max_dust, 
             data_timestamp, processing_time_seconds, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                analysis_time.isoformat(),
                len(records),
                len(dust_data),
                np.mean(dust_values),
                np.min(dust_values),
                np.max(dust_values),
                data_timestamp,
                0,  # Will be updated
                'success'
            ))
            conn.commit()
    
    conn.close()
    
    return len(records)

def export_json_output(output_file):
    """Export results to JSON format"""
    print(f"Exporting results to {output_file}...")
    
    conn = sqlite3.connect('databases/realtime_iraq_dust.db')
    
    # Get latest analysis
    query = '''
    SELECT * FROM district_results 
    WHERE dust_avg IS NOT NULL
    ORDER BY dust_avg DESC
    '''
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if df.empty:
        print("No data to export")
        return None
    
    # Create output directory
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Create GeoJSON
    features = []
    for _, row in df.iterrows():
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(row['centroid_lon']), float(row['centroid_lat'])]
            },
            "properties": {
                "district_id": row['district_id'],
                "district_name": row['district_name'],
                "province": row['province_name'],
                "dust_value": float(row['dust_avg']) if not pd.isna(row['dust_avg']) else None,
                "dust_idw": float(row['dust_idw']) if not pd.isna(row['dust_idw']) else None,
                "dust_voronoi": float(row['dust_voronoi']) if not pd.isna(row['dust_voronoi']) else None,
                "aqi_category": row['aqi_category'],
                "aqi_color": row['aqi_color'],
                "uncertainty": float(row['uncertainty_score']) if not pd.isna(row['uncertainty_score']) else None,
                "gridpoints_used": int(row['num_gridpoints_used']),
                "avg_distance_km": float(row['avg_distance_km']) if not pd.isna(row['avg_distance_km']) else None,
                "analysis_time": row['analysis_time'],
                "data_timestamp": row['data_timestamp'],
                "interpolation_method": row['interpolation_method']
            }
        }
        features.append(feature)
    
    # Statistics
    dust_values = df['dust_avg'].dropna().values
    stats = {
        "average": float(np.mean(dust_values)) if len(dust_values) > 0 else None,
        "maximum": float(np.max(dust_values)) if len(dust_values) > 0 else None,
        "minimum": float(np.min(dust_values)) if len(dust_values) > 0 else None,
        "std_dev": float(np.std(dust_values)) if len(dust_values) > 0 else None,
        "total_districts": len(features)
    }
    
    # AQI distribution
    aqi_dist = df['aqi_category'].value_counts().to_dict()
    
    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "timestamp": datetime.utcnow().isoformat(),
            "data_timestamp": df['data_timestamp'].iloc[0] if not df.empty else None,
            "statistics": stats,
            "aqi_distribution": aqi_dist,
            "total_features": len(features)
        }
    }
    
    # Save JSON
    with open(output_file, 'w') as f:
        json.dump(geojson, f, indent=2)
    
    # Also save CSV
    csv_file = output_file.replace('.json', '.csv')
    df.to_csv(csv_file, index=False)
    
    print(f"Exported {len(features)} districts to {output_file}")
    print(f"Also exported CSV to {csv_file}")
    
    return geojson

def main():
    parser = argparse.ArgumentParser(description='Final dust analysis for Iraq districts')
    parser.add_argument('--dust-db', type=str, default='databases/dust_realtime.db',
                       help='Dust database file')
    parser.add_argument('--shapefile', type=str, default='IRQ_adm/IRQ_districts.shp',
                       help='Districts shapefile')
    parser.add_argument('--hours-back', type=int, default=6,
                       help='Hours of dust data to use')
    parser.add_argument('--buffer-degrees', type=float, default=1.5,
                       help='Buffer around Iraq in degrees')
    parser.add_argument('--idw-radius-km', type=float, default=200.0,
                       help='IDW search radius in km')
    parser.add_argument('--export', type=str, required=True,
                       help='Export results to JSON file')
    parser.add_argument('--create-db', action='store_true',
                       help='Create databases (run once)')
    
    args = parser.parse_args()
    
    if args.create_db:
        create_district_databases()
        return
    
    start_time = datetime.now()
    
    # Create databases if needed
    if not os.path.exists('databases/realtime_iraq_dust.db'):
        create_district_databases()
    
    print("=" * 60)
    print("IRAQ DISTRICT DUST ANALYSIS - FINAL VERSION")
    print("=" * 60)
    print(f"Analysis started: {start_time}")
    
    # Define bounding box
    bbox = IRAQ_BBOX.copy()
    bbox['lat_min'] -= args.buffer_degrees
    bbox['lat_max'] += args.buffer_degrees
    bbox['lon_min'] -= args.buffer_degrees
    bbox['lon_max'] += args.buffer_degrees
    
    print(f"\nIraq bounding box with {args.buffer_degrees}° buffer:")
    print(f"  Latitude: {bbox['lat_min']:.2f}° to {bbox['lat_max']:.2f}°")
    print(f"  Longitude: {bbox['lon_min']:.2f}° to {bbox['lon_max']:.2f}°")
    
    # Load districts
    print("\n1. Loading districts...")
    districts = load_districts(args.shapefile)
    
    if len(districts) == 0:
        print("Error: No districts found within Iraq bounding box")
        return
    
    # Load dust data
    print("\n2. Loading dust data...")
    dust_data, data_timestamp, source_files = get_latest_dust_data(
        args.dust_db, args.hours_back, bbox
    )
    
    if dust_data.empty or len(dust_data) < 10:
        print(f"Error: Insufficient dust data ({len(dust_data)} points)")
        return
    
    # Check if dust values are realistic
    dust_mean = dust_data['dust_concentration_ugm3'].mean()
    print(f"\nDust data summary:")
    print(f"  Number of points: {len(dust_data):,}")
    print(f"  Mean concentration: {dust_mean:.2f} µg/m³")
    print(f"  Data timestamp: {data_timestamp}")
    
    if dust_mean < 1.0:
        print("\nWARNING: Dust values are very low (<1 µg/m³). This might indicate:")
        print("  1. Data is in wrong units (maybe kg/m³ instead of µg/m³)")
        print("  2. Data source has issues")
        print("  3. There's actually very little dust")
        print("\nContinuing analysis anyway...")
    
    # Build spatial index
    print("\n3. Building spatial index...")
    grid_coords = np.column_stack([
        dust_data['longitude'].values,
        dust_data['latitude'].values
    ])
    tree = cKDTree(grid_coords)
    
    # Perform IDW interpolation
    print("\n4. Performing IDW interpolation...")
    idw_args = []
    for _, district in districts.iterrows():
        idw_args.append((
            district['centroid_lat'], district['centroid_lon'],
            dust_data, tree, args.idw_radius_km, 2
        ))
    
    idw_results = {}
    with mp.Pool(processes=min(mp.cpu_count(), 4)) as pool:
        results = list(tqdm(pool.imap(interpolate_idw_single, idw_args), 
                          total=len(idw_args), desc="IDW interpolation"))
        
        for idx, result in enumerate(results):
            district_id = districts.iloc[idx]['district_id']
            idw_results[district_id] = result
    
    # Perform Voronoi interpolation
    print("\n5. Performing Voronoi interpolation...")
    voronoi_args = []
    for _, district in districts.iterrows():
        voronoi_args.append((
            district['centroid_lat'], district['centroid_lon'],
            dust_data, tree
        ))
    
    voronoi_results = {}
    with mp.Pool(processes=min(mp.cpu_count(), 4)) as pool:
        results = list(tqdm(pool.imap(interpolate_voronoi_single, voronoi_args), 
                          total=len(voronoi_args), desc="Voronoi interpolation"))
        
        for idx, result in enumerate(results):
            district_id = districts.iloc[idx]['district_id']
            voronoi_results[district_id] = result
    
    # Save results
    print("\n6. Saving results...")
    num_saved = save_results(
        districts, idw_results, voronoi_results, dust_data,
        data_timestamp, source_files, start_time
    )
    
    # Export JSON
    print("\n7. Exporting results...")
    geojson = export_json_output(args.export)
    
    # Calculate statistics
    print("\n8. Calculating final statistics...")
    conn = sqlite3.connect('databases/realtime_iraq_dust.db')
    stats_query = '''
    SELECT 
        COUNT(*) as total_districts,
        AVG(dust_avg) as avg_dust,
        MIN(dust_avg) as min_dust,
        MAX(dust_avg) as max_dust,
        AVG(uncertainty_score) as avg_uncertainty,
        COUNT(CASE WHEN aqi_category != 'Good' AND aqi_category != 'Unknown' THEN 1 END) as affected_districts
    FROM district_results
    WHERE dust_avg IS NOT NULL
    '''
    
    stats = pd.read_sql_query(stats_query, conn).iloc[0]
    conn.close()
    
    # Update processing time
    processing_time = (datetime.now() - start_time).total_seconds()
    conn = sqlite3.connect('databases/realtime_iraq_dust.db')
    conn.execute('''
    UPDATE analysis_log 
    SET processing_time_seconds = ?
    WHERE analysis_time = ?
    ''', (processing_time, start_time.isoformat()))
    conn.commit()
    conn.close()
    
    print("\n" + "=" * 60)
    print("ANALYSIS COMPLETE")
    print("=" * 60)
    print(f"Total processing time: {processing_time:.2f} seconds")
    print(f"\nSummary Statistics:")
    print(f"  Districts analyzed: {stats['total_districts']}")
    print(f"  Average dust: {stats['avg_dust']:.2f} µg/m³")
    print(f"  Minimum dust: {stats['min_dust']:.2f} µg/m³")
    print(f"  Maximum dust: {stats['max_dust']:.2f} µg/m³")
    print(f"  Average uncertainty: {stats['avg_uncertainty']:.3f}")
    print(f"  Districts with elevated dust: {stats['affected_districts']}")
    print(f"\nFiles created:")
    print(f"  Database: databases/realtime_iraq_dust.db")
    print(f"  JSON export: {args.export}")
    print(f"  CSV export: {args.export.replace('.json', '.csv')}")
    print(f"\nData source:")
    print(f"  Latest data timestamp: {data_timestamp}")
    print(f"  Source files: {', '.join([os.path.basename(f) for f in source_files[:2]])}")
    print("=" * 60)

if __name__ == '__main__':
    # Set multiprocessing start method
    try:
        mp.set_start_method('spawn', force=True)
    except:
        pass
    
    main()
