#!/usr/bin/env python3
"""
prepare_dust_history.py
Prepare historical dust/PM10 data for visualization
Optimized for Iraq region only
Creates year-separated JSON files with hierarchical IDs
"""

import sqlite3
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
import argparse
import time

# ============================================================
# CONFIGURATION
# ============================================================
YEARS = [2020, 2021, 2022, 2023, 2024, 2025]
OUTPUT_DIR = 'data/historical'

# Iraq bounding box (approximate)
IRAQ_BBOX = {
    'min_lat': 29.0,   # Southern Iraq
    'max_lat': 38.0,   # Northern Iraq
    'min_lon': 38.0,   # Western Iraq
    'max_lon': 49.0    # Eastern Iraq
}

# ============================================================
# DATABASE ACCESS
# ============================================================
def get_district_list(shapefile_path):
    """Get list of districts from shapefile with hierarchical IDs"""
    try:
        import fiona
        from shapely.geometry import shape
        
        print(f"Loading districts from {shapefile_path}...")
        districts = []
        
        # Track provinces to create consistent IDs
        provinces = {}
        province_counter = 1
        
        with fiona.open(shapefile_path) as src:
            for i, feature in enumerate(src):
                geom = shape(feature['geometry'])
                props = feature['properties']
                
                # Get province info
                province_name = props.get('NAME_1', props.get('PROVINCE', props.get('province', 'Unknown')))
                
                # Create or get province ID
                if province_name not in provinces:
                    province_id = f"P{province_counter:02d}"
                    provinces[province_name] = {
                        'id': province_id,
                        'name': province_name,
                        'districts': []
                    }
                    province_counter += 1
                else:
                    province_id = provinces[province_name]['id']
                
                # Get district info
                district_name = props.get('NAME_2', props.get('NAME', props.get('name', 'Unknown')))
                district_id = f"D{len(districts)+1:03d}"  # Create consistent district ID
                
                # Get centroid
                centroid = geom.centroid
                
                # Only include if within Iraq bounds
                if (IRAQ_BBOX['min_lat'] <= centroid.y <= IRAQ_BBOX['max_lat'] and
                    IRAQ_BBOX['min_lon'] <= centroid.x <= IRAQ_BBOX['max_lon']):
                    
                    district_data = {
                        'id': district_id,
                        'name': district_name,
                        'province_id': province_id,
                        'province_name': province_name,
                        'lat': float(centroid.y),
                        'lon': float(centroid.x),
                        'area_km2': float(geom.area * 111 * 111) if geom.area else None
                    }
                    districts.append(district_data)
                    
                    # Add to province's districts list
                    provinces[province_name]['districts'].append({
                        'id': district_id,
                        'name': district_name
                    })
        
        print(f"Loaded {len(districts)} districts across {len(provinces)} provinces")
        return pd.DataFrame(districts), provinces
        
    except Exception as e:
        print(f"Error loading shapefile: {e}")
        print("Using default Iraq districts with hierarchical IDs...")
        
        # Fallback to major cities with hierarchical IDs
        default_provinces = {
            'Baghdad': {'id': 'P01', 'districts': []},
            'Basra': {'id': 'P02', 'districts': []},
            'Nineveh': {'id': 'P03', 'districts': []},
            'Erbil': {'id': 'P04', 'districts': []},
            'Kirkuk': {'id': 'P05', 'districts': []},
            'Najaf': {'id': 'P06', 'districts': []},
            'Karbala': {'id': 'P07', 'districts': []}
        }
        
        default_districts = [
            {'id': 'D001', 'name': 'Baghdad', 'province_id': 'P01', 'province_name': 'Baghdad', 'lat': 33.3152, 'lon': 44.3661},
            {'id': 'D002', 'name': 'Basra', 'province_id': 'P02', 'province_name': 'Basra', 'lat': 30.5081, 'lon': 47.7836},
            {'id': 'D003', 'name': 'Mosul', 'province_id': 'P03', 'province_name': 'Nineveh', 'lat': 36.3400, 'lon': 43.1300},
            {'id': 'D004', 'name': 'Erbil', 'province_id': 'P04', 'province_name': 'Erbil', 'lat': 36.1900, 'lon': 44.0100},
            {'id': 'D005', 'name': 'Kirkuk', 'province_id': 'P05', 'province_name': 'Kirkuk', 'lat': 35.4700, 'lon': 44.3900},
            {'id': 'D006', 'name': 'Najaf', 'province_id': 'P06', 'province_name': 'Najaf', 'lat': 32.0000, 'lon': 44.3300},
            {'id': 'D007', 'name': 'Karbala', 'province_id': 'P07', 'province_name': 'Karbala', 'lat': 32.6100, 'lon': 44.0800},
        ]
        
        # Update province districts
        for d in default_districts:
            province_name = d['province_name']
            if province_name in default_provinces:
                default_provinces[province_name]['districts'].append({
                    'id': d['id'],
                    'name': d['name']
                })
        
        return pd.DataFrame(default_districts), default_provinces

def load_year_data(db_path, year):
    """Load data for a specific year database, filtered to Iraq region"""
    if not os.path.exists(db_path):
        print(f"Warning: Database not found: {db_path}")
        return None
    
    try:
        conn = sqlite3.connect(db_path)
        
        # Check what tables exist
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        # Use the correct table name
        table_name = 'dust_measurements'
        
        # Verify the table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if not cursor.fetchone():
            print(f"Table '{table_name}' not found.")
            conn.close()
            return None
        
        # Get column names
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Map column names (adjust if needed)
        timestamp_col = 'timestamp_utc' if 'timestamp_utc' in columns else 'timestamp'
        lat_col = 'latitude' if 'latitude' in columns else 'lat'
        lon_col = 'longitude' if 'longitude' in columns else 'lon'
        value_col = 'dust_concentration_ugm3' if 'dust_concentration_ugm3' in columns else 'value'
        
        # Query with Iraq bounding box filter
        query = f"""
        SELECT {timestamp_col} as timestamp, 
               {lat_col} as latitude, 
               {lon_col} as longitude, 
               {value_col} as value
        FROM {table_name}
        WHERE latitude BETWEEN ? AND ?
          AND longitude BETWEEN ? AND ?
        ORDER BY timestamp
        """
        
        print(f"Filtering for Iraq region...")
        
        # Execute with bounding box
        df = pd.read_sql_query(
            query, 
            conn, 
            params=(IRAQ_BBOX['min_lat'], IRAQ_BBOX['max_lat'],
                   IRAQ_BBOX['min_lon'], IRAQ_BBOX['max_lon'])
        )
        
        conn.close()
        
        if df.empty:
            print(f"No Iraq data in {year}")
            return None
        
        # Convert timestamp
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Add date components
        df['year'] = df['timestamp'].dt.year
        df['month'] = df['timestamp'].dt.month
        df['week'] = df['timestamp'].dt.isocalendar().week
        df['day'] = df['timestamp'].dt.day
        df['day_of_year'] = df['timestamp'].dt.dayofyear
        df['quarter'] = df['timestamp'].dt.quarter
        df['date'] = df['timestamp'].dt.date
        
        return df
        
    except Exception as e:
        print(f"Error loading {db_path}: {e}")
        return None

def interpolate_to_districts(grid_df, districts_df):
    """Interpolate grid data to district centroids using IDW"""
    try:
        from scipy.spatial import cKDTree
        use_scipy = True
    except ImportError:
        use_scipy = False
    
    if grid_df is None or len(grid_df) == 0:
        return [None] * len(districts_df)
    
    if use_scipy:
        # Fast method using KD-tree
        grid_coords = np.column_stack([
            grid_df['longitude'].values,
            grid_df['latitude'].values
        ])
        grid_values = grid_df['value'].values
        
        tree = cKDTree(grid_coords)
        k_nearest = min(8, len(grid_df))
        max_distance = 0.5  # degrees (~55km)
        
        results = []
        for _, district in districts_df.iterrows():
            point = np.array([district['lon'], district['lat']])
            distances, indices = tree.query(point, k=k_nearest)
            
            # Handle single point case
            if k_nearest == 1:
                distances = [distances]
                indices = [indices]
            
            # Filter by distance
            valid_distances = []
            valid_indices = []
            for d, idx in zip(distances, indices):
                if d <= max_distance:
                    valid_distances.append(d)
                    valid_indices.append(idx)
            
            if not valid_indices:
                results.append(None)
                continue
            
            # IDW interpolation
            valid_distances = np.array(valid_distances)
            if len(valid_indices) == 1:
                value = grid_values[valid_indices[0]]
            else:
                valid_distances[valid_distances == 0] = 0.001
                weights = 1.0 / (valid_distances ** 2)
                weights /= weights.sum()
                value = np.sum(grid_values[valid_indices] * weights)
            
            results.append(float(value))
        
        return results
    
    else:
        # Simple average as fallback
        results = []
        for _, district in districts_df.iterrows():
            nearby = grid_df[
                (abs(grid_df['latitude'] - district['lat']) <= 0.5) &
                (abs(grid_df['longitude'] - district['lon']) <= 0.5)
            ]
            
            if len(nearby) > 0:
                results.append(float(nearby['value'].mean()))
            else:
                results.append(None)
        
        return results

# ============================================================
# DATA AGGREGATION
# ============================================================
def aggregate_by_period(df, districts_df, period='weekly'):
    """Aggregate interpolated data by day, week, or month"""
    
    if df is None or len(df) == 0:
        return None
    
    # Get unique timestamps
    timestamps = df['timestamp'].unique()
    print(f"Processing {len(timestamps)} timestamps...")
    
    # Process in chunks
    chunk_size = 50
    all_interpolated = []
    
    for chunk_idx in range(0, len(timestamps), chunk_size):
        chunk_timestamps = timestamps[chunk_idx:chunk_idx + chunk_size]
        
        for ts in chunk_timestamps:
            ts_df = df[df['timestamp'] == ts]
            values = interpolate_to_districts(ts_df, districts_df)
            
            if any(v is not None for v in values):
                row = {
                    'timestamp': ts,
                    'year': ts.year,
                    'month': ts.month,
                    'week': ts.isocalendar().week,
                    'day': ts.day,
                    'day_of_year': ts.timetuple().tm_yday,
                    'quarter': (ts.month - 1) // 3 + 1,
                    'date': ts.date().isoformat()
                }
                for idx, district in districts_df.iterrows():
                    if values[idx] is not None:
                        row[district['id']] = values[idx]  # Use district ID as key
                all_interpolated.append(row)
    
    if not all_interpolated:
        return None
    
    result_df = pd.DataFrame(all_interpolated)
    result_df['timestamp'] = pd.to_datetime(result_df['timestamp'])
    
    # Get district ID columns
    district_cols = [col for col in result_df.columns if col.startswith('D')]
    
    # Aggregate by specified period
    if period == 'daily':
        period_agg = []
        for date, group in result_df.groupby('date'):
            first_row = group.iloc[0]
            row = {
                'date': date,
                'year': first_row['year'],
                'month': first_row['month'],
                'week': first_row['week'],
                'day': first_row['day'],
                'day_of_year': first_row['day_of_year'],
                'quarter': first_row['quarter'],
                'period': date
            }
            
            for col in district_cols:
                values = group[col].dropna()
                if len(values) > 0:
                    row[col] = values.median()
            
            period_agg.append(row)
        
        period_agg = pd.DataFrame(period_agg)
        
    elif period == 'weekly':
        period_agg = []
        for (year, week), group in result_df.groupby(['year', 'week']):
            row = {
                'year': year, 
                'week': week,
                'period': f"{int(year)}-W{int(week):02d}"
            }
            
            for col in district_cols:
                values = group[col].dropna()
                if len(values) > 0:
                    row[col] = values.median()
            
            period_agg.append(row)
        
        period_agg = pd.DataFrame(period_agg)
        
    else:  # monthly
        period_agg = []
        for (year, month), group in result_df.groupby(['year', 'month']):
            row = {
                'year': year, 
                'month': month,
                'quarter': (month - 1) // 3 + 1,
                'period': f"{int(year)}-{int(month):02d}"
            }
            
            for col in district_cols:
                values = group[col].dropna()
                if len(values) > 0:
                    row[col] = values.median()
            
            period_agg.append(row)
        
        period_agg = pd.DataFrame(period_agg)
    
    return period_agg

# ============================================================
# JSON EXPORT - PER YEAR
# ============================================================
def export_year_data(year_data, year, districts_df, provinces, output_dir, period):
    """Export data for a single year to separate JSON files"""
    
    year_dir = os.path.join(output_dir, str(year))
    os.makedirs(year_dir, exist_ok=True)
    
    if year_data is None or len(year_data) == 0:
        print(f"  No data for year {year}")
        return False
    
    # Prepare time series data for this year
    time_series = []
    district_cols = [col for col in year_data.columns if col.startswith('D')]
    
    for _, row in year_data.iterrows():
        period_data = {
            'period': row['period'],
            'year': int(row['year'])
        }
        
        # Add time components
        if 'month' in row:
            period_data['month'] = int(row['month'])
        if 'week' in row:
            period_data['week'] = int(row['week'])
        if 'day' in row:
            period_data['day'] = int(row['day'])
        if 'quarter' in row:
            period_data['quarter'] = int(row['quarter'])
        if 'date' in row:
            period_data['date'] = str(row['date'])
        
        # Add district values
        for col in district_cols:
            if col in row and not pd.isna(row[col]):
                period_data[col] = float(row[col])
        
        time_series.append(period_data)
    
    if time_series:
        # Save full version
        full_path = os.path.join(year_dir, f'{period}.json')
        with open(full_path, 'w') as f:
            json.dump(time_series, f, indent=2)
        
        # Create compact version (optimized for web)
        compact_series = []
        for item in time_series:
            compact_item = {
                'p': item['period'],  # period
                'y': item['year']      # year
            }
            
            # Add time components
            if 'month' in item:
                compact_item['m'] = item['month']
            if 'week' in item:
                compact_item['w'] = item['week']
            if 'day' in item:
                compact_item['d'] = item['day']
            if 'quarter' in item:
                compact_item['q'] = item['quarter']
            
            # Add district values (rounded)
            for key, val in item.items():
                if key.startswith('D') and val is not None:
                    compact_item[key] = round(val, 1)
            
            compact_series.append(compact_item)
        
        compact_path = os.path.join(year_dir, f'{period}_compact.json')
        with open(compact_path, 'w') as f:
            json.dump(compact_series, f)
        
        # Create province-aggregated version
        province_series = []
        for item in time_series:
            province_item = {
                'period': item['period'],
                'year': item['year']
            }
            
            # Add time components
            if 'month' in item:
                province_item['month'] = item['month']
            if 'week' in item:
                province_item['week'] = item['week']
            
            # Aggregate by province
            province_values = {}
            for province_id, province_info in provinces.items():
                # Find all districts in this province
                district_ids = [d['id'] for d in province_info['districts']]
                values = []
                
                for dist_id in district_ids:
                    if dist_id in item and item[dist_id] is not None:
                        values.append(item[dist_id])
                
                if values:
                    province_values[province_id] = {
                        'median': float(np.median(values)),
                        'mean': float(np.mean(values)),
                        'min': float(np.min(values)),
                        'max': float(np.max(values))
                    }
            
            province_item['values'] = province_values
            province_series.append(province_item)
        
        province_path = os.path.join(year_dir, f'{period}_by_province.json')
        with open(province_path, 'w') as f:
            json.dump(province_series, f, indent=2)
        
        # File sizes
        full_size = os.path.getsize(full_path) / 1024
        compact_size = os.path.getsize(compact_path) / 1024
        
        print(f"  ✓ {year}: {len(time_series)} periods ({full_size:.1f} KB, compact: {compact_size:.1f} KB)")
        return True
    
    return False

def save_metadata(districts_df, provinces, output_dir):
    """Save district and province metadata once"""
    
    # Prepare districts list
    districts_list = []
    for _, d in districts_df.iterrows():
        districts_list.append({
            'id': d['id'],
            'name': d['name'],
            'province_id': d['province_id'],
            'province_name': d['province_name'],
            'lat': float(d['lat']),
            'lon': float(d['lon']),
            'area_km2': float(d['area_km2']) if pd.notna(d.get('area_km2')) else None
        })
    
    # Save districts metadata
    districts_path = os.path.join(output_dir, 'districts.json')
    with open(districts_path, 'w') as f:
        json.dump(districts_list, f, indent=2)
    
    # Prepare provinces list
    provinces_list = []
    for province_name, info in provinces.items():
        provinces_list.append({
            'id': info['id'],
            'name': province_name,
            'districts': info['districts'],
            'district_count': len(info['districts'])
        })
    
    # Save provinces metadata
    provinces_path = os.path.join(output_dir, 'provinces.json')
    with open(provinces_path, 'w') as f:
        json.dump(provinces_list, f, indent=2)
    
    # Create index file with available years and periods
    index_path = os.path.join(output_dir, 'index.json')
    index_data = {
        'generated': datetime.now().isoformat(),
        'provinces': len(provinces_list),
        'districts': len(districts_list),
        'years': [],
        'periods': ['daily', 'weekly', 'monthly']
    }
    
    # Check existing year directories
    if os.path.exists(output_dir):
        for item in os.listdir(output_dir):
            if item.isdigit() and len(item) == 4:  # Year directories
                year_dir = os.path.join(output_dir, item)
                if os.path.isdir(year_dir):
                    index_data['years'].append(int(item))
    
    index_data['years'].sort()
    
    with open(index_path, 'w') as f:
        json.dump(index_data, f, indent=2)
    
    print(f"\nMetadata saved:")
    print(f"  - {len(districts_list)} districts to districts.json")
    print(f"  - {len(provinces_list)} provinces to provinces.json")
    print(f"  - Available years: {index_data['years']}")

# ============================================================
# MAIN
# ============================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--shapefile', required=True, help='Path to Iraq districts shapefile')
    parser.add_argument('--data-dir', required=True, help='Directory containing year databases')
    parser.add_argument('--output-dir', default=OUTPUT_DIR, help='Output directory for JSON files')
    parser.add_argument('--period', choices=['daily', 'weekly', 'monthly'], required=True)
    parser.add_argument('--years', nargs='+', type=int, help='Years to process (optional, processes all available if not specified)')
    args = parser.parse_args()
    
    print("=" * 60)
    print("PREPARE IRAQ HISTORICAL DUST DATA")
    print("=" * 60)
    print(f"Iraq bounds: {IRAQ_BBOX}")
    print(f"Aggregation period: {args.period}")
    print("")
    
    # Load districts and provinces
    print("Loading Iraqi districts...")
    districts_df, provinces = get_district_list(args.shapefile)
    print(f"Loaded {len(districts_df)} districts across {len(provinces)} provinces")
    print("")
    
    # Save metadata once (independent of year processing)
    save_metadata(districts_df, provinces, args.output_dir)
    
    # Determine years to process
    if args.years:
        years_to_process = args.years
    else:
        # Find all year databases in data-dir
        years_to_process = []
        for file in os.listdir(args.data_dir):
            if file.endswith('_dust.db'):
                try:
                    year = int(file.split('_')[0])
                    years_to_process.append(year)
                except:
                    pass
        years_to_process.sort()
    
    print(f"\nYears to process: {years_to_process}")
    print("")
    
    # Process each year
    successful_years = []
    total_start = time.time()
    
    for year in years_to_process:
        db_path = os.path.join(args.data_dir, f"{year}_dust.db")
        print(f"\n{'-'*40}")
        print(f"PROCESSING YEAR: {year}")
        print(f"{'-'*40}")
        
        year_start = time.time()
        
        df = load_year_data(db_path, year)
        if df is not None:
            agg_df = aggregate_by_period(df, districts_df, period=args.period)
            if agg_df is not None and len(agg_df) > 0:
                success = export_year_data(agg_df, year, districts_df, provinces, args.output_dir, args.period)
                if success:
                    successful_years.append(year)
            else:
                print(f"  ✗ No data after aggregation")
        else:
            print(f"  ✗ No data found")
        
        year_time = time.time() - year_start
        print(f"  Time: {year_time:.1f} seconds")
    
    # Update index with successful years
    if successful_years:
        index_path = os.path.join(args.output_dir, 'index.json')
        if os.path.exists(index_path):
            with open(index_path, 'r') as f:
                index_data = json.load(f)
            index_data['years'] = sorted(list(set(index_data['years'] + successful_years)))
            index_data['last_updated'] = datetime.now().isoformat()
            with open(index_path, 'w') as f:
                json.dump(index_data, f, indent=2)
    
    total_time = time.time() - total_start
    print("\n" + "="*60)
    print(f"COMPLETE! Total time: {total_time:.1f} seconds")
    print("="*60)
    print(f"Output directory: {args.output_dir}")
    print(f"\nDirectory structure:")
    print(f"  {args.output_dir}/")
    print(f"  ├── districts.json")
    print(f"  ├── provinces.json")
    print(f"  ├── index.json")
    for year in successful_years:
        print(f"  ├── {year}/")
        print(f"  │   ├── {args.period}.json")
        print(f"  │   ├── {args.period}_compact.json")
        print(f"  │   └── {args.period}_by_province.json")
    print(f"\nSuccessfully processed {len(successful_years)} years: {successful_years}")

if __name__ == "__main__":
    main()