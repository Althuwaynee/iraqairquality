#!/usr/bin/env python3
"""
(pythonenv) omar@omardurham:~/Documents/Dust$ python codes/create_realtime_database_full_grid.py --input . --db databases/dust_realtime.db


create_realtime_database_full_grid.py
Create real-time dust concentration database from latest .nc files
Now also includes data from the previous .nc file from year-specific database

"""

import sqlite3
import pandas as pd
import numpy as np
import xarray as xr
import os
import glob
import argparse
from datetime import datetime, timedelta, UTC
import time
import json
from pathlib import Path
import re

def find_latest_data_folder(base_path, pattern="*latest"):
    """Find the folder containing latest real-time data"""
    try:
        # Look for folders with "latest" in their name
        folders = glob.glob(os.path.join(base_path, pattern))
        
        if not folders:
            raise FileNotFoundError(f"No folders found matching pattern: {pattern}")
        
        # Sort by modification time and get the newest
        latest_folder = max(folders, key=os.path.getmtime)
        return latest_folder
    except Exception as e:
        print(f"Error finding latest folder: {e}")
        return None

def get_nc_files(folder_path, pattern="*.nc"):
    """Get all .nc files from folder"""
    nc_files = glob.glob(os.path.join(folder_path, pattern))
    return sorted(nc_files)

def get_year_db_path(base_dir="databases"):
    """Get the path to the year-specific database (e.g., 2024_dust.db)"""
    current_year = datetime.now().year
    
    # Try current year first
    db_name = f"{current_year}_dust.db"
    db_path = os.path.join(base_dir, db_name)
    
    if os.path.exists(db_path):
        return db_path
    
    # If current year doesn't exist, look for any year database
    year_dbs = glob.glob(os.path.join(base_dir, "*_dust.db"))
    if year_dbs:
        # Get the most recent year database
        year_dbs.sort(reverse=True)
        return year_dbs[0]
    
    return None

def find_previous_nc_file_from_db(latest_nc_file, year_db_path):
    """Find the .nc file that comes before the latest one in the year database"""
    if not year_db_path or not os.path.exists(year_db_path):
        print(f"Year database not found: {year_db_path}")
        return None
    
    latest_nc_name = os.path.basename(latest_nc_file)
    
    try:
        # Connect to year database
        conn = sqlite3.connect(year_db_path)
        
        # First, check what columns exist in the dust_measurements table
        cursor = conn.execute("PRAGMA table_info(dust_measurements)")
        columns = [row[1] for row in cursor.fetchall()]
        print(f"Columns in year database: {columns}")
        
        # Determine the correct column name for source file
        source_col = None
        possible_cols = ['source_file', 'file_source', 'filename', 'file_name']
        for col in possible_cols:
            if col in columns:
                source_col = col
                break
        
        if not source_col:
            print("No source file column found in year database")
            conn.close()
            return None
        
        print(f"Using column '{source_col}' for source file")
        
        # Query for distinct file names
        query = f"SELECT DISTINCT {source_col} FROM dust_measurements WHERE {source_col} LIKE '%.nc'"
        cursor = conn.execute(query)
        db_files = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        if not db_files:
            print("No .nc files found in year database")
            return None
        
        # Sort the files (assuming they have timestamps in names)
        db_files_sorted = sorted(db_files)
        
        print(f"Found {len(db_files_sorted)} .nc files in year database")
        print(f"Latest 5 files: {db_files_sorted[-5:]}")
        
        # Find the position of latest file in database files
        if latest_nc_name in db_files_sorted:
            idx = db_files_sorted.index(latest_nc_name)
            if idx > 0:
                previous_file = db_files_sorted[idx - 1]
                print(f"Found previous file in database: {previous_file} (comes before {latest_nc_name})")
                return previous_file
            else:
                print(f"Latest file {latest_nc_name} is the first file in database")
                # Return the most recent file from database instead
                if len(db_files_sorted) > 1:
                    previous_file = db_files_sorted[-2]
                    print(f"Using second most recent file instead: {previous_file}")
                    return previous_file
                return None
        else:
            # If latest file not in database, get the most recent file
            print(f"Latest file {latest_nc_name} not found in database, using most recent from database")
            if len(db_files_sorted) >= 2:
                previous_file = db_files_sorted[-2]
                print(f"Using second most recent file: {previous_file}")
                return previous_file
            elif db_files_sorted:
                print(f"Only one file in database: {db_files_sorted[-1]}")
                return db_files_sorted[-1]
            else:
                return None
            
    except Exception as e:
        print(f"Error querying year database: {e}")
        import traceback
        traceback.print_exc()
        return None
def find_previous_nc_files_from_db(latest_nc_file, year_db_path, num_files=4):
    """Find multiple previous .nc files from the year database"""
    if not year_db_path or not os.path.exists(year_db_path):
        print(f"Year database not found: {year_db_path}")
        return []
    
    latest_nc_name = os.path.basename(latest_nc_file)
    
    try:
        # Connect to year database
        conn = sqlite3.connect(year_db_path)
        
        # First, check what columns exist in the dust_measurements table
        cursor = conn.execute("PRAGMA table_info(dust_measurements)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Determine the correct column name for source file
        source_col = None
        possible_cols = ['source_file', 'file_source', 'filename', 'file_name']
        for col in possible_cols:
            if col in columns:
                source_col = col
                break
        
        if not source_col:
            print("No source file column found in year database")
            conn.close()
            return []
        
        # Query for distinct file names
        query = f"SELECT DISTINCT {source_col} FROM dust_measurements WHERE {source_col} LIKE '%.nc'"
        cursor = conn.execute(query)
        db_files = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        if not db_files:
            print("No .nc files found in year database")
            return []
        
        # Sort the files (assuming they have timestamps in names)
        db_files_sorted = sorted(db_files)
        
        print(f"Found {len(db_files_sorted)} .nc files in year database")
        
        # Find the position of latest file in database files
        if latest_nc_name in db_files_sorted:
            idx = db_files_sorted.index(latest_nc_name)
            # Get up to num_files previous files (or as many as available)
            start_idx = max(0, idx - num_files)
            previous_files = db_files_sorted[start_idx:idx]
            
            if previous_files:
                print(f"Found {len(previous_files)} previous files (requested {num_files}):")
                for i, file in enumerate(previous_files):
                    print(f"  {i+1}. {file}")
                return previous_files
            else:
                print(f"No previous files found before {latest_nc_name}")
                return []
        else:
            # If latest file not in database, get the most recent files
            print(f"Latest file {latest_nc_name} not found in database, using most recent files from database")
            num_available = min(num_files, len(db_files_sorted))
            previous_files = db_files_sorted[-num_available:] if db_files_sorted else []
            
            if previous_files:
                print(f"Using {len(previous_files)} most recent files:")
                for i, file in enumerate(previous_files):
                    print(f"  {i+1}. {file}")
                return previous_files
            else:
                return []
            
    except Exception as e:
        print(f"Error querying year database: {e}")
        import traceback
        traceback.print_exc()
        return []
def extract_data_from_year_db(year_db_path, nc_file_name):
    """Extract data for a specific .nc file from the year database"""
    if not year_db_path or not os.path.exists(year_db_path):
        return []
    
    try:
        conn = sqlite3.connect(year_db_path)
        
        # First, check what columns exist
        cursor = conn.execute("PRAGMA table_info(dust_measurements)")
        columns = [row[1] for row in cursor.fetchall()]
        
        # Determine the correct column names
        source_col = None
        possible_cols = ['source_file', 'file_source', 'filename', 'file_name']
        for col in possible_cols:
            if col in columns:
                source_col = col
                break
        
        if not source_col:
            print("No source file column found in year database")
            conn.close()
            return []
        
        # Map column names
        time_col = 'timestamp_utc' if 'timestamp_utc' in columns else 'time'
        lat_col = 'latitude' if 'latitude' in columns else 'lat'
        lon_col = 'longitude' if 'longitude' in columns else 'lon'
        dust_col = 'dust_concentration_ugm3' if 'dust_concentration_ugm3' in columns else 'dust'
        
        # Query for data from the specific file
        query = f"""
        SELECT {time_col}, {lat_col}, {lon_col}, {dust_col}, {source_col}
        FROM dust_measurements 
        WHERE {source_col} = ?
        """
        
        print(f"Querying year database with: {query}")
        print(f"Looking for file: {nc_file_name}")
        
        cursor = conn.execute(query, (nc_file_name,))
        rows = cursor.fetchall()
        conn.close()
        
        print(f"Extracted {len(rows)} records from year database for file: {nc_file_name}")
        
        # Check a few rows
        if rows:
            print(f"Sample row: {rows[0]}")
        
        return rows
        
    except Exception as e:
        print(f"Error extracting data from year database: {e}")
        import traceback
        traceback.print_exc()
        return []

def extract_coverage_from_nc(nc_file):
    """Extract spatial and temporal coverage from .nc file"""
    try:
        with xr.open_dataset(nc_file) as ds:
            # Get spatial bounds
            lat_min = float(ds.latitude.min())
            lat_max = float(ds.latitude.max())
            lon_min = float(ds.longitude.min())
            lon_max = float(ds.longitude.max())
            
            # Get time information
            if 'time' in ds:
                times = ds.time.values
                time_min = pd.Timestamp(times.min()).strftime('%Y-%m-%d %H:%M:%S')
                time_max = pd.Timestamp(times.max()).strftime('%Y-%m-%d %H:%M:%S')
            else:
                time_min = time_max = "Unknown"
            
            # Get dust variable name (common names)
            dust_vars = [var for var in ds.variables if 'dust' in var.lower() or 'DUST' in var]
            dust_var = dust_vars[0] if dust_vars else None
            
            return {
                'file': os.path.basename(nc_file),
                'lat_min': lat_min,
                'lat_max': lat_max,
                'lon_min': lon_min,
                'lon_max': lon_max,
                'time_min': time_min,
                'time_max': time_max,
                'dust_var': dust_var,
                'size_mb': os.path.getsize(nc_file) / (1024 * 1024)
            }
    except Exception as e:
        print(f"Error reading {nc_file}: {e}")
        return None

def create_database_schema(db_conn):
    """Create database schema for real-time data"""
    
    # Main measurements table
    db_conn.execute('''
    CREATE TABLE IF NOT EXISTS dust_measurements_realtime (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp_utc TIMESTAMP NOT NULL,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        dust_concentration_ugm3 REAL,
        source_file TEXT,
        data_source TEXT DEFAULT 'latest_nc',  -- 'latest_nc' or 'year_db'
        processed_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Indexes for fast queries
    db_conn.execute('''
    CREATE INDEX IF NOT EXISTS idx_realtime_timestamp 
    ON dust_measurements_realtime(timestamp_utc)
    ''')
    
    db_conn.execute('''
    CREATE INDEX IF NOT EXISTS idx_realtime_location 
    ON dust_measurements_realtime(latitude, longitude)
    ''')
    
    db_conn.execute('''
    CREATE INDEX IF NOT EXISTS idx_realtime_dust 
    ON dust_measurements_realtime(dust_concentration_ugm3)
    ''')
    
    db_conn.execute('''
    CREATE INDEX IF NOT EXISTS idx_realtime_source 
    ON dust_measurements_realtime(source_file)
    ''')
    
    # Metadata table for tracking processed files
    db_conn.execute('''
    CREATE TABLE IF NOT EXISTS processed_files (
        file_path TEXT PRIMARY KEY,
        file_size_mb REAL,
        lat_min REAL,
        lat_max REAL,
        lon_min REAL,
        lon_max REAL,
        time_min TIMESTAMP,
        time_max TIMESTAMP,
        total_points INTEGER,
        processing_time_seconds REAL,
        data_source TEXT,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Spatial coverage summary table
    db_conn.execute('''
    CREATE TABLE IF NOT EXISTS spatial_coverage (
        update_id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        lat_min REAL,
        lat_max REAL,
        lon_min REAL,
        lon_max REAL,
        total_points INTEGER,
        avg_dust_ugm3 REAL,
        min_dust_ugm3 REAL,
        max_dust_ugm3 REAL
    )
    ''')
    
    db_conn.commit()
    print("Database schema created successfully")

def process_nc_file(nc_file, db_conn, dust_var_name=None, data_source='latest_nc'):
    """Process a single .nc file and insert into database"""
    start_time = time.time()
    
    try:
        print(f"Processing: {os.path.basename(nc_file)} (source: {data_source})")
        
        # Open the NetCDF file
        with xr.open_dataset(nc_file) as ds:
            # Try to identify dust variable
            if dust_var_name and dust_var_name in ds:
                dust_var = dust_var_name
            else:
                # Common dust variable names
                possible_names = [
                    'dust', 'DUST', 'dust_concentration', 'PM10_DUST','SCONC_DUST',
                    'dust_mass_concentration', 'DUST_UGM3'
                ]
                dust_var = None
                for name in possible_names:
                    if name in ds.variables:
                        dust_var = name
                        break
                
                if not dust_var:
                    # Try to find any variable with 'dust' in name
                    dust_vars = [var for var in ds.variables if 'dust' in var.lower()]
                    if dust_vars:
                        dust_var = dust_vars[0]
                    else:
                        print(f"Warning: No dust variable found in {nc_file}")
                        return None
            
            # Extract data
            dust_data = ds[dust_var]
            
            # Get coordinates
            if 'latitude' in ds:
                lats = ds.latitude.values
            elif 'lat' in ds:
                lats = ds.lat.values
            else:
                print(f"Warning: No latitude variable found in {nc_file}")
                return None
            
            if 'longitude' in ds:
                lons = ds.longitude.values
            elif 'lon' in ds:
                lons = ds.lon.values
            else:
                print(f"Warning: No longitude variable found in {nc_file}")
                return None
            
            # Get time
            time_values = None
            if 'time' in ds:
                time_values = ds.time.values
            elif 'TIME' in ds:
                time_values = ds.TIME.values
            
            # Prepare data for insertion
            rows_to_insert = []
            
            if time_values is not None:
                # 3D data (time, lat, lon)
                for t_idx, time_val in enumerate(time_values):
                    timestamp = pd.Timestamp(time_val).strftime('%Y-%m-%d %H:%M:%S')
                    dust_slice = dust_data[t_idx, :, :].values
                    
                    for i in range(len(lats)):
                        for j in range(len(lons)):
                            dust_value = float(dust_slice[i, j])

                            if not np.isnan(dust_value):
                                dust_ugm3 = int(dust_value * 1e9)
    
                            # Clip to reasonable range
                                dust_ugm3 = max(0, min(dust_ugm3, 32767))  # INT16 range
                                rows_to_insert.append((
                                    timestamp,
                                    float(lats[i]),
                                    float(lons[j]),
                                    dust_ugm3,
                                    os.path.basename(nc_file),
                                    data_source
                                ))
            else:
                # 2D data (lat, lon) - assume current time
                timestamp = datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')
                dust_values = dust_data.values
                
                for i in range(len(lats)):
                    for j in range(len(lons)):
                        dust_value = float(dust_values[i, j])
                        if not np.isnan(dust_value):
                            dust_ugm3 = int(dust_value * 1e9)
                            dust_ugm3 = max(0, min(dust_ugm3, 32767))  # INT16 range
                            rows_to_insert.append((
                                timestamp,
                                float(lats[i]),
                                float(lons[j]),
                                dust_ugm3,
                                os.path.basename(nc_file),
                                data_source
                            ))
            
            # Batch insert
            if rows_to_insert:
                db_conn.executemany('''
                INSERT INTO dust_measurements_realtime 
                (timestamp_utc, latitude, longitude, dust_concentration_ugm3, source_file, data_source)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', rows_to_insert)
                
                db_conn.commit()
                
                processing_time = time.time() - start_time
                
                # Record metadata
                db_conn.execute('''
                INSERT INTO processed_files 
                (file_path, file_size_mb, total_points, processing_time_seconds, data_source)
                VALUES (?, ?, ?, ?, ?)
                ''', (
                    nc_file,
                    os.path.getsize(nc_file) / (1024 * 1024),
                    len(rows_to_insert),
                    processing_time,
                    data_source
                ))
                
                print(f"  Added {len(rows_to_insert)} points in {processing_time:.2f}s")
                return len(rows_to_insert)
            else:
                print(f"  No valid data points found")
                return 0
                
    except Exception as e:
        print(f"Error processing {nc_file}: {e}")
        import traceback
        traceback.print_exc()
        return None

def insert_data_from_year_db(db_conn, year_db_data, source_file_name):
    """Insert data extracted from year database into realtime database"""
    if not year_db_data:
        return 0
    
    try:
        start_time = time.time()
        rows_inserted = 0
        
        # Prepare data for insertion (add data_source column)
        rows_to_insert = []
        for row in year_db_data:
            # row structure: (timestamp_utc, latitude, longitude, dust_concentration_ugm3, source_file)
            rows_to_insert.append(row + ('year_db',))  # Add data_source
        
        # Batch insert
        db_conn.executemany('''
        INSERT INTO dust_measurements_realtime 
        (timestamp_utc, latitude, longitude, dust_concentration_ugm3, source_file, data_source)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', rows_to_insert)
        
        db_conn.commit()
        
        processing_time = time.time() - start_time
        
        # Record metadata
        db_conn.execute('''
        INSERT INTO processed_files 
        (file_path, total_points, processing_time_seconds, data_source)
        VALUES (?, ?, ?, ?)
        ''', (
            f"year_db:{source_file_name}",
            len(rows_to_insert),
            processing_time,
            'year_db'
        ))
        
        print(f"  Added {len(rows_to_insert)} points from year database in {processing_time:.2f}s")
        return len(rows_to_insert)
        
    except Exception as e:
        print(f"Error inserting data from year database: {e}")
        import traceback
        traceback.print_exc()
        return 0

def update_spatial_coverage(db_conn):
    """Update spatial coverage summary"""
    query = '''
    SELECT 
        MIN(latitude) as lat_min,
        MAX(latitude) as lat_max,
        MIN(longitude) as lon_min,
        MAX(longitude) as lon_max,
        COUNT(*) as total_points,
        AVG(dust_concentration_ugm3) as avg_dust,
        MIN(dust_concentration_ugm3) as min_dust,
        MAX(dust_concentration_ugm3) as max_dust
    FROM dust_measurements_realtime
    WHERE timestamp_utc >= datetime('now', '-1 day')
    '''
    
    coverage = db_conn.execute(query).fetchone()
    
    if coverage and coverage[4] > 0:  # Check if we have points
        db_conn.execute('''
        INSERT INTO spatial_coverage 
        (lat_min, lat_max, lon_min, lon_max, total_points, avg_dust_ugm3, min_dust_ugm3, max_dust_ugm3)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', coverage)
        
        db_conn.commit()
        
        print("\nCurrent Spatial Coverage:")
        print(f"  Latitude: {coverage[0]:.2f}° to {coverage[1]:.2f}°")
        print(f"  Longitude: {coverage[2]:.2f}° to {coverage[3]:.2f}°")
        print(f"  Total points: {coverage[4]:,}")
        print(f"  Dust concentration: {coverage[5]:.1f} avg, {coverage[6]:.1f} min, {coverage[7]:.1f} max µg/m³")

def cleanup_old_data(db_conn, keep_hours=24):
    """Remove data older than specified hours"""
    cutoff = (datetime.now(UTC) - timedelta(hours=keep_hours)).strftime('%Y-%m-%d %H:%M:%S')
    
    # Count before deletion
    count_query = "SELECT COUNT(*) FROM dust_measurements_realtime WHERE timestamp_utc < ?"
    old_count = db_conn.execute(count_query, (cutoff,)).fetchone()[0]
    
    if old_count > 0:
        # Delete old data
        delete_query = "DELETE FROM dust_measurements_realtime WHERE timestamp_utc < ?"
        db_conn.execute(delete_query, (cutoff,))
        db_conn.commit()
        
        # Vacuum to reclaim space
        db_conn.execute("VACUUM")
        
        print(f"Cleaned up {old_count} records older than {keep_hours} hours")
    
    return old_count

def get_database_stats(db_conn):
    """Get database statistics"""
    stats = {}
    
    # Total records
    query = "SELECT COUNT(*) FROM dust_measurements_realtime"
    stats['total_records'] = db_conn.execute(query).fetchone()[0]
    
    # Records by source
    query = "SELECT data_source, COUNT(*) FROM dust_measurements_realtime GROUP BY data_source"
    stats['records_by_source'] = dict(db_conn.execute(query).fetchall())
    
    # Time range
    query = "SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM dust_measurements_realtime"
    min_time, max_time = db_conn.execute(query).fetchone()
    stats['time_range'] = f"{min_time} to {max_time}"
    
    # Spatial extent
    query = '''
    SELECT 
        MIN(latitude), MAX(latitude),
        MIN(longitude), MAX(longitude)
    FROM dust_measurements_realtime
    '''
    lat_min, lat_max, lon_min, lon_max = db_conn.execute(query).fetchone()
    stats['spatial_extent'] = f"Lat: {lat_min:.1f}° to {lat_max:.1f}°, Lon: {lon_min:.1f}° to {lon_max:.1f}°"
    
    # Dust statistics
    query = '''
    SELECT 
        AVG(dust_concentration_ugm3),
        MIN(dust_concentration_ugm3),
        MAX(dust_concentration_ugm3),
        COUNT(DISTINCT timestamp_utc) as unique_timestamps
    FROM dust_measurements_realtime
    '''
    avg_dust, min_dust, max_dust, unique_ts = db_conn.execute(query).fetchone()
    stats['dust_stats'] = {
        'average': avg_dust,
        'minimum': min_dust,
        'maximum': max_dust
    }
    stats['unique_timestamps'] = unique_ts
    
    # Processed files
    query = "SELECT COUNT(*), data_source FROM processed_files GROUP BY data_source"
    processed_by_source = db_conn.execute(query).fetchall()
    stats['processed_files'] = {
        'total': sum(count for count, _ in processed_by_source),
        'by_source': dict(processed_by_source)
    }
    
    return stats

def main():
    parser = argparse.ArgumentParser(description='Create real-time dust database from latest .nc files and previous file from year database')
    parser.add_argument('--input', type=str, default='/home/omar/Documents/Dust/',
                       help='Input directory containing "latest" folders')
    parser.add_argument('--db', type=str, default='databases/dust_realtime.db',
                       help='Output database file')
    parser.add_argument('--year-db-dir', type=str, default='databases',
                       help='Directory containing year-specific databases (e.g., 2024_dust.db)')
    parser.add_argument('--pattern', type=str, default='*latest',
                       help='Pattern to match latest folders')
    parser.add_argument('--keep-hours', type=int, default=150,
                       help='Keep data for this many hours (older data deleted)')
    parser.add_argument('--dust-var', type=str, 
                       help='Name of dust variable in .nc files (auto-detected if not specified)')
    parser.add_argument('--stats', action='store_true',
                       help='Show database statistics')
    parser.add_argument('--cleanup', action='store_true',
                       help='Clean up old data without processing new files')
    
    args = parser.parse_args()
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(args.db), exist_ok=True)
    
    # Connect to database
    conn = sqlite3.connect(args.db)
    
    try:
        if args.stats:
            # Show statistics only
            stats = get_database_stats(conn)
            print("\n=== Real-time Database Statistics ===")
            print(f"Total records: {stats['total_records']:,}")
            print(f"Records by source: {stats['records_by_source']}")
            print(f"Time range: {stats['time_range']}")
            print(f"Spatial extent: {stats['spatial_extent']}")
            print(f"Unique timestamps: {stats['unique_timestamps']}")
            print(f"Dust concentration:")
            print(f"  Average: {stats['dust_stats']['average']:.1f} µg/m³")
            print(f"  Minimum: {stats['dust_stats']['minimum']:.1f} µg/m³")
            print(f"  Maximum: {stats['dust_stats']['maximum']:.1f} µg/m³")
            print(f"Processed files: {stats['processed_files']['total']} ({stats['processed_files']['by_source']})")
            
        elif args.cleanup:
            # Cleanup only
            print(f"Cleaning up data older than {args.keep_hours} hours...")
            removed = cleanup_old_data(conn, args.keep_hours)
            print(f"Removed {removed} records")
            
        else:
            # Process new data
            print(f"Looking for latest data in: {args.input}")
            
            # Find latest folder
            latest_folder = find_latest_data_folder(args.input, args.pattern)
            
            if not latest_folder:
                print(f"No folders found matching pattern: {args.pattern}")
                return
            
            print(f"Found latest folder: {latest_folder}")
            
            # Create database schema if needed
            create_database_schema(conn)
            
            # Scan for .nc files
            nc_files = get_nc_files(latest_folder)
            
            if not nc_files:
                print(f"No .nc files found in {latest_folder}")
                return
            
            print(f"Found {len(nc_files)} .nc files")
            
            # Find year database
            year_db_path = get_year_db_path(args.year_db_dir)
            print(f"Year database: {year_db_path}")
            
            total_points = 0
            
            # Process the LATEST .nc file
            if nc_files:
                latest_nc = nc_files[-1]  # Assuming last one is latest
                
                # Check if latest file already processed
                query = "SELECT COUNT(*) FROM processed_files WHERE file_path = ?"
                already_processed = conn.execute(query, (latest_nc,)).fetchone()[0] > 0
                
                if not already_processed:
                    # Process latest file
                    points_added = process_nc_file(latest_nc, conn, args.dust_var, 'latest_nc')
                    if points_added:
                        total_points += points_added
                else:
                    print(f"Skipping already processed: {os.path.basename(latest_nc)}")
                    
                # Find and process previous file from year database
                # Find and process previous 4 files from year database
                if year_db_path:
                    # Get list of all files from year database
                    previous_files = find_previous_nc_files_from_db(latest_nc, year_db_path, num_files=4)
                    
                    if previous_files:
                        print(f"\nFound {len(previous_files)} previous files to process: {previous_files}")
                        
                        for previous_nc_name in previous_files:
                            print(f"\nAttempting to extract data for previous file: {previous_nc_name}")
                            
                            # Check if previous file already processed from year_db
                            query = "SELECT COUNT(*) FROM processed_files WHERE file_path = ?"
                            already_processed = conn.execute(query, (f"year_db:{previous_nc_name}",)).fetchone()[0] > 0
                            
                            if not already_processed:
                                # Extract data from year database
                                year_db_data = extract_data_from_year_db(year_db_path, previous_nc_name)
                                
                                if year_db_data:
                                    # Insert data into realtime database
                                    points_added = insert_data_from_year_db(conn, year_db_data, previous_nc_name)
                                    if points_added:
                                        total_points += points_added
                                else:
                                    print(f"No data found in year database for file: {previous_nc_name}")
                            else:
                                print(f"Skipping already processed from year_db: {previous_nc_name}")
                    else:
                        print("No previous files found in year database")
                else:
                    print("No year database found, skipping previous file extraction")
            
            # Update spatial coverage
            if total_points > 0:
                update_spatial_coverage(conn)
            
            # Cleanup old data
            removed = cleanup_old_data(conn, args.keep_hours)
            
            # Show final statistics
            print(f"\n=== Processing Complete ===")
            print(f"Added {total_points:,} new points")
            print(f"Removed {removed:,} old points")
            
            stats = get_database_stats(conn)
            print(f"Total records in database: {stats['total_records']:,}")
            if 'records_by_source' in stats:
                print(f"Records by source: {stats['records_by_source']}")
    
    finally:
        conn.close()

if __name__ == '__main__':
    main()