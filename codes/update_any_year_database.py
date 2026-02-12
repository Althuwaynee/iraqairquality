#!/usr/bin/env python3
"""

update_year_database.py
#Create/update database for specific year of dust data.
#Usage: 
python update_year_database.py --year 2025

# Process specific year
python update_year_database.py --year 2025

# Process multiple years
python update_year_database.py --years 2023,2024,2025

# Process year range
python update_year_database.py --range 2023-2025

# Process all detected years
python update_year_database.py --all

# Show statistics for 2025
python update_year_database.py --year 2025 --stats

# Export September 2025 to CSV
python update_year_database.py --year 2025 --export-month 9

# Query across years
python query_multiyear.py --start 2025-09-01 --end 2025-09-30 --bbox 33 34 44 45

"""

import sqlite3
import xarray as xr
import pandas as pd
import numpy as np
import os
import glob
import logging
import argparse
from datetime import datetime
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dust_database.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

class YearDustDatabase:
    def __init__(self, year, db_dir='/home/omar/Documents/Dust/databases'):
        """Initialize database for specific year"""
        self.year = year
        self.db_dir = db_dir
        os.makedirs(db_dir, exist_ok=True)
        
        self.db_path = os.path.join(db_dir, f'{year}_dust.db')
        self.conn = None
        self.cursor = None
        
        # Constants for data validation
        self.MAX_DUST_VALUE = 2147483647  # SQLite INTEGER safe limit (INT32 max)
        self.MIN_DUST_VALUE = 0           # Dust can't be negative
        
        self.setup_database()
    
    def setup_database(self):
        """Create database schema for the year"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            
            # Create main data table
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS dust_measurements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_utc TEXT NOT NULL,        -- ISO format: 2025-09-30T12:00:00
                latitude REAL NOT NULL,             -- Decimal degrees
                longitude REAL NOT NULL,            -- Decimal degrees  
                dust_concentration_ugm3 INTEGER NOT NULL,  -- Integer micrograms/m³
                forecast_hour INTEGER,              -- Hours since forecast init (0, 3, 6, ...)
                forecast_init TEXT,                 -- Forecast initialization date
                file_source TEXT,                   -- Source NetCDF filename
                month INTEGER GENERATED ALWAYS AS (CAST(substr(timestamp_utc, 6, 2) AS INTEGER)) VIRTUAL,
                day INTEGER GENERATED ALWAYS AS (CAST(substr(timestamp_utc, 9, 2) AS INTEGER)) VIRTUAL,
                hour INTEGER GENERATED ALWAYS AS (CAST(substr(timestamp_utc, 12, 2) AS INTEGER)) VIRTUAL
            )
            ''')
            
            # Create indices for fast querying
            self.cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_timestamp ON dust_measurements(timestamp_utc)
            ''')
            self.cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_location ON dust_measurements(latitude, longitude)
            ''')
            self.cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_date_parts ON dust_measurements(month, day, hour)
            ''')
            
            # Create unique constraint to prevent duplicates
            self.cursor.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_measurement 
            ON dust_measurements(timestamp_utc, latitude, longitude, forecast_init, forecast_hour)
            ''')
            
            self.conn.commit()
            logger.info(f"Database initialized: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Database setup failed: {str(e)}")
            raise
    
    def get_year_from_filename(self, filename):
        """Extract year from filename"""
        # Try patterns: YYYYMMDD_3H_MEDIAN.nc or IRQ_YYYY_MM/YYYYMMDD_3H_MEDIAN.nc
        match = re.search(r'(\d{4})\d{4}_3H_MEDIAN\.nc', filename)
        if match:
            return int(match.group(1))
        return None
    
    def should_process_file(self, filepath, filename):
        """Check if file belongs to this database year"""
        # Get year from filename
        file_year = self.get_year_from_filename(filename)
        
        if file_year != self.year:
            logger.debug(f"Skipping {filename} - belongs to {file_year}, not {self.year}")
            return False
        
        # Check if already processed
        self.cursor.execute(
            'SELECT COUNT(*) FROM dust_measurements WHERE file_source = ?',
            (filename,)
        )
        count = self.cursor.fetchone()[0]
        
        if count > 0:
            logger.debug(f"File already processed: {filename} ({count} rows)")
            return False
        
        return True
    
    def find_files_for_year(self, base_path='.'):
        """Find all NetCDF files for this specific year"""
        year_files = []
        
        # Pattern 1: Folders like 2025_09_latest/
        pattern1 = os.path.join(base_path,'IRQ' ,f'IRQ_{self.year}_*', '*_3H_MEDIAN.nc')
        
        # Pattern 2: Folders like IRQ_2025_09/
        pattern2 = os.path.join(base_path, 'IRQ',f'IRQ_{self.year}_*', '*_3H_MEDIAN.nc')
        
        # Pattern 3: Direct .nc files in current directory
        pattern3 = os.path.join(base_path, 'IRQ',f'IRQ_{self.year}_*', '*_3H_MEDIAN.nc')   #os.path.join(base_path, f'{self.year}????_3H_MEDIAN.nc')
        
        for pattern in [pattern1, pattern2, pattern3]:
            for filepath in glob.glob(pattern):
                filename = os.path.basename(filepath)
                if self.should_process_file(filepath, filename):
                    year_files.append(filepath)
        
        logger.info(f"Found {len(year_files)} new files for year {self.year}")
        return sorted(year_files)
    
    def validate_dust_data(self, df, filename):
        """Validate dust concentration values before insertion"""
        original_count = len(df)
        
        # 1. Remove NaN values
        df = df.dropna(subset=['dust_concentration_ugm3'])
        
        # 2. Round to nearest integer
        df['dust_concentration_ugm3'] = df['dust_concentration_ugm3'].round(0)
        
        # 3. Clip to valid range
        df['dust_concentration_ugm3'] = df['dust_concentration_ugm3'].clip(
            lower=self.MIN_DUST_VALUE, 
            upper=self.MAX_DUST_VALUE
        )
        
        # 4. Convert to integer
        df['dust_concentration_ugm3'] = df['dust_concentration_ugm3'].astype(int)
        
        # Log validation results
        removed_nan = original_count - len(df)
        if removed_nan > 0:
            logger.debug(f"  Removed {removed_nan} NaN values from {filename}")
        
        # Check for extreme values (for monitoring)
        if len(df) > 0:
            max_val = df['dust_concentration_ugm3'].max()
            min_val = df['dust_concentration_ugm3'].min()
            avg_val = df['dust_concentration_ugm3'].mean()
            
            if max_val > 1000000:  # > 1,000,000 μg/m³ is unusual
                logger.warning(f"  High dust concentration detected: {max_val} μg/m³ in {filename}")
            
            logger.debug(f"  Value range: {min_val}-{max_val} μg/m³, avg: {avg_val:.1f}")
        
        return df
    
    def process_netcdf_file(self, filepath):
        """Process single NetCDF file"""
        filename = os.path.basename(filepath)
        forecast_init = filename[:8]  # YYYYMMDD
        
        try:
            logger.info(f"Processing: {filename}")
            ds = xr.open_dataset(filepath, engine='netcdf4')
            
            if 'SCONC_DUST' not in ds:
                logger.warning(f"No SCONC_DUST variable in {filename}")
                ds.close()
                return 0
            
            # Convert kg/m³ to μg/m³
            dust_data = ds['SCONC_DUST'] * 1e9
            
            inserted_count = 0
            batch_data = []
            
            # Process each time step
            for i, time_step in enumerate(ds.time):
                try:
                    timestamp_utc = pd.to_datetime(time_step.values)
                    timestamp_str = timestamp_utc.strftime('%Y-%m-%dT%H:%M:%S')
                    
                    # Calculate forecast hour
                    init_date = pd.Timestamp(forecast_init)
                    forecast_hour = int((timestamp_utc - init_date).total_seconds() / 3600)
                    
                    # Get data for this time step
                    time_data = dust_data.isel(time=i)
                    
                    # Convert to DataFrame
                    df = time_data.to_dataframe(name='dust_concentration_ugm3').reset_index()
                    
                    # Validate and clean data
                    df = self.validate_dust_data(df, filename)
                    
                    if len(df) == 0:
                        continue
                    
                    # Add metadata columns
                    df['timestamp_utc'] = timestamp_str
                    df['forecast_hour'] = forecast_hour
                    df['forecast_init'] = f"{forecast_init[:4]}-{forecast_init[4:6]}-{forecast_init[6:8]}"
                    df['file_source'] = filename
                    
                    # Prepare batch data efficiently
                    batch_records = []
                    for _, row in df.iterrows():
                        batch_records.append((
                            row['timestamp_utc'],
                            float(row['latitude']),
                            float(row['longitude']),
                            int(row['dust_concentration_ugm3']),
                            row['forecast_hour'],
                            row['forecast_init'],
                            row['file_source']
                        ))
                    
                    # Insert batch
                    if batch_records:
                        self.insert_batch(batch_records)
                        inserted_count += len(batch_records)
                    
                except Exception as time_error:
                    logger.error(f"Error processing timestep {i}: {str(time_error)}")
                    continue
            
            self.conn.commit()
            ds.close()
            
            logger.info(f"Inserted {inserted_count} rows from {filename}")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Failed to process {filename}: {str(e)}")
            return 0
    
    def insert_batch(self, batch_data):
        """Insert batch of data efficiently"""
        try:
            self.cursor.executemany('''
            INSERT OR IGNORE INTO dust_measurements 
            (timestamp_utc, latitude, longitude, dust_concentration_ugm3, 
             forecast_hour, forecast_init, file_source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', batch_data)
        except Exception as e:
            logger.error(f"Batch insert failed: {str(e)}")
            raise
    
    def update_database(self, base_path='IRQ'):
        """Scan for new files for this year and update database"""
        year_files = self.find_files_for_year(base_path)
        
        if not year_files:
            logger.info(f"No new files found for year {self.year}")
            return 0
        
        total_inserted = 0
        for filepath in year_files:
            inserted = self.process_netcdf_file(filepath)
            total_inserted += inserted
        
        return total_inserted
    
    def get_stats(self):
        """Get statistics for this year's database"""
        stats = {}
        
        self.cursor.execute('SELECT COUNT(*) FROM dust_measurements')
        stats['total_rows'] = self.cursor.fetchone()[0]
        
        self.cursor.execute('SELECT MIN(timestamp_utc), MAX(timestamp_utc) FROM dust_measurements')
        min_date, max_date = self.cursor.fetchone()
        stats['date_range'] = (min_date, max_date)
        
        self.cursor.execute('SELECT COUNT(DISTINCT file_source) FROM dust_measurements')
        stats['source_files'] = self.cursor.fetchone()[0]
        
        # Dust value statistics
        self.cursor.execute('''
        SELECT 
            MIN(dust_concentration_ugm3),
            MAX(dust_concentration_ugm3),
            AVG(dust_concentration_ugm3),
            COUNT(*) as total_values
        FROM dust_measurements
        ''')
        min_val, max_val, avg_val, count = self.cursor.fetchone()
        stats['dust_stats'] = {
            'min': min_val,
            'max': max_val,
            'avg': avg_val,
            'count': count
        }
        
        return stats
    
    def export_month(self, month, output_dir='exports'):
        """Export specific month to CSV"""
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f'{self.year}_{month:02d}_dust.csv')
        
        query = '''
        SELECT timestamp_utc, latitude, longitude, dust_concentration_ugm3,
               forecast_hour, forecast_init, file_source
        FROM dust_measurements
        WHERE month = ?
        ORDER BY timestamp_utc, latitude, longitude
        '''
        
        df = pd.read_sql_query(query, self.conn, params=(month,))
        
        if not df.empty:
            df.to_csv(output_path, index=False)
            logger.info(f"Exported {len(df)} rows for {self.year}-{month:02d} to {output_path}")
        else:
            logger.warning(f"No data found for {self.year}-{month:02d}")
        
        return df
    
    def optimize_database(self):
        """Optimize database size and performance"""
        logger.info("Optimizing database...")
        
        # Run VACUUM to reclaim space
        self.cursor.execute("VACUUM")
        
        # Update statistics for query optimizer
        self.cursor.execute("ANALYZE")
        
        self.conn.commit()
        logger.info("Database optimization complete")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            # Optimize before closing
            self.optimize_database()
            self.conn.close()
            logger.info(f"Database connection closed: {self.db_path}")

def process_multiple_years(years, base_path='.'):
    """Process multiple years in sequence"""
    for year in years:
        logger.info(f"Processing year: {year}")
        db = YearDustDatabase(year)
        
        try:
            inserted = db.update_database(base_path)
            stats = db.get_stats()
            
            logger.info(f"Year {year} complete:")
            logger.info(f"  New rows inserted: {inserted:,}")
            logger.info(f"  Total rows in DB: {stats['total_rows']:,}")
            logger.info(f"  Date range: {stats['date_range'][0]} to {stats['date_range'][1]}")
            logger.info(f"  Dust range: {stats['dust_stats']['min']}-{stats['dust_stats']['max']} μg/m³")
            
        finally:
            db.close()

def main():
    parser = argparse.ArgumentParser(description='Year-based dust database manager')
    parser.add_argument('--year', type=int, help='Specific year to process')
    parser.add_argument('--years', type=str, help='Comma-separated years (2023,2024,2025)')
    parser.add_argument('--range', type=str, help='Year range (2023-2025)')
    parser.add_argument('--all', action='store_true', help='Process all available years')
    parser.add_argument('--stats', action='store_true', help='Show database statistics')
    parser.add_argument('--export-month', type=int, help='Export specific month to CSV')
    parser.add_argument('--path', type=str, default='.', help='Path to search for NetCDF files')
    parser.add_argument('--optimize', action='store_true', help='Optimize database without processing')
    
    args = parser.parse_args()
    
    # Determine which years to process
    years_to_process = []
    
    if args.year:
        years_to_process = [args.year]
    elif args.years:
        years_to_process = [int(y.strip()) for y in args.years.split(',')]
    elif args.range:
        start, end = map(int, args.range.split('-'))
        years_to_process = list(range(start, end + 1))
    elif args.all:
        # Auto-detect available years from folders
        folders = glob.glob(os.path.join(args.path, '*_*_latest'))
        for folder in folders:
            match = re.search(r'(\d{4})_\d{2}_latest', folder)
            if match:
                years_to_process.append(int(match.group(1)))
        years_to_process = sorted(set(years_to_process))
    else:
        # Default: current year
        years_to_process = [datetime.now().year]
    
    if not years_to_process:
        logger.error("No years specified or found")
        return
    
    logger.info(f"Processing years: {years_to_process}")
    
    # Process each year
    for year in years_to_process:
        logger.info(f"=== Processing year {year} ===")
        
        db = YearDustDatabase(year)
        
        try:
            if args.stats:
                stats = db.get_stats()
                print(f"\nYear {year} Statistics:")
                print("="*50)
                print(f"Total measurements: {stats['total_rows']:,}")
                print(f"Date range: {stats['date_range'][0]} to {stats['date_range'][1]}")
                print(f"Source files: {stats['source_files']}")
                print(f"Dust concentration range: {stats['dust_stats']['min']} - {stats['dust_stats']['max']} μg/m³")
                print(f"Dust concentration average: {stats['dust_stats']['avg']:.1f} μg/m³")
                print("="*50)
            
            elif args.export_month:
                db.export_month(args.export_month)
            
            elif args.optimize:
                db.optimize_database()
                logger.info(f"Database optimized: {year}")
            
            else:
                # Default: update database
                inserted = db.update_database(args.path)
                if inserted > 0:
                    logger.info(f"Inserted {inserted:,} new rows for year {year}")
                
                # Show final stats
                stats = db.get_stats()
                logger.info(f"Year {year} complete: {stats['total_rows']:,} total rows")
        
        finally:
            db.close()

if __name__ == '__main__':
    main()