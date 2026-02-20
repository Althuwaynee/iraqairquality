#!/usr/bin/env python3
"""
prepare_raster_maps.py
Generate various dust distribution maps from historical grid data
Supports incremental updates using cumulative statistics
"""

import sqlite3
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
import argparse
from scipy.interpolate import griddata
import hashlib

# Configuration
OUTPUT_DIR = 'iraqairquality/data/maps'
STATE_FILE = os.path.join(OUTPUT_DIR, 'processing_state.json')
IRAQ_BBOX = {
    'min_lat': 27.0, 'max_lat': 38.0,
    'min_lon': 37.5, 'max_lon': 51.0
}

# WHO thresholds
IRAQ_24H_THRESHOLD = 100  # µg/m³
EXTREME_THRESHOLD = 200  # µg/m³ for dust storm conditions

class DustRasterGenerator:
    def __init__(self, grid_shapefile):
        self.grid_shapefile = grid_shapefile
        self.grid_points = None
        self.state = self.load_state()
        self.load_grid()
        
    def load_state(self):
        """Load processing state from JSON file"""
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    state = json.load(f)
                print(f"Loaded processing state from {STATE_FILE}")
                return state
            except Exception as e:
                print(f"Error loading state file: {e}")
                return self.create_initial_state()
        else:
            print("No state file found. Creating initial state.")
            return self.create_initial_state()
    
    def create_initial_state(self):
        """Create initial state structure"""
        return {
            'version': '1.0',
            'last_updated': datetime.now().isoformat(),
            'processed_years': [],
            'cumulative_stats': {
                'long_term': {},
                'seasonal': {
                    'winter': {},
                    'spring': {},
                    'summer': {},
                    'autumn': {}
                },
                'monthly': {month: {} for month in range(1, 13)},
                'annual': {},
                'extreme_days': {
                    'daily_max': {},  # Multi-year daily max
                    'by_year': {}     # Per-year daily max
                }
            },
            'file_hashes': {}
        }
    
    def save_state(self):
        """Save processing state to JSON file"""
        self.state['last_updated'] = datetime.now().isoformat()
        
        temp_file = STATE_FILE + '.tmp'
        with open(temp_file, 'w') as f:
            json.dump(self.state, f, indent=2)
        
        os.replace(temp_file, STATE_FILE)
        print(f"Saved processing state to {STATE_FILE}")
    
    def get_file_hash(self, filepath):
        """Calculate MD5 hash of a file"""
        hash_md5 = hashlib.md5()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def load_grid(self):
        """Load grid points from shapefile"""
        import fiona
        from shapely.geometry import shape
        
        print("Loading grid points...")
        points = []
        with fiona.open(self.grid_shapefile) as src:
            for feature in src:
                geom = shape(feature['geometry'])
                if geom.geom_type == 'Point':
                    point_key = f"{geom.y:.3f}_{geom.x:.3f}"
                    points.append({
                        'lat': geom.y,
                        'lon': geom.x,
                        'key': point_key,
                        'id': feature['properties'].get('id', len(points))
                    })
        self.grid_points = pd.DataFrame(points)
        print(f"Loaded {len(self.grid_points)} grid points")
    
    def _month_to_season(self, month):
        """Convert month to season"""
        if month in [12, 1, 2]:
            return 'winter'
        elif month in [3, 4, 5]:
            return 'spring'
        elif month in [6, 7, 8]:
            return 'summer'
        else:
            return 'autumn'
    
    def process_year_data(self, year, data_dir):
        """Process a single year of data and update cumulative stats"""
        db_path = os.path.join(data_dir, f"{year}_dust.db")
        if not os.path.exists(db_path):
            print(f"Database for {year} not found")
            return False
        
        current_hash = self.get_file_hash(db_path)
        if year in self.state['file_hashes'] and self.state['file_hashes'][year] == current_hash:
            print(f"Year {year} already processed and unchanged. Skipping.")
            return False
        
        print(f"\nProcessing year {year}...")
        
        try:
            conn = sqlite3.connect(db_path)
            query = """
            SELECT timestamp_utc as timestamp, latitude, longitude, dust_concentration_ugm3 as value
            FROM dust_measurements
            WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?
            """
            df = pd.read_sql_query(
                query, conn,
                params=(IRAQ_BBOX['min_lat'], IRAQ_BBOX['max_lat'],
                       IRAQ_BBOX['min_lon'], IRAQ_BBOX['max_lon'])
            )
            conn.close()
            
            if df.empty:
                print(f"No data for {year} in Iraq region")
                return False
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['year'] = df['timestamp'].dt.year
            df['month'] = df['timestamp'].dt.month
            df['season'] = df['month'].map(self._month_to_season)
            df['date'] = df['timestamp'].dt.date
            df['point_key'] = df['latitude'].round(3).astype(str) + '_' + df['longitude'].round(3).astype(str)
            
            self.update_cumulative_stats(df, year)
            
            if year not in self.state['processed_years']:
                self.state['processed_years'].append(year)
                self.state['processed_years'].sort()
            self.state['file_hashes'][year] = current_hash
            
            print(f"✓ Successfully processed year {year}")
            return True
            
        except Exception as e:
            print(f"Error processing {year}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def update_cumulative_stats(self, df, year):
        """Update cumulative statistics with new year's data"""
        
        # 1. Long-term mean
        point_stats = df.groupby('point_key').agg({
            'value': ['count', 'sum']
        }).reset_index()
        point_stats.columns = ['point_key', 'count', 'sum']
        
        if 'long_term' not in self.state['cumulative_stats']:
            self.state['cumulative_stats']['long_term'] = {}
        
        for _, row in point_stats.iterrows():
            key = row['point_key']
            if key not in self.state['cumulative_stats']['long_term']:
                self.state['cumulative_stats']['long_term'][key] = {'count': 0, 'sum': 0.0}
            
            self.state['cumulative_stats']['long_term'][key]['count'] += int(row['count'])
            self.state['cumulative_stats']['long_term'][key]['sum'] += float(row['sum'])
        
        # 2. Seasonal stats
        seasonal_stats = df.groupby(['point_key', 'season']).agg({
            'value': ['count', 'sum']
        }).reset_index()
        seasonal_stats.columns = ['point_key', 'season', 'count', 'sum']
        
        for _, row in seasonal_stats.iterrows():
            key = row['point_key']
            season = row['season']
            if season not in self.state['cumulative_stats']['seasonal']:
                self.state['cumulative_stats']['seasonal'][season] = {}
            
            if key not in self.state['cumulative_stats']['seasonal'][season]:
                self.state['cumulative_stats']['seasonal'][season][key] = {'count': 0, 'sum': 0.0}
            
            self.state['cumulative_stats']['seasonal'][season][key]['count'] += int(row['count'])
            self.state['cumulative_stats']['seasonal'][season][key]['sum'] += float(row['sum'])
        
        # 3. Monthly stats
        monthly_stats = df.groupby(['point_key', 'month']).agg({
            'value': ['count', 'sum']
        }).reset_index()
        monthly_stats.columns = ['point_key', 'month', 'count', 'sum']
        
        for _, row in monthly_stats.iterrows():
            key = row['point_key']
            month = str(row['month'])
            if month not in self.state['cumulative_stats']['monthly']:
                self.state['cumulative_stats']['monthly'][month] = {}
            
            if key not in self.state['cumulative_stats']['monthly'][month]:
                self.state['cumulative_stats']['monthly'][month][key] = {'count': 0, 'sum': 0.0}
            
            self.state['cumulative_stats']['monthly'][month][key]['count'] += int(row['count'])
            self.state['cumulative_stats']['monthly'][month][key]['sum'] += float(row['sum'])
        
        # 4. Annual stats
        annual_stats = df.groupby(['point_key', 'year']).agg({
            'value': ['count', 'sum']
        }).reset_index()
        annual_stats.columns = ['point_key', 'year', 'count', 'sum']
        
        year_str = str(year)
        if year_str not in self.state['cumulative_stats']['annual']:
            self.state['cumulative_stats']['annual'][year_str] = {}
        
        for _, row in annual_stats.iterrows():
            key = row['point_key']
            if key not in self.state['cumulative_stats']['annual'][year_str]:
                self.state['cumulative_stats']['annual'][year_str][key] = {'count': 0, 'sum': 0.0}
            
            self.state['cumulative_stats']['annual'][year_str][key]['count'] += int(row['count'])
            self.state['cumulative_stats']['annual'][year_str][key]['sum'] += float(row['sum'])
        
        # 5. Extreme days - Store both multi-year and per-year
        daily_max = df.groupby(['point_key', 'date'])['value'].max().reset_index()
        
        # Initialize per-year storage
        if 'by_year' not in self.state['cumulative_stats']['extreme_days']:
            self.state['cumulative_stats']['extreme_days']['by_year'] = {}
        
        if year_str not in self.state['cumulative_stats']['extreme_days']['by_year']:
            self.state['cumulative_stats']['extreme_days']['by_year'][year_str] = {
                'daily_max': {},
                'total_days': 0,
                'who_exceed': 0,
                'extreme_exceed': 0
            }
        
        year_extreme = self.state['cumulative_stats']['extreme_days']['by_year'][year_str]
        
        for _, row in daily_max.iterrows():
            key = row['point_key']
            date_str = row['date'].isoformat()
            max_val = row['value']
            
            # Multi-year storage
            if 'daily_max' not in self.state['cumulative_stats']['extreme_days']:
                self.state['cumulative_stats']['extreme_days']['daily_max'] = {}
            
            if key not in self.state['cumulative_stats']['extreme_days']['daily_max']:
                self.state['cumulative_stats']['extreme_days']['daily_max'][key] = {}
            
            if date_str not in self.state['cumulative_stats']['extreme_days']['daily_max'][key]:
                self.state['cumulative_stats']['extreme_days']['daily_max'][key][date_str] = max_val
                
                # Update multi-year totals (only once per unique date)
                if 'total_days' not in self.state['cumulative_stats']['extreme_days']:
                    self.state['cumulative_stats']['extreme_days']['total_days'] = 0
                    self.state['cumulative_stats']['extreme_days']['who_exceed'] = 0
                    self.state['cumulative_stats']['extreme_days']['extreme_exceed'] = 0
                
                self.state['cumulative_stats']['extreme_days']['total_days'] += 1
                if max_val > IRAQ_24H_THRESHOLD:
                    self.state['cumulative_stats']['extreme_days']['who_exceed'] += 1
                if max_val > EXTREME_THRESHOLD:
                    self.state['cumulative_stats']['extreme_days']['extreme_exceed'] += 1
            
            # Per-year storage
            if key not in year_extreme['daily_max']:
                year_extreme['daily_max'][key] = {}
            
            if date_str not in year_extreme['daily_max'][key]:
                year_extreme['daily_max'][key][date_str] = max_val
                year_extreme['total_days'] += 1
                
                if max_val > IRAQ_24H_THRESHOLD:
                    year_extreme['who_exceed'] += 1
                if max_val > EXTREME_THRESHOLD:
                    year_extreme['extreme_exceed'] += 1
    
    def calculate_long_term_mean(self):
        """Calculate long-term mean from cumulative stats"""
        print("Calculating long-term mean...")
        results = []
        
        for key, stats in self.state['cumulative_stats']['long_term'].items():
            if stats['count'] > 0:
                lat, lon = map(float, key.split('_'))
                mean_val = stats['sum'] / stats['count']
                results.append({
                    'latitude': lat,
                    'longitude': lon,
                    'value': mean_val
                })
        
        return pd.DataFrame(results)
    
    def calculate_seasonal_means(self, year=None):
        """Calculate seasonal means from cumulative stats"""
        print(f"Calculating seasonal means{' for year ' + str(year) if year else ' (multi-year)'}...")
        seasons = ['winter', 'spring', 'summer', 'autumn']
        results = {season: [] for season in seasons}
        
        if year is None:
            # Multi-year seasonal means
            for season in seasons:
                if season in self.state['cumulative_stats']['seasonal']:
                    for key, stats in self.state['cumulative_stats']['seasonal'][season].items():
                        if stats['count'] > 0:
                            lat, lon = map(float, key.split('_'))
                            mean_val = stats['sum'] / stats['count']
                            results[season].append({
                                'latitude': lat,
                                'longitude': lon,
                                'value': mean_val
                            })
        else:
            # Single-year seasonal means - need monthly data per year
            # For now, use annual average
            year_str = str(year)
            if year_str in self.state['cumulative_stats']['annual']:
                for key, stats in self.state['cumulative_stats']['annual'][year_str].items():
                    if stats['count'] > 0:
                        lat, lon = map(float, key.split('_'))
                        mean_val = stats['sum'] / stats['count']
                        for season in seasons:
                            results[season].append({
                                'latitude': lat,
                                'longitude': lon,
                                'value': mean_val
                            })
        
        dfs = {}
        for season in seasons:
            if results[season]:
                dfs[season] = pd.DataFrame(results[season])
        
        return dfs
    
    def calculate_monthly_means(self, year=None):
        """Calculate monthly means from cumulative stats"""
        print(f"Calculating monthly means{' for year ' + str(year) if year else ' (multi-year)'}...")
        results = {}
        
        if year is None:
            # Multi-year monthly means
            for month in range(1, 13):
                month_str = str(month)
                month_data = []
                
                if month_str in self.state['cumulative_stats']['monthly']:
                    for key, stats in self.state['cumulative_stats']['monthly'][month_str].items():
                        if stats['count'] > 0:
                            lat, lon = map(float, key.split('_'))
                            mean_val = stats['sum'] / stats['count']
                            month_data.append({
                                'latitude': lat,
                                'longitude': lon,
                                'value': mean_val
                            })
                
                if month_data:
                    results[month] = pd.DataFrame(month_data)
        else:
            # Single-year monthly means - use annual average for now
            year_str = str(year)
            if year_str in self.state['cumulative_stats']['annual']:
                for month in range(1, 13):
                    month_data = []
                    for key, stats in self.state['cumulative_stats']['annual'][year_str].items():
                        if stats['count'] > 0:
                            lat, lon = map(float, key.split('_'))
                            mean_val = stats['sum'] / stats['count']
                            month_data.append({
                                'latitude': lat,
                                'longitude': lon,
                                'value': mean_val
                            })
                    if month_data:
                        results[month] = pd.DataFrame(month_data)
        
        return results
    
    def calculate_annual_means(self):
        """Calculate annual means from cumulative stats"""
        print("Calculating annual means...")
        results = {}
        
        for year_str, year_data in self.state['cumulative_stats']['annual'].items():
            year = int(year_str)
            year_results = []
            
            for key, stats in year_data.items():
                if stats['count'] > 0:
                    lat, lon = map(float, key.split('_'))
                    mean_val = stats['sum'] / stats['count']
                    year_results.append({
                        'latitude': lat,
                        'longitude': lon,
                        'value': mean_val
                    })
            
            if year_results:
                results[year] = pd.DataFrame(year_results)
        
        return results
    
    def calculate_extreme_days(self, year=None):
        """Calculate exceedance percentages from stored daily max"""
        print(f"Calculating extreme day statistics{' for year ' + str(year) if year else ' (multi-year)'}...")
        
        if year is None:
            # Multi-year exceedance
            if 'daily_max' not in self.state['cumulative_stats']['extreme_days']:
                return None, None
            
            daily_max = self.state['cumulative_stats']['extreme_days']['daily_max']
            total_days = self.state['cumulative_stats']['extreme_days'].get('total_days', 0)
            
            if total_days == 0:
                return None, None
            
            who_results = []
            extreme_results = []
            
            for key, days in daily_max.items():
                lat, lon = map(float, key.split('_'))
                total_point_days = len(days)
                
                if total_point_days > 0:
                    who_count = sum(1 for val in days.values() if val > IRAQ_24H_THRESHOLD)
                    extreme_count = sum(1 for val in days.values() if val > EXTREME_THRESHOLD)
                    
                    who_pct = (who_count / total_point_days) * 100
                    extreme_pct = (extreme_count / total_point_days) * 100
                    
                    who_results.append({
                        'latitude': lat,
                        'longitude': lon,
                        'value': round(who_pct, 1)
                    })
                    
                    extreme_results.append({
                        'latitude': lat,
                        'longitude': lon,
                        'value': round(extreme_pct, 1)
                    })
            
            who_df = pd.DataFrame(who_results) if who_results else None
            extreme_df = pd.DataFrame(extreme_results) if extreme_results else None
            
            return who_df, extreme_df
        
        else:
            # Single-year exceedance
            year_str = str(year)
            if 'by_year' not in self.state['cumulative_stats']['extreme_days']:
                return None, None
            
            year_data = self.state['cumulative_stats']['extreme_days']['by_year'].get(year_str)
            if not year_data or year_data['total_days'] == 0:
                return None, None
            
            daily_max = year_data['daily_max']
            who_results = []
            extreme_results = []
            
            for key, days in daily_max.items():
                lat, lon = map(float, key.split('_'))
                total_point_days = len(days)
                
                if total_point_days > 0:
                    who_count = sum(1 for val in days.values() if val > IRAQ_24H_THRESHOLD)
                    extreme_count = sum(1 for val in days.values() if val > EXTREME_THRESHOLD)
                    
                    who_pct = (who_count / total_point_days) * 100
                    extreme_pct = (extreme_count / total_point_days) * 100
                    
                    who_results.append({
                        'latitude': lat,
                        'longitude': lon,
                        'value': round(who_pct, 1)
                    })
                    
                    extreme_results.append({
                        'latitude': lat,
                        'longitude': lon,
                        'value': round(extreme_pct, 1)
                    })
            
            who_df = pd.DataFrame(who_results) if who_results else None
            extreme_df = pd.DataFrame(extreme_results) if extreme_results else None
            
            return who_df, extreme_df
    
    def interpolate_to_grid(self, df, value_col):
        """Interpolate point data to regular grid for raster display"""
        if df is None or len(df) == 0:
            return None
            
        lats = np.arange(IRAQ_BBOX['min_lat'], IRAQ_BBOX['max_lat'], 0.1)
        lons = np.arange(IRAQ_BBOX['min_lon'], IRAQ_BBOX['max_lon'], 0.1)
        grid_lon, grid_lat = np.meshgrid(lons, lats)
        
        points = df[['longitude', 'latitude']].values
        values = df[value_col].values
        
        grid_values = griddata(points, values, (grid_lon, grid_lat), method='linear')
        
        return {
            'lats': lats,
            'lons': lons,
            'values': grid_values,
            'min': np.nanmin(grid_values) if not np.all(np.isnan(grid_values)) else 0,
            'max': np.nanmax(grid_values) if not np.all(np.isnan(grid_values)) else 0
        }
    
    def save_raster_json(self, name, grid_data):
        """Save interpolated grid as JSON for web"""
        if grid_data is None:
            print(f"  ⚠ No data for {name}, skipping")
            return
        
        rows = []
        for i, lat in enumerate(grid_data['lats']):
            for j, lon in enumerate(grid_data['lons']):
                val = grid_data['values'][i, j]
                if not np.isnan(val):
                    rows.append({
                        'lat': float(lat),
                        'lon': float(lon),
                        'val': float(val)
                    })
        
        if not rows:
            print(f"  ⚠ No valid grid points for {name}, skipping")
            return
        
        output = {
            'name': name,
            'generated': datetime.now().isoformat(),
            'bounds': IRAQ_BBOX,
            'min': float(grid_data['min']),
            'max': float(grid_data['max']),
            'data': rows
        }
        
        with open(os.path.join(OUTPUT_DIR, f'{name}_compact.json'), 'w') as f:
            json.dump(output, f)
        
        print(f"  ✓ {name}: {len(rows)} grid points (min={grid_data['min']:.1f}, max={grid_data['max']:.1f})")
    
    def save_metadata(self):
        """Save map metadata with all available options"""
        metadata = {
            'generated': datetime.now().isoformat(),
            'bounds': IRAQ_BBOX,
            'processed_years': self.state['processed_years'],
            'total_measurements': sum(
                stats['count'] for stats in self.state['cumulative_stats']['long_term'].values()
            ) if 'long_term' in self.state['cumulative_stats'] else 0,
            'maps': {
                'long_term_mean': {'name': 'المتوسط طويل الأمد', 'type': 'mean', 'unit': 'µg/m³', 'period': 'multi'},
                'seasonal_winter': {'name': 'الشتاء (متعدد السنوات)', 'type': 'seasonal', 'unit': 'µg/m³', 'period': 'multi'},
                'seasonal_spring': {'name': 'الربيع (متعدد السنوات)', 'type': 'seasonal', 'unit': 'µg/m³', 'period': 'multi'},
                'seasonal_summer': {'name': 'الصيف (متعدد السنوات)', 'type': 'seasonal', 'unit': 'µg/m³', 'period': 'multi'},
                'seasonal_autumn': {'name': 'الخريف (متعدد السنوات)', 'type': 'seasonal', 'unit': 'µg/m³', 'period': 'multi'},
                'exceedance_who': {'name': 'أيام تجاوز معيار الصحة', 'type': 'exceedance', 'unit': '%', 'period': 'multi'},
                'exceedance_extreme': {'name': 'أيام العواصف الترابية', 'type': 'exceedance', 'unit': '%', 'period': 'multi'}
            },
            'months': [f'monthly_{i:02d}' for i in range(1, 13)],
            'years': [f'annual_{year}' for year in self.state['processed_years']]
        }
        
        with open(os.path.join(OUTPUT_DIR, 'metadata.json'), 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"\nMetadata saved. Processed years: {self.state['processed_years']}")
    
    def generate_all_maps(self):
        """Generate all map types from cumulative stats"""
        print("\n" + "="*60)
        print("GENERATING MAPS FROM CUMULATIVE STATISTICS")
        print("="*60)
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        # ============= MULTI-YEAR MAPS =============
        print("\n--- MULTI-YEAR MAPS ---")
        
        # 1. Long-term mean
        print("\n1. Generating long-term mean map...")
        long_term_df = self.calculate_long_term_mean()
        if long_term_df is not None and len(long_term_df) > 0:
            long_term_grid = self.interpolate_to_grid(long_term_df, 'value')
            self.save_raster_json('long_term_mean', long_term_grid)
        
        # 2. Multi-year seasonal means
        print("\n2. Generating multi-year seasonal maps...")
        seasonal_dfs = self.calculate_seasonal_means()
        for season, df in seasonal_dfs.items():
            if df is not None and len(df) > 0:
                season_grid = self.interpolate_to_grid(df, 'value')
                self.save_raster_json(f'seasonal_{season}', season_grid)
        
        # 3. Multi-year monthly means
        print("\n3. Generating multi-year monthly maps...")
        monthly_dfs = self.calculate_monthly_means()
        for month, df in monthly_dfs.items():
            if df is not None and len(df) > 0:
                month_grid = self.interpolate_to_grid(df, 'value')
                self.save_raster_json(f'monthly_{month:02d}', month_grid)
        
        # 4. Multi-year exceedance
        print("\n4. Generating multi-year exceedance maps...")
        who_df, extreme_df = self.calculate_extreme_days()
        if who_df is not None and len(who_df) > 0:
            who_grid = self.interpolate_to_grid(who_df, 'value')
            self.save_raster_json('exceedance_who', who_grid)
        
        if extreme_df is not None and len(extreme_df) > 0:
            extreme_grid = self.interpolate_to_grid(extreme_df, 'value')
            self.save_raster_json('exceedance_extreme', extreme_grid)
        
        # 5. Annual means
        print("\n5. Generating annual maps...")
        annual_dfs = self.calculate_annual_means()
        for year, df in annual_dfs.items():
            if df is not None and len(df) > 0:
                year_grid = self.interpolate_to_grid(df, 'value')
                self.save_raster_json(f'annual_{year}', year_grid)
        
        # ============= SINGLE-YEAR MAPS =============
        print("\n--- SINGLE-YEAR MAPS ---")
        
        for year in self.state['processed_years']:
            print(f"\nGenerating maps for year {year}...")
            
            # Single-year seasonal
            seasonal_year_dfs = self.calculate_seasonal_means(year=year)
            for season, df in seasonal_year_dfs.items():
                if df is not None and len(df) > 0:
                    season_grid = self.interpolate_to_grid(df, 'value')
                    self.save_raster_json(f'seasonal_{season}_{year}', season_grid)
            
            # Single-year monthly
            monthly_year_dfs = self.calculate_monthly_means(year=year)
            for month, df in monthly_year_dfs.items():
                if df is not None and len(df) > 0:
                    month_grid = self.interpolate_to_grid(df, 'value')
                    self.save_raster_json(f'monthly_{month:02d}_{year}', month_grid)
            
            # Single-year exceedance
            who_year_df, extreme_year_df = self.calculate_extreme_days(year=year)
            if who_year_df is not None and len(who_year_df) > 0:
                who_year_grid = self.interpolate_to_grid(who_year_df, 'value')
                self.save_raster_json(f'exceedance_who_{year}', who_year_grid)
            
            if extreme_year_df is not None and len(extreme_year_df) > 0:
                extreme_year_grid = self.interpolate_to_grid(extreme_year_df, 'value')
                self.save_raster_json(f'exceedance_extreme_{year}', extreme_year_grid)
        
        self.save_metadata()
        
        print("\n" + "="*60)
        print(f"COMPLETE! Maps saved to {OUTPUT_DIR}")
        print("="*60)
    
    def update_from_data_dir(self, data_dir, years=None):
        """Process new/updated databases and update cumulative stats"""
        if years is None:
            years = []
            for file in os.listdir(data_dir):
                if file.endswith('_dust.db'):
                    try:
                        year = int(file.split('_')[0])
                        years.append(year)
                    except:
                        pass
            years.sort()
        
        print("="*60)
        print("PROCESSING DATABASES AND UPDATING STATISTICS")
        print("="*60)
        
        any_processed = False
        for year in years:
            if self.process_year_data(year, data_dir):
                any_processed = True
        
        if any_processed:
            self.save_state()
            print("\nStatistics updated. Generating maps...")
            self.generate_all_maps()
        else:
            print("\nNo new data to process.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--grid-shapefile', required=True, help='Grid points shapefile')
    parser.add_argument('--data-dir', required=True, help='Directory containing year databases')
    parser.add_argument('--years', nargs='+', type=int, help='Specific years to process (optional)')
    parser.add_argument('--regenerate', action='store_true', help='Regenerate maps from existing stats without processing new data')
    args = parser.parse_args()
    
    generator = DustRasterGenerator(args.grid_shapefile)
    
    if args.regenerate:
        generator.generate_all_maps()
    else:
        generator.update_from_data_dir(args.data_dir, args.years)

if __name__ == "__main__":
    main()