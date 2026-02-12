import xarray as xr
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import os
from datetime import datetime, timedelta
import logging
import sys

# Configuration
PARENT_DIR = '/home/omar/Documents/Dust/'
PROCESSED_DIR = os.path.join(PARENT_DIR, 'processed_csv')
os.makedirs(PROCESSED_DIR, exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dust_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Track processed timestamps to avoid duplicates
processed_timestamps = set()

def check_netcdf_backends():
    try:
        import netCDF4
        return True
    except ImportError:
        try:
            import h5netcdf
            return True
        except ImportError:
            return False

if not check_netcdf_backends():
    raise ImportError("Required NetCDF backends not found")

def apply_shapefile_mask(data, shapefile_path=None, shape_id=None):
    if shapefile_path is None:
        return data, "Full Region"
    
    try:
        gdf = gpd.read_file(shapefile_path)
    except Exception as e:
        logger.error(f"Error loading shapefile: {str(e)}")
        return data, "Full Region"
    
    if shape_id is not None:
        try:
            shape_id = int(shape_id)
            mask = gdf[gdf['ID_0'] == shape_id]
            if len(mask) == 0:
                return data, "Full Region"
            region_name = mask.iloc[0]['ISO']
        except Exception as e:
            return data, "Full Region"
    else:
        mask = gdf
        region_name = "All_Iraq_Provinces"
    
    try:
        lon, lat = np.meshgrid(data.longitude, data.latitude)
        points = np.column_stack((lon.flatten(), lat.flatten()))
        mask_array = np.zeros(len(points), dtype=bool)
        
        for polygon in mask.geometry:
            mask_array |= np.array([polygon.contains(Point(x, y)) for x, y in points])
        
        mask_array = mask_array.reshape(data.shape)
        masked_data = data.where(mask_array)
    except Exception as e:
        return data, region_name
    
    return masked_data, region_name

# Shapefile paths
province_shapefile = 'IRQ_adm/IRQ_adm1.shp'
shape_id = 108  # Kirkuk AlTameem province

# Get all IRQ_20* folders sorted by date (oldest first)
input_folders = sorted(
    [f for f in os.listdir(PARENT_DIR) if f.startswith('IRQ_2026') and os.path.isdir(os.path.join(PARENT_DIR, f))],
    key=lambda x: (int(x.split('_')[1]), int(x.split('_')[2]))  # Sort by YEAR, MONTH
)

if not input_folders:
    logger.error(f"No IRQ_2025* folders found")
    sys.exit(1)

for folder in input_folders:
    INPUT_DATA_DIR = os.path.join(PARENT_DIR, folder)
    logger.info(f"Processing folder: {INPUT_DATA_DIR}")
    
    # Get all .nc files sorted by date (oldest first)
    nc_files = sorted(
        [f for f in os.listdir(INPUT_DATA_DIR) if f.endswith('.nc')],
        key=lambda x: (int(x[:8]))  # Sort by YYYYMMDD prefix
    )
    
    if not nc_files:
        logger.warning(f"No .nc files found in directory: {INPUT_DATA_DIR}")
        continue
    
    for filename in nc_files:
        try:
            full_path = os.path.join(INPUT_DATA_DIR, filename)
            forecast_date = filename[:8]  # YYYYMMDD from filename
            logger.info(f"Processing forecast initialized on: {forecast_date}")
            
            ds = xr.open_dataset(full_path, engine='netcdf4')
            
            if 'SCONC_DUST' not in ds:
                ds.close()
                continue
            
            # Convert units
            ds['SCONC_DUST'] = ds['SCONC_DUST'] * 1e9
            
            # Process each time step
            for i, time_step in enumerate(ds.time):
                try:
                    # Get UTC time from data
                    time_dt = pd.to_datetime(time_step.values)
                    
                    # Create unique timestamp key (UTC)
                    timestamp_key = time_dt.strftime("%Y%m%d_%H%M")
                    
                    # Skip if we've already processed a newer forecast for this timestamp
                    if timestamp_key in processed_timestamps:
                        logger.debug(f"Skipping {timestamp_key} - already processed by newer forecast")
                        continue
                    
                    # Convert to Iraq time for output
                    iraq_dt = time_dt + timedelta(hours=3)
                    time_str = iraq_dt.strftime("%Y-%b-%d %H:00") + " (UTC+3)"
                    logger.info(f"Processing: {time_str}")
                    
                    # Extract and mask data
                    dust_data = ds['SCONC_DUST'].isel(time=i)
                    masked_data, region_name = apply_shapefile_mask(dust_data, province_shapefile, shape_id)
                    
                    # Create output directory
                    region_dir = os.path.join(PROCESSED_DIR, region_name)
                    os.makedirs(region_dir, exist_ok=True)
                    
                    # Create filename with forecast initialization info
                    output_filename = f"dust_{region_name}_{iraq_dt.strftime('%Y%m%d_%H%M')}_init{forecast_date}.csv"
                    
                    # Convert to DataFrame and clean
                    df = masked_data.to_dataframe(name='dust_concentration_ugm3').reset_index()
                    df = df.dropna(subset=['dust_concentration_ugm3'])
                    df['dust_concentration_ugm3'] = df['dust_concentration_ugm3'].round(0).astype(int)
                    df['dust_concentration_ugm3'] = df['dust_concentration_ugm3'].clip(lower=0)
                    
                    # Add metadata columns
                    df['forecast_initialization'] = forecast_date
                    df['forecast_hour'] = int((time_dt - pd.Timestamp(forecast_date)).total_seconds() / 3600)
                    df['data_source'] = filename
                    
                    # Save CSV
                    csv_path = os.path.join(region_dir, output_filename)
                    df.to_csv(csv_path, index=False)
                    
                    # Mark this timestamp as processed
                    processed_timestamps.add(timestamp_key)
                    logger.info(f"Saved: {output_filename}")
                    
                except Exception as e:
                    logger.error(f"Error processing timestep: {str(e)}")
                    continue
            
            ds.close()
            
        except Exception as e:
            logger.error(f"Error processing {filename}: {str(e)}")
            continue

# Create summary of processed data
summary_file = os.path.join(PROCESSED_DIR, 'processing_summary.txt')
with open(summary_file, 'w') as f:
    f.write(f"Total unique timestamps processed: {len(processed_timestamps)}\n")
    f.write(f"Processed timestamps:\n")
    for ts in sorted(processed_timestamps):
        f.write(f"  {ts}\n")

logger.info(f"Processing complete. Summary saved to: {summary_file}")
