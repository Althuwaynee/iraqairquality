import xarray as xr
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import os
from matplotlib.colors import LinearSegmentedColormap
from datetime import datetime, timedelta
import logging
from matplotlib.patches import Patch

def check_netcdf_backends():
    """Check if required NetCDF backends are available"""
    try:
        import netCDF4
        return True
    except ImportError:
        try:
            import h5netcdf
            return True
        except ImportError:
            return False

# Verify NetCDF backends are available
if not check_netcdf_backends():
    raise ImportError(
        "Required NetCDF backends not found. Please install one of:\n"
        "conda install netCDF4 h5netcdf\n"
        "or\n"
        "pip install netCDF4 h5netcdf"
    )

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

# Create output directory if it doesn't exist
os.makedirs('output_maps', exist_ok=True)
logger.info("Output directory created/verified")

# Process NetCDF file
filename = "20250529_3H_MEDIAN.nc"

try:
    # Parse the datetime from filename (YYYYMMDD)
    file_dt = datetime.strptime(filename.split('_')[0], "%Y%m%d")
    iraq_dt = file_dt + timedelta(hours=3)  # UTC+3 for Iraq
    time_str = iraq_dt.strftime("%Y-%b-%d %H:00") + " (UTC+3)"
    logger.info(f"Processed timestamps - UTC: {file_dt}, Iraq: {iraq_dt}")
except Exception as e:
    logger.error(f"Error processing filename timestamp: {str(e)}")
    raise

try:
    # Open the NetCDF dataset with explicit engine specification
    ds = xr.open_dataset(filename, engine='netcdf4')  # or 'h5netcdf'
    logger.info("Successfully opened NetCDF file")
    
    # Verify required variables exist
    if 'SCONC_DUST' not in ds:
        raise KeyError("SCONC_DUST variable not found in the NetCDF file")
    
    # Convert units (kg/m³ to µg/m³)
    ds['SCONC_DUST'] = ds['SCONC_DUST'] * 1e9
    logger.info("Converted dust concentrations to µg/m³")
    
except Exception as e:
    logger.error(f"Error processing NetCDF file: {str(e)}")
    logger.error("Possible solutions:")
    logger.error("1. Install required backends: conda install netCDF4 h5netcdf")
    logger.error("2. Verify the NetCDF file is not corrupted")
    logger.error("3. Check file path is correct")
    raise

# Rest of your code remains the same...
# [Include all the remaining code from the previous version]
