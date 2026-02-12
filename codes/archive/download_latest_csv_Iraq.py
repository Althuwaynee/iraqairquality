# grb2_to_map_iraq_all_boundary.py
import xarray as xr
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import os
from matplotlib.colors import ListedColormap, BoundaryNorm
from datetime import datetime, timedelta
import logging
from matplotlib.patches import Patch
import requests
import xml.etree.ElementTree as ET
import ssl
import certifi
import os
import glob

files = glob.glob('latest/*')
for f in files:
    os.remove(f)

# SSL context setup
ssl._create_default_https_context = ssl._create_unverified_context

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('iraq_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# Configuration
USERNAME = "omar.althuwaynee"
PASSWORD = "nah5MooG"
CATALOG_URL = "https://dust.aemet.es/thredds/catalog/restrictedDataRoot/MULTI-MODEL/latest/catalog.xml"
BASE_DOWNLOAD_URL = "https://dust.aemet.es/thredds/fileServer/restrictedDataRoot/MULTI-MODEL/latest/"

# Shapefile for all of Iraq
SHAPEFILE = 'IRQ_adm/IRQ_adm0.shp'  # Country boundary

def download_latest_file():
    """Download the latest NetCDF file from AEMET"""
    logger.info("Fetching latest catalog...")
    
    try:
        # Use certifi for SSL verification
        response = requests.get(
            CATALOG_URL,
            auth=(USERNAME, PASSWORD),
            verify=certifi.where(),
            timeout=30
        )
        response.raise_for_status()
    except requests.exceptions.SSLError:
        # Fallback to verify=False if SSL still fails
        logger.warning("SSL verification failed, using verify=False")
        response = requests.get(
            CATALOG_URL,
            auth=(USERNAME, PASSWORD),
            verify=False,
            timeout=30
        )
    
    if response.status_code != 200:
        logger.error(f"Failed to access catalog: {response.status_code}")
        return None
    
    root = ET.fromstring(response.content)
    namespace = {'ns': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}
    
    # Find the latest file
    latest_file = None
    for dataset in root.findall('.//ns:dataset', namespace):
        name = dataset.get('name')
        if name and name.endswith('_3H_MEDIAN.nc'):
            latest_file = name
            break
    
    if not latest_file:
        logger.error("No 3H_MEDIAN.nc file found")
        return None
    
    logger.info(f"Latest file: {latest_file}")
    download_url = BASE_DOWNLOAD_URL + latest_file
    
    # Download the file
    logger.info(f"Downloading: {latest_file}")
    try:
        response = requests.get(
            download_url,
            auth=(USERNAME, PASSWORD),
            verify=False,  # Use False to bypass SSL issues
            stream=True,
            timeout=60
        )
        response.raise_for_status()
        
        with open(latest_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        logger.info(f"Downloaded: {latest_file}")
        return latest_file
    except Exception as e:
        logger.error(f"Download failed: {str(e)}")
        return None

def create_iraq_boundary_mask(data, shapefile_path):
    """Create mask for entire Iraq boundary"""
    try:
        gdf = gpd.read_file(shapefile_path)
        logger.info(f"Loaded shapefile: {shapefile_path}")
        
        # Combine all geometries (for country boundary)
        combined_geom = gdf.unary_union
        
        # Create mask
        lon, lat = np.meshgrid(data.longitude, data.latitude)
        points = np.column_stack((lon.flatten(), lat.flatten()))
        mask_array = np.array([combined_geom.contains(Point(x, y)) for x, y in points])
        mask_array = mask_array.reshape(data.shape)
        
        masked_data = data.where(mask_array)
        return masked_data, "IRQ", gdf.total_bounds
        
    except Exception as e:
        logger.error(f"Error creating mask: {str(e)}")
        return data, "Full_Region", None

def process_netcdf_file(filename):
    """Process NetCDF file for all Iraq"""
    try:
        logger.info(f"Processing: {filename}")
        ds = xr.open_dataset(filename, engine='netcdf4')
        
        if 'SCONC_DUST' not in ds:
            logger.error("SCONC_DUST variable not found")
            return
        
        # Convert units
        ds['SCONC_DUST'] = ds['SCONC_DUST'] * 1e9
        logger.info("Converted to µg/m³")
        
        # Define categories
#        boundaries = [0, 45, 90, 180, 300, 500]
#        colors = ['#99ff66', '#ffff99', '#ffcc80', '#804d00', '#ff3300']
#        cmap = ListedColormap(colors)
#        norm = BoundaryNorm(boundaries, ncolors=cmap.N, clip=True)
        
        # Process each time step
        for i, time_step in enumerate(ds.time):
            time_dt = pd.to_datetime(time_step.values)
            iraq_dt = time_dt + timedelta(hours=3)
            time_str = iraq_dt.strftime("%Y%m%d_%H%M")
            
            logger.info(f"Processing: {time_str}")
            
            # Get data
            dust_data = ds['SCONC_DUST'].isel(time=i)
            
            # Apply Iraq boundary mask
            masked_data, region_name, bounds = create_iraq_boundary_mask(dust_data, SHAPEFILE)
            

            
            # Save
            output_filename = f"latest/dust_map_IRQ_{time_str}"
            os.makedirs('latest', exist_ok=True)
            

            
            # Save CSV
            df = masked_data.to_dataframe(name='dust_concentration_ugm3').reset_index()
                        # Drop rows outside Iraq (where concentration is NaN)
            df = df.dropna(subset=['dust_concentration_ugm3'])
            
            # Convert to integers (round to nearest integer)
            df['dust_concentration_ugm3'] = df['dust_concentration_ugm3'].round(0).astype(int)
            
            # Ensure values are within reasonable range
            df['dust_concentration_ugm3'] = df['dust_concentration_ugm3'].clip(lower=0)
            
            # Add timestamp column
            df['datetime'] = iraq_dt
            
            # Reorder columns
            df = df[['datetime', 'latitude', 'longitude', 'dust_concentration_ugm3']]
            
            df.to_csv(f'{output_filename}.csv', index=False)
            
            logger.info(f"Saved: {output_filename}")
        
        ds.close()
        logger.info("Processing complete")
        
    except Exception as e:
        logger.error(f"Processing error: {str(e)}")

def main():
    """Main function to download and process Iraq data"""
    logger.info("Starting Iraq boundary download...")
    
    # Ensure shapefile exists
    if not os.path.exists(SHAPEFILE):
        logger.error(f"Shapefile not found: {SHAPEFILE}")
        logger.info("Please download Iraq boundary shapefile from:")
        logger.info("https://data.humdata.org/dataset/iraq-administrative-boundaries")
        return
    
    # Download latest file
    nc_file = download_latest_file()
    if nc_file:
        # Process for all Iraq
        process_netcdf_file(nc_file)
        
        # Cleanup
        if os.path.exists(nc_file):
            os.remove(nc_file)
            logger.info(f"Cleaned up: {nc_file}")
    else:
        logger.error("Failed to download file")

if __name__ == "__main__":
    main()
