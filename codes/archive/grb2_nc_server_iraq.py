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
import subprocess
from datetime import datetime
import subprocess
import os
import subprocess
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import matplotlib.image as mpimg
import requests
import certifi
import xml.etree.ElementTree as ET
import requests
import sys

lock_file = "/tmp/dust_job.lock"

if os.path.exists(lock_file):
    print("Job already running. Exiting.")
    sys.exit()

open(lock_file, 'w').close()

try:
    # Your script logic here
    ...
finally:
    os.remove(lock_file)


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
#os.makedirs('{output_filename}', exist_ok=True)
logger.info("Output directory created/verified")

# Process NetCDF file

# Define user credentials
USERNAME = "omar.althuwaynee"           # <-- replace with your actual username
PASSWORD = "nah5MooG"       # <-- replace with your actual password



CATALOG_URL = "https://dust.aemet.es/thredds/catalog/restrictedDataRoot/MULTI-MODEL/latest/catalog.xml"
BASE_DOWNLOAD_URL = "https://dust.aemet.es/thredds/fileServer/restrictedDataRoot/MULTI-MODEL/latest/"
headers = {'User-Agent': 'OmarResearchBot/1.0'}
response = requests.get(CATALOG_URL, headers=headers, auth=(USERNAME, PASSWORD), verify=False)
logger.info("Fetching latest catalog XML from AEMET...")

if response.status_code != 200:
    logger.error(f"Failed to access catalog: {response.status_code}")
    raise Exception("Failed to get catalog.xml from AEMET")

root = ET.fromstring(response.content)
namespace = {'ns': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}

# Find the latest file
latest_file = None
for dataset in root.findall('.//ns:dataset', namespace):
    name = dataset.get('name')
    if name.endswith('_3H_MEDIAN.nc'):
        latest_file = name
        break

if not latest_file:
    logger.error("No 3H_MEDIAN.nc file found in catalog")
    raise Exception("No suitable file found in catalog")

logger.info(f"Latest file identified: {latest_file}")
download_url = BASE_DOWNLOAD_URL + latest_file

# Download the file
logger.info(f"Downloading file: {download_url}")
try:
    response = requests.get(
        download_url,
        auth=(USERNAME, PASSWORD),
        headers=headers,
        verify=False,  # Bypass SSL verification
        stream=True
    )
    response.raise_for_status()
    
    with open(latest_file, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    logger.info(f"Successfully downloaded {latest_file}")
except Exception as e:
    logger.error(f"Failed to download file: {str(e)}")
    raise

# Open the downloaded NetCDF file
logger.info(f"Opening downloaded file: {latest_file}")
try:
    ds = xr.open_dataset(latest_file)
    logger.info("Successfully opened NetCDF file")
except Exception as e:
    logger.error(f"Failed to open NetCDF file: {str(e)}")
    raise
########################################################################

# Define discrete steps and colors for dust concentration levels
boundaries = [0, 45, 90, 180, 300, 500]
colors = ['#99ff66', '#ffff99', '#ffcc80', '#804d00', '#ff3300']
cmap = ListedColormap(colors)
norm = BoundaryNorm(boundaries, ncolors=cmap.N, clip=True)

# Function to apply shapefile mask and get bounds
def apply_shapefile_mask(data, shapefile_path=None, shape_id=None):
    if shapefile_path is None:
        return data, "Full Region", None
    
    try:
        gdf = gpd.read_file(shapefile_path)
    except Exception as e:
        logger.error(f"Error loading shapefile: {str(e)}")
        return data, "Full Region", None
    
    if shape_id is not None:
        try:
            shape_id = int(shape_id)
            mask = gdf[gdf['ID_0'] == shape_id]
            if len(mask) == 0:
                logger.warning(f"Shape ID {shape_id} not found, using full region")
                return data, "Full Region", None
            region_name = mask.iloc[0]['ISO']
            bounds = mask.total_bounds
        except Exception as e:
            logger.error(f"Error processing shape ID: {str(e)}")
            return data, "Full Region", None
    else:
        mask = gdf
        region_name = "All_Iraq_Provinces"
        bounds = mask.total_bounds
    
    try:
        lon, lat = np.meshgrid(data.longitude, data.latitude)
        points = np.column_stack((lon.flatten(), lat.flatten()))
        mask_array = np.zeros(len(points), dtype=bool)
        
        for polygon in mask.geometry:
            mask_array |= np.array([polygon.contains(Point(x, y)) for x, y in points])
        
        mask_array = mask_array.reshape(data.shape)
        masked_data = data.where(mask_array)
    except Exception as e:
        logger.error(f"Error creating mask: {str(e)}")
        return data, region_name, None
    
    return masked_data, region_name, bounds

# Load district boundaries for overlay
def load_districts(shapefile_path, province_id=None):
    try:
        districts = gpd.read_file(shapefile_path)
        if province_id is not None:
            districts = districts[districts['ID_0'] == province_id]
        return districts
    except Exception as e:
        logger.error(f"Error loading districts: {str(e)}")
        return None

# Shapefile paths
province_shapefile = 'IRQ_adm/IRQ_adm1.shp'
district_shapefile = 'IRQ_adm/IRQ_adm1.shp'
shape_id = 108  # Kirkuk AlTameem province

# Create output directory if it doesn't exist

# Convert units (kg/m³ to µg/m³)
ds['SCONC_DUST'] = ds['SCONC_DUST'] * 1e9
logger.info("Converted dust concentrations to µg/m³")
# Process each time step
for i, time_step in enumerate(ds.time):
    try:
        # Get the datetime for this time step
        time_dt = pd.to_datetime(time_step.values)
        iraq_dt = time_dt + timedelta(hours=3)  # UTC+3 for Iraq
        time_str = iraq_dt.strftime("%Y-%b-%d %H:00") + " (UTC+3)"
        
        logger.info(f"Processing time step: {time_str}")
        
        # Select data for this time step
        dust_data = ds['SCONC_DUST'].isel(time=i)
        
        # Apply mask if shapefile provided
        masked_data, region_name, bounds = apply_shapefile_mask(dust_data, province_shapefile, shape_id)
        
        # Load districts for the selected province
        districts = load_districts(district_shapefile, shape_id)
        
        # Create figure
        plt.figure(figsize=(10, 8))
        
        # Create plot with categorized colormap and normalization
        plot = masked_data.plot(cmap=cmap, norm=norm, vmin=0, vmax=500, add_colorbar=False)
        
        # Add colorbar with labels
        tick_positions = boundaries[:-1]  # Use the start of each bin for labeling
        tick_labels = [
            'Good (0–45)', 
            'Moderate (45–90)', 
            'Sensitive (90–180)', 
            'Unhealthy (180–300)', 
            'Hazardous (300–500+)'
        ]

        cbar = plt.colorbar(plot, extend='max', ticks=tick_positions, shrink=0.7, pad=0.02)
        cbar.set_label('Dust Concentration (µg/m³)', fontsize=9)
        cbar.ax.set_yticklabels(tick_labels, fontsize=8)

        
        # Add district boundaries if available
        if districts is not None:
            districts.boundary.plot(
                ax=plt.gca(), 
                color='black', 
                linewidth=0.5,
                label='District Boundaries'
            )
            
            # Add district labels at centroids
            for idx, row in districts.iterrows():
                plt.annotate(
                    text=row['NAME_1'],
                    xy=(row.geometry.centroid.x, row.geometry.centroid.y),
                    ha='center',
                    va='center',
                    fontsize=8,
                    color='black',
                    bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7)
                )
        
        # Set title with timestamp and region
        title = (f"Informatics Research Centre in IRAQ - Surface Dust Concentration \n"
                 f"{region_name} - Iraq Republic | {time_str} ")
        plt.title(title, fontsize=11, pad=19)
        
        # Zoom to province bounds if available
        if bounds is not None:
            plt.xlim(bounds[0]-0.5, bounds[2]+0.5)
            plt.ylim(bounds[1]-0.5, bounds[3]+0.5)
        
        plt.grid(alpha=0.3, linestyle='--')
        
        # Add custom legend
        legend_elements = [
            Patch(facecolor='none', edgecolor='black', linewidth=0.5, label='Provinces boundaries'),
            Patch(facecolor='none', edgecolor='none', label='Produced by: Dr. Omar Althuwaynee'),
            Patch(facecolor='none', edgecolor='none', label='Email: omar.faisel@gmail.com'),
            Patch(facecolor='none', edgecolor='none', label='Data: WMO BarcelonaDustRegionalCenter & SDSWAS)')
        ]
        
        plt.legend(handles=legend_elements, loc='lower left', fontsize=7, title_fontsize=8)

        # Load logo image (replace with your actual path)
        logo_path = 'IRC.png'  # Ensure this is relative or absolute path
        try:
            logo_img = mpimg.imread(logo_path)
            imagebox = OffsetImage(logo_img, zoom=0.15)  # Adjust zoom as needed

            # Position the logo (coordinates in axis fraction)
            ab = AnnotationBbox(
                imagebox, (0.11, 0.18),  # (x, y) in axis fraction (near lower right)
                frameon=False,
                xycoords='axes fraction'
            )
            plt.gca().add_artist(ab)

            # Add a dummy label for the logo in the legend
            legend_elements.append(Patch(facecolor='none', edgecolor='none', label='Affiliation: Omar Althuwaynee\nomar.althuwaynee@gmail.com'))

        except Exception as e:
            logger.warning(f"Could not load logo: {e}")



        
        
        
        # Save and close
        output_filename = f"dust_map_{region_name.replace(' ', '_')}_{iraq_dt.strftime('%Y%m%d_%H%M')}"
        os.makedirs(region_name, exist_ok=True)
        logger.info("Output directory created/verified")


 
        
        
        plt.savefig(
            f'{region_name}/{output_filename}.png',
            dpi=300,
            bbox_inches='tight',
            facecolor='white'
        )
        plt.close()
        
        # Save data to CSV
        df = masked_data.to_dataframe(name='dust_concentration_ugm3').reset_index()
        df.to_csv(f'{region_name}/{output_filename}.csv', index=False)
        
        logger.info(f"Saved outputs for time step: {output_filename}")
        
    except Exception as e:
        logger.error(f"Error processing time step {time_step.values}: {str(e)}")
        continue

logger.info("Processing complete for all time steps")
