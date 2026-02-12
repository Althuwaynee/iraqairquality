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
#filename = "20250529_3H_MEDIAN.nc"



# Define user credentials
USERNAME = "omar.althuwaynee"           # <-- replace with your actual username
PASSWORD = "nah5MooG"       # <-- replace with your actual password




import xml.etree.ElementTree as ET
import requests

CATALOG_URL = "https://dust.aemet.es/thredds/catalog/restrictedDataRoot/MULTI-MODEL/latest/catalog.xml"
BASE_DOWNLOAD_URL = "https://dust.aemet.es/thredds/fileServer/restrictedDataRoot/MULTI-MODEL/latest/"


logger.info("Fetching latest catalog XML from AEMET...")
response = requests.get(CATALOG_URL, auth=(USERNAME, PASSWORD))

if response.status_code != 200:
    logger.error(f"Failed to access catalog: {response.status_code}")
    raise Exception("Failed to get catalog.xml from AEMET")

root = ET.fromstring(response.content)
namespace = {'ns': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}

# Find the latest .nc file in the catalog
latest_nc_file = None
for dataset in root.findall('.//ns:dataset', namespace):
    name = dataset.get('name')
    if name and name.endswith('_3H_MEDIAN.nc'):
        latest_nc_file = name
        break

if not latest_nc_file:
    logger.error("No valid NetCDF file found in latest catalog.")
    raise Exception("No _3H_MEDIAN.nc file found.")

logger.info(f"Latest file identified: {latest_nc_file}")

# Now download this file
download_url = BASE_DOWNLOAD_URL + latest_nc_file
logger.info(f"Downloading file: {download_url}")

subprocess.run(
    [
        "wget",
        "--user", USERNAME,
        "--password", PASSWORD,
        "-O", latest_nc_file,
        download_url
    ],
    check=True
)

filename = latest_nc_file
logger.info(f"Download successful. File saved as: {filename}")









try:
    # Open the NetCDF dataset with explicit engine specification
    ds = xr.open_dataset(filename, engine='netcdf4')
    logger.info("Successfully opened NetCDF file")
    
    # Verify required variables exist
    if 'SCONC_DUST' not in ds:
        raise KeyError("SCONC_DUST variable not found in the NetCDF file")
    
    # Convert units (kg/m³ to µg/m³)
    ds['SCONC_DUST'] = ds['SCONC_DUST'] * 1e9
    logger.info("Converted dust concentrations to µg/m³")
    
except Exception as e:
    logger.error(f"Error processing NetCDF file: {str(e)}")
    raise

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
            mask = gdf[gdf['ID_1'] == shape_id]
            if len(mask) == 0:
                logger.warning(f"Shape ID {shape_id} not found, using full region")
                return data, "Full Region", None
            region_name = mask.iloc[0]['NAME_1']
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
            districts = districts[districts['ID_1'] == province_id]
        return districts
    except Exception as e:
        logger.error(f"Error loading districts: {str(e)}")
        return None

# Shapefile paths
province_shapefile = 'IRQ_adm/IRQ_adm1.shp'
district_shapefile = 'IRQ_adm/IRQ_adm2.shp'
shape_id = 17  # Salah ad-Din province

        # Create output directory if it doesn't exist


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
                    text=row['NAME_2'],
                    xy=(row.geometry.centroid.x, row.geometry.centroid.y),
                    ha='center',
                    va='center',
                    fontsize=8,
                    color='black',
                    bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7)
                )
        
        # Set title with timestamp and region
        title = (f"Informatics Research Centre in IRAQ - Surface Dust Concentration \n"
                 f"{region_name} Province - Iraq | {time_str} ")
        plt.title(title, fontsize=11, pad=19)
        
        # Zoom to province bounds if available
        if bounds is not None:
            plt.xlim(bounds[0]-0.5, bounds[2]+0.5)
            plt.ylim(bounds[1]-0.5, bounds[3]+0.5)
        
        plt.grid(alpha=0.3, linestyle='--')
        
        # Add custom legend for categories
        category_labels = [
        #   ("Good / Low Impact", 'green'),
        #   ("Moderate / Elevated", 'yellow'),
        #  ("Unhealthy for Sensitive Groups", 'orange'),
        #   ("Unhealthy / Very High", 'red'),
        #    ("Hazardous / Extreme", 'black')
        ]
        legend_elements = [Patch(facecolor=color, edgecolor='black', label=label) for label, color in category_labels[:-1]]
        # Create custom legend for districts and user info
        # Prepare full legend with categories, boundaries, and author info
        #legend_elements = [Patch(facecolor=color, edgecolor='black', label=label) for label, color in category_labels[:-1]]
        #legend_elements.append(Patch(facecolor='none', edgecolor='black', linewidth=0.5, label=category_labels[-1][0]))
        legend_elements.append(Patch(facecolor='none', edgecolor='black', linewidth=0.5, label='District Boundaries'))
        legend_elements.append(Patch(facecolor='none', edgecolor='none', label='Produced by: Dr. Omar Althuwaynee'))
        legend_elements.append(Patch(facecolor='none', edgecolor='none', label='Email: omar.faisel@gmail.com'))
        legend_elements.append(Patch(facecolor='none', edgecolor='none', label='Data: WMO Barcelona Dust Regional Center & SDS-WAS)'))

        #plt.legend(handles=legend_elements, loc='upper right', frameon=True, title='Legend')



        #legend_elements.append(Patch(facecolor='none', edgecolor='black', linewidth=0.5, label=category_labels[-1][0]))
        #plt.legend(handles=legend_elements, loc='lower left', title="Air Quality Levels", fontsize=8, title_fontsize=9)
        plt.legend(handles=legend_elements, loc='lower left', fontsize=8, title_fontsize=9)
        


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
