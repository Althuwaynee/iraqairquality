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
import requests
import certifi
import xml.etree.ElementTree as ET
import sys

lock_file = "/tmp/dust_job.lock"

if os.path.exists(lock_file):
    print("Job already running. Exiting.")
    sys.exit()

open(lock_file, 'w').close()

try:
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

    # Define user credentials
    USERNAME = "omar.althuwaynee"
    PASSWORD = "nah5MooG"

    # Configuration for March 2026 data
    YEAR = "2025"
    MONTH = "06"  # Use 2 digits (example 09 not 9 for September)
    BASE_URL = "https://dust.aemet.es/thredds/fileServer/restrictedDataRoot/MULTI-MODEL"
    
    # Generate all dates in March 2026
    start_date = datetime(2025,5,29)
    end_date = datetime(2025,7,1) #
    date_range = pd.date_range(start_date, end_date)

    # Create directory for March 2026 data
    output_dir = f"IRQ_{YEAR}_{MONTH}"
    os.makedirs(output_dir, exist_ok=True)
    logger.info(f"Created output directory: {output_dir}")

    # Download each day's data
    for single_date in date_range:
        date_str = single_date.strftime("%Y%m%d")
        filename = f"{date_str}_3H_MEDIAN.nc"
        download_url = f"{BASE_URL}/{YEAR}/{MONTH}/{filename}"
        output_path = os.path.join(output_dir, filename)

        # Skip if file already exists
        if os.path.exists(output_path):
            logger.info(f"File already exists, skipping: {filename}")
            continue

        logger.info(f"Downloading file: {filename}")
        try:
            response = requests.get(
                download_url,
                auth=(USERNAME, PASSWORD),
                headers={'User-Agent': 'OmarResearchBot/1.0'},
                verify=False,
                stream=True
            )
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Successfully downloaded {filename}")
        except Exception as e:
            logger.error(f"Failed to download file {filename}: {str(e)}")
            continue

    # Process each downloaded file
    for nc_file in os.listdir(output_dir):
        if nc_file.endswith('.nc'):
            try:
                file_path = os.path.join(output_dir, nc_file)
                logger.info(f"Processing file: {nc_file}")
                ds = xr.open_dataset(file_path)
                
                # Convert units (kg/m³ to µg/m³)
                ds['SCONC_DUST'] = ds['SCONC_DUST'] * 1e9
                logger.info("Converted dust concentrations to µg/m³")

                # Define discrete steps and colors for dust concentration levels
                boundaries = [0, 45, 90, 180, 300, 500]
                colors = ['#99ff66', '#ffff99', '#ffcc80', '#804d00', '#ff3300']
                cmap = ListedColormap(colors)
                norm = BoundaryNorm(boundaries, ncolors=cmap.N, clip=True)

                # Shapefile paths
                province_shapefile = 'IRQ_adm/IRQ_adm1.shp'
                district_shapefile = 'IRQ_adm/IRQ_adm1.shp'
                shape_id = 108  

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

                        # Save and close
                        output_filename = f"dust_map_{region_name.replace(' ', '_')}_{iraq_dt.strftime('%Y%m%d_%H%M')}"
                        plt.savefig(
                            f'{output_dir}/{output_filename}.png',
                            dpi=300,
                            bbox_inches='tight',
                            facecolor='white'
                        )
                        plt.close()
                        
                        # Save data to CSV
                        df = masked_data.to_dataframe(name='dust_concentration_ugm3').reset_index()
                        df.to_csv(f'{output_dir}/{output_filename}.csv', index=False)
                        
                        logger.info(f"Saved outputs for time step: {output_filename}")
                        
                    except Exception as e:
                        logger.error(f"Error processing time step {time_step.values}: {str(e)}")
                        continue

            except Exception as e:
                logger.error(f"Error processing file {nc_file}: {str(e)}")
                continue

    logger.info("Processing complete for all March 2026 data")

finally:
    os.remove(lock_file)

# Helper functions (same as before)
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

def load_districts(shapefile_path, province_id=None):
    try:
        districts = gpd.read_file(shapefile_path)
        if province_id is not None:
            districts = districts[districts['ID_0'] == province_id]
        return districts
    except Exception as e:
        logger.error(f"Error loading districts: {str(e)}")
        return None
