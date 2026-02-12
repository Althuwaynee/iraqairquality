import xarray as xr
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
import os
from matplotlib.colors import LinearSegmentedColormap, BoundaryNorm, ListedColormap
from datetime import datetime, timedelta
import logging
from matplotlib.patches import Patch

# [Previous imports and setup code remains the same until colormap definition]






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

# Create custom colormap (light blue -> light yellow -> brown -> dark brown)
colors = [(0.6, 0.85, 1.0), (1.0, 1.0, 0.7), (0.7, 0.5, 0.3), (0.4, 0.2, 0.1)]
cmap_name = 'dust_gradient'
dust_cmap = LinearSegmentedColormap.from_list(cmap_name, colors)

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
        return data, region_name, bounds
    
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





# [Previous imports and setup code remains the same until plotting]

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
        
        # Define health impact categories
        categories = {
            'Good/Low': {'color': '#00E400', 'min': 0, 'max': 45},
            'Moderate': {'color': '#FFFF00', 'min': 45, 'max': 90},
            'Unhealthy/Sensitive': {'color': '#FF7E00', 'min': 90, 'max': 180},
            'Unhealthy': {'color': '#FF0000', 'min': 180, 'max': 300},
            'Hazardous': {'color': '#8F3F97', 'min': 300, 'max': 500}
        }

        # Create colormap and normalization
        colors = [cat['color'] for cat in categories.values()]
        bounds = [0] + [cat['max'] for cat in categories.values()][:-1] + [500]
        cmap = ListedColormap(colors)
        norm = BoundaryNorm(bounds, cmap.N)
        
        # Create figure with adjusted size for legend
        fig, ax = plt.subplots(figsize=(12, 10))
        
        # Plot the data with discrete colormap
        plot = masked_data.plot(ax=ax, cmap=cmap, norm=norm, add_colorbar=False)
        
        # Add district boundaries if available
        if districts is not None:
            districts.boundary.plot(
                ax=ax, 
                color='black', 
                linewidth=0.5,
                label='District Boundaries'
            )
            
            # Add district labels at centroids
            for idx, row in districts.iterrows():
                ax.annotate(
                    text=row['NAME_2'],
                    xy=(row.geometry.centroid.x, row.geometry.centroid.y),
                    ha='center',
                    va='center',
                    fontsize=8,
                    color='black',
                    bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7)
                )
        
        # Set title with timestamp and region
        ax.set_title(
            f"Surface Dust Concentration - {region_name} Province-Iraq\n"
            f"{time_str} | Data valid: {time_dt.strftime('%Y-%b-%d %H:00 UTC')}",
            fontsize=12, pad=20
        )
        
        # Zoom to province bounds if available
        if bounds is not None:
            ax.set_xlim(bounds[0]-0.5, bounds[2]+0.5)
            ax.set_ylim(bounds[1]-0.5, bounds[3]+0.5)
        
        # Add grid lines
        ax.grid(alpha=0.3, linestyle='--')
        
        # Create custom colorbar
        cbar = fig.colorbar(plot, ax=ax, extend='max', spacing='uniform', 
                          ticks=[(bounds[i]+bounds[i+1])/2 for i in range(len(bounds)-1)])
        cbar.set_label('Dust Concentration (µg/m³)')
        cbar.ax.set_yticklabels([
            f"{name}\n{cat['min']}-{cat['max']}" 
            for name, cat in categories.items()
        ])
        
        # Create health implications legend
        health_legend_elements = [
            Patch(facecolor=cat['color'], edgecolor='black', 
                 label=f"{name} ({cat['min']}-{cat['max']} µg/m³)\n{health_impact}")
            for name, cat, health_impact in zip(
                categories.keys(),
                categories.values(),
                [
                    "Minimal health risk",
                    "Acceptable for most",
                    "Sensitive groups at risk",
                    "Everyone may be affected",
                    "Health emergency"
                ]
            )
        ]
        
        # Create district boundaries legend
        if districts is not None:
            district_legend_elements = [
                Patch(facecolor='none', edgecolor='black', linewidth=0.5, 
                     label='District Boundaries')
            ]
        
        # Position the legends
        if districts is not None:
            leg1 = ax.legend(handles=district_legend_elements, 
                            loc='upper right',
                            framealpha=1)
            ax.add_artist(leg1)
        
        leg2 = ax.legend(handles=health_legend_elements,
                        loc='lower center',
                        bbox_to_anchor=(0.5, -0.25),
                        ncol=2,
                        fontsize=9,
                        title="Health Implications",
                        title_fontsize=10,
                        framealpha=1)
        
        # Adjust layout to make room for legend
        plt.tight_layout(rect=[0, 0.1, 1, 1])
        
        # Save and close
        output_filename = f"dust_map_{region_name.replace(' ', '_')}_{iraq_dt.strftime('%Y%m%d_%H%M')}"
        plt.savefig(
            f'output_maps/{output_filename}.png',
            dpi=300,
            bbox_inches='tight',
            facecolor='white'
        )
        plt.close()
        
        # Save data to CSV
        df = masked_data.to_dataframe(name='dust_concentration_ugm3').reset_index()
        df.to_csv(f'output_maps/{output_filename}.csv', index=False)
        
        logger.info(f"Saved outputs for time step: {output_filename}")
        
    except Exception as e:
        logger.error(f"Error processing time step {time_step.values}: {str(e)}")
        continue

logger.info("Processing complete for all time steps")









































