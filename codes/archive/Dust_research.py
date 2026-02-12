import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs # For geographical plots

# --- Replace 'your_file.nc' with the actual path to your file ---
# Example if in Google Drive:
file_path = '/home/omar/Documents/Dust/20250529_3H_MEDIAN.nc'

# Example if uploaded directly to Colab session:
# file_path = 'your_file.nc' # If it's in the root /content/ directory

try:
    ds = xr.open_dataset(file_path)
    print("NetCDF file opened successfully!")
    print(ds) # Print a summary of the dataset
except FileNotFoundError:
    print(f"Error: File not found at {file_path}")
except Exception as e:
    print(f"An error occurred while opening the file: {e}")






import xarray as xr
import pandas as pd

# Open the NetCDF file
#ds = xr.open_dataset('your_file.nc')  # Replace with your actual file path
print("NetCDF file opened successfully!")
print(ds)

# Select the variables you want to export (or use all)
# For example, to export OD550_DUST for the first time step:
#df = ds['OD550_DUST'].isel(time=0).to_dataframe()

# Or to export all data (this will be large):
df = ds.to_dataframe()

# Reset index to flatten the multi-index if needed
df = df.reset_index()

# Save to CSV
csv_path = 'dust_data_1.csv'
df.to_csv(csv_path, index=False)
print(f"Data successfully saved to {csv_path}")
