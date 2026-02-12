import xarray as xr
import pandas as pd
 
# Open the dataset
ds = xr.open_dataset(
    "2025053012_3H_SDSWAS_NMMB-BSC-v2_OPER.grb2",
    engine="cfgrib",
    filter_by_keys={"typeOfLevel": "atmosphere"}  # or "hybrid"
)

# Select surface data (atmosphere=0) at first time step
dust_surface = ds['unknown'].isel(step=0, atmosphere=0)

# Convert to DataFrame
df = dust_surface.to_dataframe(name='dust_concentration')

# Reset index to have latitude and longitude as columns
df = df.reset_index()

# Save to CSV
df.to_csv('dust_surface_map.csv', index=False)
print("Map data saved to dust_surface_map.csv")



# Optional: Display the first few rows of the CSV
print(df.head())
