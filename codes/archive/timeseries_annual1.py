import os
import glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- Configuration ---
folder_path = 'IRQ'
file_pattern = 'dust_map_IRQ_*.csv'
output_plot = 'IRQ/dust_concentration_timeseries_annual.png'
logo_path = 'IRC.png'  # Set to None if no logo
start_date = '2020-01-01'
end_date = '2024-12-31'

# --- File processing function ---
def process_file(file_path):
    try:
        filename = os.path.basename(file_path)
        parts = filename.split('_')
        dt = datetime.strptime(f"{parts[3]}{parts[4].split('.')[0]}", "%Y%m%d%H%M")
        
        df = pd.read_csv(file_path)
        if 'dust_concentration_ugm3' not in df.columns:
            print(f"Warning: 'dust_concentration_ugm3' column missing in {file_path}")
            return None
        
        df['datetime'] = dt
        return df[['datetime', 'dust_concentration_ugm3']]
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None

# --- Load data in parallel ---
def load_data_parallel():
    file_paths = glob.glob(os.path.join(folder_path, file_pattern))
    with ThreadPoolExecutor() as executor:
        dfs = list(executor.map(process_file, file_paths))
    return pd.concat([df for df in dfs if df is not None], ignore_index=True)

# --- Main execution ---
print("Loading data...")
all_data = load_data_parallel()

if all_data.empty:
    print("No valid data found. Check your file pattern and folder path.")
    exit()

# Convert datetime and filter by date range
print("Processing data...")
all_data['datetime'] = pd.to_datetime(all_data['datetime'])
start_date = pd.to_datetime(start_date) if start_date else None
end_date = pd.to_datetime(end_date) if end_date else None
if start_date:
    all_data = all_data[all_data['datetime'] >= start_date]
if end_date:
    all_data = all_data[all_data['datetime'] <= end_date]

# Add year and day-of-year
all_data['year'] = all_data['datetime'].dt.year
all_data['day_of_year'] = all_data['datetime'].dt.dayofyear

# Compute daily median
print("Calculating daily medians...")
groups = all_data.groupby(['year', 'day_of_year'])['dust_concentration_ugm3']
daily_median = groups.median().reset_index()

# --- Plotting ---
print("Creating plot...")
plt.figure(figsize=(14, 8))
ax = plt.gca()

# Add air quality zones
categories = [
    ('Good (0–45)', 0, 45, '#ccffcc'),  #green
    ('Moderate (45–90)', 45, 90, '#ffffcc'),  #yellow
    ('Sensitive (90–180)', 90, 180, '#ffbf80'), #orange
    ('Unhealthy (180–300)', 180, 300, '#ffb3b3'), #red
    ('Hazardous (300+)', 300, 500, '#ffb3ff')   #purple
]
for label, ymin, ymax, color in categories:
    ax.axhspan(ymin, ymax, facecolor=color, alpha=0.2, label=label)

# Plot each year's data
years = sorted(daily_median['year'].unique())
colors = plt.cm.tab10(np.linspace(0, 1, len(years)))
for i, year in enumerate(years):
    year_data = daily_median[daily_median['year'] == year]
    ax.plot(
        year_data['day_of_year'], year_data['dust_concentration_ugm3'],
        color=colors[i], linestyle='-', linewidth=0.8,
        markersize=2, label=str(year)
    )

# Labels and ticks
plt.title('Daily (Median 3-hour steps) Dust Concentration in IRAQ', fontsize=14, pad=10)
plt.xlabel('Day of Year\n(Data: WMO Barcelona Dust Regional Center & SDSWAS)', fontsize=12)
plt.ylabel('Dust Concentration (µg/m³)', fontsize=12)
ax.set_ylim(0, 500)
ax.set_yticks([0, 45, 90, 180, 300, 500])
ax.grid(True, linestyle='--', alpha=0.7)

month_days = [1, 32, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335]
month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
ax.set_xticks(month_days)
ax.set_xticklabels(month_names)
plt.xticks(rotation=45)

# Optional logo
if logo_path and os.path.exists(logo_path):
    logo_img = mpimg.imread(logo_path)
    imagebox = OffsetImage(logo_img, zoom=0.15)
    ab = AnnotationBbox(imagebox, (0.95, 0.83), frameon=False, xycoords='axes fraction')
    ax.add_artist(ab)

# Create legend
year_handles = [plt.Line2D([], [], color=colors[i], marker='o', linestyle='-', markersize=4, label=str(year))
                for i, year in enumerate(years)]
category_handles = [plt.Rectangle((0, 0), 1, 1, fc=color, alpha=0.2, label=label)
                    for label, _, _, color in categories]
author_handle = plt.Line2D([], [], color='white', label="Produced By: Dr. Omar Althuwaynee\nomar.faisel@gmail.com")
plt.legend(handles=year_handles + category_handles + [author_handle],
           loc='upper right', bbox_to_anchor=(1, 1))

# Save and close
plt.tight_layout()
plt.savefig(output_plot, dpi=300, bbox_inches='tight')
print(f"Plot saved as {output_plot}")
plt.close()

