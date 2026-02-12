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
output_plot = 'IRQ/dust_concentration_timeseries_Aggregated_annual.png'
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






# --- NEW: Resample to monthly/quarterly instead of daily median ---
print("Resampling to monthly averages...")
all_data.set_index('datetime', inplace=True)
resampled_data = all_data['dust_concentration_ugm3'].resample('M').mean().reset_index()  # 'M' for monthly, 'Q' for quarterly

# Extract year and month/day for plotting
resampled_data['year'] = resampled_data['datetime'].dt.year
resampled_data['month'] = resampled_data['datetime'].dt.month  # Use 'quarter' for quarterly

# --- Plotting (Updated for resampled data) ---
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






# Plot each year's resampled data
years = sorted(resampled_data['year'].unique())
colors = plt.cm.tab10(np.linspace(0, 1, len(years)))

for i, year in enumerate(years):
    year_data = resampled_data[resampled_data['year'] == year]
    ax.plot(
        year_data['month'],  # Use 'quarter' for quarterly
        year_data['dust_concentration_ugm3'],
        color=colors[i], linestyle='-', linewidth=1.5,  # Thicker line for clarity
        marker='o', markersize=4, label=str(year)
    )

# --- Updated Labels and Ticks ---
plt.title('Monthly Average Dust Concentration in IRAQ', fontsize=14, pad=10)  # Updated title
plt.xlabel('Month', fontsize=12)  # Simplified label
plt.ylabel('Dust Concentration (µg/m³)', fontsize=12)

# X-axis for months (adjust for quarters if needed)
ax.set_xticks(range(1, 13))
ax.set_xticklabels(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                    'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'])
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

