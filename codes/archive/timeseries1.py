import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import glob
from matplotlib.dates import DateFormatter
import matplotlib.dates as mdates
import matplotlib.image as mpimg
from matplotlib.offsetbox import OffsetImage, AnnotationBbox

# Configuration - All inputs provided a priori
folder_path = 'IRQ'
file_pattern = 'dust_map_IRQ_*.csv'
output_plot = 'IRQ/dust_concentration_timeseries.png'
logo_path = 'IRC.png'  # Optional: set to None if no logo
start_date = '2024-01-01'  # Empty string for all data, or use 'YYYY-MM-DD' format
end_date = '2025-06-20'    # Empty string for all data, or use 'YYYY-MM-DD' format

# Initialize empty DataFrame
all_data = pd.DataFrame()

# Process each file
for file_path in glob.glob(os.path.join(folder_path, file_pattern)):
    try:
        filename = os.path.basename(file_path)
        date_str = filename.split('_')[3]
        time_str = filename.split('_')[4].split('.')[0]
        dt = datetime.strptime(f"{date_str}{time_str}", "%Y%m%d%H%M")
        
        df = pd.read_csv(file_path)
        df['datetime'] = dt
        all_data = pd.concat([all_data, df], ignore_index=True)
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

# Check if we have data
if all_data.empty:
    print("No valid data found. Check your file pattern and folder path.")
    exit()

# Convert and filter datetime
all_data['datetime'] = pd.to_datetime(all_data['datetime'])
if start_date:
    start_date = pd.to_datetime(start_date)
    all_data = all_data[all_data['datetime'] >= start_date]
if end_date:
    end_date = pd.to_datetime(end_date)
    all_data = all_data[all_data['datetime'] <= end_date]

# Group and aggregate
time_series = all_data.groupby('datetime')['dust_concentration_ugm3'].median().sort_index()
time_series = time_series.resample('D').median()

# Plotting
plt.figure(figsize=(14, 8))
ax = plt.gca()

# Air quality zones
categories = {
    'Good (0–45)': (0, 45, 'green'),
    'Moderate (45–90)': (45, 90, 'yellow'),
    'Sensitive (90–180)': (90, 180, 'orange'),
    'Unhealthy (180–300)': (180, 300, 'red'),
    'Hazardous (300+)': (300, 500, 'purple')
}

for label, (ymin, ymax, color) in categories.items():
    ax.axhspan(ymin, ymax, facecolor=color, alpha=0.2, label=label)

# Plot time series
time_series.plot(marker='o', linestyle='-', color='blue', linewidth=1.1, markersize=7, ax=ax)

# Axis labels and formatting
plt.title('Daily (Median 3 hours steps) Dust Concentration In IRAQ', fontsize=14, pad=10)
plt.xlabel('Day\n(Data: WMO Barcelona Dust Regional Center & SDSWAS)    ', fontsize=12)
plt.ylabel('Dust Concentration (µg/m³)', fontsize=12)
ax.set_ylim(0, 500)
ax.set_yticks([0, 45, 90, 180, 300, 500])
ax.grid(True, linestyle='--', alpha=0.7)

import matplotlib.dates as mdates

locator = mdates.AutoDateLocator()
formatter = mdates.ConciseDateFormatter(locator)

ax.xaxis.set_major_locator(locator)
ax.xaxis.set_major_formatter(formatter)
plt.xticks(rotation=45)

# Add logo (optional)
if logo_path and os.path.exists(logo_path):
    logo_img = mpimg.imread(logo_path)
    imagebox = OffsetImage(logo_img, zoom=0.15)
    ab = AnnotationBbox(imagebox, (0.95, 0.93), frameon=False, xycoords='axes fraction')
    ax.add_artist(ab)

# Add legend and author info
handles, labels = ax.get_legend_handles_labels()
handles.append(plt.Line2D([0], [0], color='white', label="Produced By: Dr. Omar Althuwaynee\nomar.faisel@gmail.com"))
plt.legend(handles=handles, loc='upper right', bbox_to_anchor=(1, 1))

# Save and show
plt.tight_layout()
plt.savefig(output_plot, dpi=300, bbox_inches='tight')
print(f"Plot saved as {output_plot}")
plt.show()
