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
output_plot = 'IRQ/dust_concentration_barchart_annual.png'
logo_path = 'IRC.png'  # Set to None if no logo
start_date = '2010-01-01'
end_date = '2025-12-31'

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

# Define concentration categories
categories = [
    ('Good (0–45)', 0, 45),
    ('Moderate (45–90)', 45, 90),
    ('Sensitive (90–180)', 90, 180),
    ('Unhealthy (180–300)', 180, 300),
    ('Hazardous (300+)', 300, 10000)  # Using a large upper bound
]

# Categorize each day
def categorize_day(value):
    for i, (label, lower, upper) in enumerate(categories):
        if lower <= value < upper:
            return i
    return len(categories) - 1  # default to last category

daily_median['category'] = daily_median['dust_concentration_ugm3'].apply(categorize_day)

# Count days in each category per year
yearly_counts = daily_median.groupby(['year', 'category']).size().unstack(fill_value=0)

# --- Plotting ---
print("Creating bar plot...")
plt.figure(figsize=(14, 8))
ax = plt.gca()

# Set up bar positions
years = sorted(daily_median['year'].unique())
n_years = len(years)
n_categories = len(categories)
bar_width = 0.15
x = np.arange(n_years)  # the x locations for the years

# Colors for each category
category_colors = ['green', 'yellow', 'orange', 'red', 'purple']
category_labels = [cat[0] for cat in categories]  # Get the labels from categories

# Plot bars for each category
bars = []
for i in range(n_categories):
    if i in yearly_counts.columns:
        counts = yearly_counts[i].values
    else:
        counts = np.zeros(n_years)
    
    # Adjust x positions for each category
    x_pos = x + i * bar_width - (n_categories * bar_width / 2) + bar_width/2
    
    bar = ax.bar(
        x_pos, counts, 
        width=bar_width, 
        color=category_colors[i], 
        alpha=0.7,
        edgecolor='black',
        linewidth=0.5,
        label=category_labels[i]  # Use the proper label here
    )
    bars.append(bar)
    
    # Add count labels on top of bars
    for j, count in enumerate(counts):
        if count > 0:  # Only label if count > 0
            ax.text(
                x_pos[j], count + 0.5, str(int(count)),
                ha='center', va='bottom', fontsize=8
            )

# Labels and title
plt.title('Annual Dust Concentration in IRAQ (Median Daily Values)', fontsize=14, pad=10)
plt.xlabel('Year\n(Data: WMO Barcelona Dust Regional Center & SDSWAS)', fontsize=12)
plt.ylabel('Number of Days', fontsize=12)
ax.set_xticks(x)
ax.set_xticklabels(years)

# Grid and limits
ax.grid(True, linestyle='--', alpha=0.3, axis='y')
ax.set_ylim(0, max(yearly_counts.max()) * 1.15)

# Optional logo
if logo_path and os.path.exists(logo_path):
    logo_img = mpimg.imread(logo_path)
    imagebox = OffsetImage(logo_img, zoom=0.15)
    ab = AnnotationBbox(imagebox, (0.95, 0.83), frameon=False, xycoords='axes fraction')
    ax.add_artist(ab)

# Create legend - use the proper labels from the categories
legend_handles = [plt.Rectangle((0,0),1,1, color=category_colors[i], alpha=0.7, label=category_labels[i]) 
                 for i in range(n_categories)]
author_handle = plt.Line2D([], [], color='white', label="Produced By: Dr. Omar Althuwaynee\nomar.faisel@gmail.com")
plt.legend(handles=legend_handles + [author_handle],
           loc='upper right', bbox_to_anchor=(1, 1))

# Save and close
plt.tight_layout()
plt.savefig(output_plot, dpi=300, bbox_inches='tight')
print(f"Plot saved as {output_plot}")
plt.close()
