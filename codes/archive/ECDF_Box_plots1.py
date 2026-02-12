import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.ticker import PercentFormatter
from datetime import datetime
import glob
import matplotlib.image as mpimg
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
from matplotlib.lines import Line2D
from concurrent.futures import ThreadPoolExecutor

def load_dust_data():
    """Optimized data loading function"""
    folder_path = 'IRQ'
    file_pattern = 'dust_map_IRQ_*.csv'
    start_date = pd.to_datetime('2010-01-01') if '2010-01-01' else None
    end_date = pd.to_datetime('2024-12-31') if '2024-12-31' else None

    def process_file(file_path):
        try:
            filename = os.path.basename(file_path)
            parts = filename.split('_')
            dt = datetime.strptime(f"{parts[3]}{parts[4].split('.')[0]}", "%Y%m%d%H%M")
            
            # Use low_memory=False and specify dtype if possible
            df = pd.read_csv(file_path, low_memory=False)
            df['datetime'] = dt
            return df
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            return None

    # Use parallel processing for file loading
    file_paths = glob.glob(os.path.join(folder_path, file_pattern))
    with ThreadPoolExecutor() as executor:
        dfs = list(executor.map(process_file, file_paths))
    
    # Filter out None values and concatenate
    all_data = pd.concat([df for df in dfs if df is not None], ignore_index=True)

    if all_data.empty:
        raise ValueError("No valid data found. Check your file pattern and folder path.")

    # Filter by date
    if start_date:
        all_data = all_data[all_data['datetime'] >= start_date]
    if end_date:
        all_data = all_data[all_data['datetime'] <= end_date]

    return all_data

def prepare_plot_data(all_data):
    """Optimized data preparation"""
    # Vectorized year extraction
    plot_data = all_data[['datetime', 'dust_concentration_ugm3']].copy()
    plot_data['year'] = plot_data['datetime'].dt.year
    
    # Vectorized filtering
    plot_data = plot_data[
        plot_data['dust_concentration_ugm3'].notna() & 
        (plot_data['dust_concentration_ugm3'] >= 0)
    ]
    
    return plot_data[['year', 'dust_concentration_ugm3']]

def plot_violin_boxplot(df, output_path="IRQ/dust_stats_violin.png", logo_path="IRC.png"):
    plt.figure(figsize=(14, 8))
    
    # Create violin plot
    ax = sns.violinplot(
        x='year', 
        y='dust_concentration_ugm3',
        hue='year',
        data=df,
        palette="tab10",
        cut=0,
        inner=None,
        legend=False
    )
    
    # Add boxplot inside violins
    sns.boxplot(
        x='year',
        y='dust_concentration_ugm3',
        data=df,
        width=0.15,
        boxprops={'facecolor':'none'},
        medianprops={'color':'white', 'linewidth':2},
        whiskerprops={'color':'black', 'linewidth':1.5},
        capprops={'color':'black', 'linewidth':1.5},
        ax=ax
    )
    
    # Add air quality thresholds
    thresholds = [45, 90, 180, 300]
    colors = ['green', 'yellow', 'orange', 'red']
    for val, color in zip(thresholds, colors):
        ax.axhline(val, ls='--', color=color, alpha=0.3, lw=1)
        ax.text(ax.get_xlim()[1]+0.2, val, 
                f"{val} µg/m³", 
                va='center', ha='left',
                bbox=dict(facecolor='white', alpha=0.8))
    
    # Customize plot
    ax.set_title('Yearly Dust Concentration Distribution  | IRAQ\n(Violin + Boxplot)', fontsize=14, pad=20)
    ax.set_xlabel('Year', fontsize=12)
    ax.set_ylabel('Dust Concentration (µg/m³)', fontsize=12)
    ax.set_ylim(0, 500)
    ax.set_yticks([0, 45, 90, 180, 300, 500])
    ax.grid(axis='y', ls='--', alpha=0.7)
    
    # Add logo if exists
    if logo_path and os.path.exists(logo_path):
        logo_img = mpimg.imread(logo_path)
        imagebox = OffsetImage(logo_img, zoom=0.15)
        ab = AnnotationBbox(imagebox, (0.95, 0.93), frameon=False, xycoords='axes fraction')
        ax.add_artist(ab)
    
    # Create enhanced legend
    years = sorted(df['year'].unique())
    colors = sns.color_palette("tab10", len(years))
    
    year_elements = [Line2D([0], [0], color=color, lw=4, label=f'Year: {year}') 
                    for year, color in zip(years, colors)]
    
    info_elements = [
        Line2D([0], [0], color='white', label="Produced By: Dr. Omar Althuwaynee"),
        Line2D([0], [0], color='white', label="Email: omar.faisel@gmail.com"),
        Line2D([0], [0], color='white', label="Data: WMO Barcelona Dust Regional Center & SDSWAS")
    ]
    
    ax.legend(handles=year_elements + info_elements, 
              loc='upper right', 
              bbox_to_anchor=(1, 1), 
              frameon=False, 
              fontsize=10)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"Saved violin+boxplot to {output_path}")

def plot_ecdf(df, output_path="IRQ/dust_stats_ecdf.png", logo_path="IRC.png"):
    plt.figure(figsize=(14, 8))
    
    # Pre-calculate unique years and colors
    years = sorted(df['year'].unique())
    colors = sns.color_palette("tab10", len(years))
    
    # First plot all the ECDF curves and store the line objects
    line_objects = []
    for year, color in zip(years, colors):
        data = df[df['year'] == year]['dust_concentration_ugm3'].values
        x = np.sort(data)
        y = np.arange(1, len(x)+1) / len(x)
        line, = plt.plot(x, y, color=color, lw=2)  # Note the comma to unpack the line object
        line_objects.append((year, line))
    
    # Add air quality thresholds
    thresholds = [45, 90, 180, 300]
    threshold_colors = ['green', 'yellow', 'orange', 'red']
    for val, color in zip(thresholds, threshold_colors):
        plt.axvline(val, ls='--', color=color, alpha=0.3, lw=1)
        plt.text(val, 0.02, 
                f"{val} µg/m³", 
                rotation=90, va='bottom', ha='center',
                bbox=dict(facecolor='white', alpha=0.8))
    
    # Customize plot
    plt.title('Empirical Cumulative Distribution (ECDF) by Year | IRAQ', fontsize=14, pad=20)
    plt.xlabel('Dust Concentration (µg/m³)', fontsize=12)
    plt.ylabel('Cumulative Proportion', fontsize=12)
    plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
    plt.grid(ls='--', alpha=0.7)
    plt.xlim(0, 500)
    
    # Add logo if exists
    if logo_path and os.path.exists(logo_path):
        logo_img = mpimg.imread(logo_path)
        imagebox = OffsetImage(logo_img, zoom=0.15)
        ab = AnnotationBbox(imagebox, (0.88, 0.65), frameon=False, xycoords='axes fraction')
        plt.gca().add_artist(ab)
    
    # Create legend using the actual line objects from the plot
    year_legend_elements = [
        Line2D([0], [0], color=line.get_color(), lw=2, label=f'Year: {year}')
        for year, line in line_objects
    ]
    
    info_elements = [
        Line2D([0], [0], color='white', label="Produced By: Dr. Omar Althuwaynee"),
        Line2D([0], [0], color='white', label="Email: omar.faisel@gmail.com"),
        Line2D([0], [0], color='white', label="Data: WMO Barcelona Dust Regional Center & SDSWAS")
    ]
    
    # Create and position the legends
    year_legend = plt.legend(
        handles=year_legend_elements,
        loc='upper left',
        bbox_to_anchor=(0.68, 0.72),
        title=' ',
        title_fontsize=12,
        fontsize=10
    )
    plt.gca().add_artist(year_legend)
    
    plt.legend(
        handles=info_elements,
        loc='upper left',
        bbox_to_anchor=(0.63, 0.5),
        frameon=False,
        fontsize=10
    )
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"Saved ECDF plot to {output_path}")
if __name__ == "__main__":
    # Load and prepare data
    raw_data = load_dust_data()
    plot_data = prepare_plot_data(raw_data)
    
    # Generate plots
    plot_violin_boxplot(plot_data)
    plot_ecdf(plot_data)
