#!/usr/bin/env python3
"""
generate_aqi_flowchart.py
Generates a professional flowchart for Iraq Air Quality Platform data pipeline
Output: PNG image suitable for website embedding
"""

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import networkx as nx
import numpy as np
from matplotlib.patches import FancyBboxPatch, Rectangle, FancyArrowPatch
import matplotlib.patheffects as path_effects

# ============================================================
# CONFIGURATION
# ============================================================
OUTPUT_FILE = "iraq_aqi_pipeline_flowchart.png"
FIGURE_SIZE = (20, 14)
DPI = 150
BG_COLOR = '#fafafa'
TITLE = 'Iraq Air Quality Platform – Data Processing Pipeline'

# Color scheme
COLORS = {
    'data': '#e1f5fe',      # Light blue - data sources
    'process': '#fff3e0',   # Light orange - processing steps
    'database': '#f3e5f5',  # Light purple - databases
    'output': '#e8f5e9',    # Light green - output files
    'web': '#ffebee',       # Light red - web visualization
    'arrow': '#666666',     # Dark gray - arrows
    'title_bg': '#2c3e50',  # Dark blue - section titles
}

# ============================================================
# CREATE FLOWCHART
# ============================================================
def create_flowchart():
    """Create the complete processing pipeline flowchart"""
    
    fig, ax = plt.subplots(1, 1, figsize=FIGURE_SIZE, facecolor=BG_COLOR)
    ax.set_xlim(0, 20)
    ax.set_ylim(0, 14)
    ax.axis('off')
    
    # ============ TITLE ============
    title = ax.text(10, 13.5, TITLE, 
                    fontsize=24, fontweight='bold', 
                    ha='center', va='center',
                    fontname='DejaVu Sans')
    title.set_path_effects([path_effects.withStroke(linewidth=3, foreground='white')])
    
    subtitle = ax.text(10, 13.0, 
                      'From Barcelona Dust Regional Center → Web Map Visualization',
                      fontsize=14, color='#555555', 
                      ha='center', va='center',
                      style='italic')
    
    # ============ SECTION 1: DATA ACQUISITION ============
    # Section title
    ax.add_patch(Rectangle((0.5, 11.8), 19, 0.4, 
                          facecolor=COLORS['title_bg'], alpha=0.8, ec='none'))
    ax.text(10, 12.0, '📡 1. DATA ACQUISITION & GRID PROCESSING',
           fontsize=16, fontweight='bold', color='white', ha='center', va='center')
    
    # Source - Barcelona
    bbox_src = FancyBboxPatch((1, 10.5), 3.5, 1.2,
                            boxstyle="round,pad=0.1,rounding_size=0.2",
                            facecolor=COLORS['data'], ec='#01579b', linewidth=2)
    ax.add_patch(bbox_src)
    ax.text(2.75, 11.1, 'Barcelona Dust\nRegional Center', 
           fontsize=12, fontweight='bold', ha='center', va='center')
    ax.text(2.75, 10.7, 'WMO Regional Center', 
           fontsize=9, ha='center', va='center', style='italic', color='#01579b')
    
    # NC Files
    bbox_nc = FancyBboxPatch((5, 10.5), 3, 1.2,
                           boxstyle="round,pad=0.1,rounding_size=0.2",
                           facecolor=COLORS['data'], ec='#01579b', linewidth=2)
    ax.add_patch(bbox_nc)
    ax.text(6.5, 11.1, 'Latest .nc Files', 
           fontsize=12, fontweight='bold', ha='center', va='center')
    ax.text(6.5, 10.7, '*/latest folder', 
           fontsize=9, ha='center', va='center', color='#01579b')
    
    # Arrow 1→2
    arrow = FancyArrowPatch((4.25, 11.1), (5, 11.1), 
                           arrowstyle='->,head_width=0.2', 
                           color=COLORS['arrow'], linewidth=1.5)
    ax.add_patch(arrow)
    
    # Year Database
    bbox_year = FancyBboxPatch((9, 9.8), 3, 1.2,
                             boxstyle="round,pad=0.1,rounding_size=0.2",
                             facecolor=COLORS['database'], ec='#4a148c', linewidth=2)
    ax.add_patch(bbox_year)
    ax.text(10.5, 10.4, 'Year Database', 
           fontsize=12, fontweight='bold', ha='center', va='center')
    ax.text(10.5, 10.0, '2024_dust.db', 
           fontsize=9, ha='center', va='center', color='#4a148c')
    
    # Grid Processing Script
    bbox_process1 = FancyBboxPatch((5, 8.8), 4, 1.2,
                                 boxstyle="round,pad=0.1,rounding_size=0.2",
                                 facecolor=COLORS['process'], ec='#e65100', linewidth=2)
    ax.add_patch(bbox_process1)
    ax.text(7, 9.4, 'create_realtime_database_full_grid.py', 
           fontsize=10, fontweight='bold', ha='center', va='center')
    ax.text(7, 9.0, '• Extract grid points\n• Convert kg/m³ → µg/m³\n• Load 4 previous files',
           fontsize=8, ha='center', va='center', color='#e65100')
    
    # Arrow NC → Process
    arrow2 = FancyArrowPatch((6.5, 10.5), (7, 9.8), 
                            arrowstyle='->,head_width=0.2',
                            color=COLORS['arrow'], linewidth=1.5,
                            connectionstyle="arc3,rad=-0.1")
    ax.add_patch(arrow2)
    
    # Arrow YearDB → Process
    arrow3 = FancyArrowPatch((10.5, 9.8), (8, 9.4), 
                            arrowstyle='->,head_width=0.2',
                            color=COLORS['arrow'], linewidth=1.5,
                            connectionstyle="arc3,rad=0.2")
    ax.add_patch(arrow3)
    
    # Realtime Database
    bbox_realtime = FancyBboxPatch((5, 7.2), 4, 1.2,
                                 boxstyle="round,pad=0.1,rounding_size=0.2",
                                 facecolor=COLORS['database'], ec='#4a148c', linewidth=2)
    ax.add_patch(bbox_realtime)
    ax.text(7, 7.8, 'dust_measurements_realtime', 
           fontsize=11, fontweight='bold', ha='center', va='center')
    ax.text(7, 7.4, 'Grid points • 3h resolution', 
           fontsize=9, ha='center', va='center', color='#4a148c')
    
    # Arrow Process → DB
    arrow4 = FancyArrowPatch((7, 8.8), (7, 7.8), 
                            arrowstyle='->,head_width=0.2',
                            color=COLORS['arrow'], linewidth=1.5)
    ax.add_patch(arrow4)
    
    # ============ SECTION 2: BACKFILL & IDW ============
    # Section title
    ax.add_patch(Rectangle((0.5, 6.4), 19, 0.4, 
                          facecolor=COLORS['title_bg'], alpha=0.8, ec='none'))
    ax.text(10, 6.6, '🗺️ 2. BACKFILL & DISTRICT INTERPOLATION (Constrained IDW)',
           fontsize=16, fontweight='bold', color='white', ha='center', va='center')
    
    # Backfill Script
    bbox_backfill = FancyBboxPatch((1, 4.8), 3.5, 1.2,
                                 boxstyle="round,pad=0.1,rounding_size=0.2",
                                 facecolor=COLORS['process'], ec='#e65100', linewidth=2)
    ax.add_patch(bbox_backfill)
    ax.text(2.75, 5.4, 'backfill_district_pm10.py', 
           fontsize=10, fontweight='bold', ha='center', va='center')
    ax.text(2.75, 5.0, 'Past 72 hours • Historical coverage', 
           fontsize=8, ha='center', va='center', color='#e65100')
    
    # Shapefile
    bbox_shape = FancyBboxPatch((5, 4.0), 3, 1.2,
                              boxstyle="round,pad=0.1,rounding_size=0.2",
                              facecolor=COLORS['data'], ec='#01579b', linewidth=2)
    ax.add_patch(bbox_shape)
    ax.text(6.5, 4.6, 'Iraq Districts', 
           fontsize=12, fontweight='bold', ha='center', va='center')
    ax.text(6.5, 4.2, 'Shapefile • Administrative boundaries', 
           fontsize=8, ha='center', va='center', color='#01579b')
    
    # IDW Script
    bbox_idw = FancyBboxPatch((5, 2.2), 4, 1.2,
                            boxstyle="round,pad=0.1,rounding_size=0.2",
                            facecolor=COLORS['process'], ec='#e65100', linewidth=2)
    ax.add_patch(bbox_idw)
    ax.text(7, 2.8, 'realtime_IRQ_csv_json_dust.py', 
           fontsize=10, fontweight='bold', ha='center', va='center')
    ax.text(7, 2.4, 'Constrained IDW: 55km • 4 neighbors • Power=2', 
           fontsize=8, ha='center', va='center', color='#e65100')
    
    # District Database
    bbox_district_db = FancyBboxPatch((5, 0.5), 4, 1.2,
                                    boxstyle="round,pad=0.1,rounding_size=0.2",
                                    facecolor=COLORS['database'], ec='#4a148c', linewidth=2)
    ax.add_patch(bbox_district_db)
    ax.text(7, 1.1, 'district_pm10_hourly', 
           fontsize=11, fontweight='bold', ha='center', va='center')
    ax.text(7, 0.7, 'District-level PM10 • 3h intervals', 
           fontsize=8, ha='center', va='center', color='#4a148c')
    
    # Arrows - Backfill
    arrow5 = FancyArrowPatch((2.75, 4.8), (5.5, 2.8), 
                            arrowstyle='->,head_width=0.2',
                            color=COLORS['arrow'], linewidth=1.5,
                            connectionstyle="arc3,rad=-0.2")
    ax.add_patch(arrow5)
    
    # Arrows - Shapefile → IDW
    arrow6 = FancyArrowPatch((6.5, 4.0), (7, 2.8), 
                            arrowstyle='->,head_width=0.2',
                            color=COLORS['arrow'], linewidth=1.5,
                            connectionstyle="arc3,rad=-0.1")
    ax.add_patch(arrow6)
    
    # Arrows - Realtime DB → IDW
    arrow7 = FancyArrowPatch((7, 7.2), (7, 3.4), 
                            arrowstyle='->,head_width=0.2',
                            color=COLORS['arrow'], linewidth=1.5,
                            connectionstyle="arc3,rad=0.1")
    ax.add_patch(arrow7)
    
    # Arrow - IDW → District DB
    arrow8 = FancyArrowPatch((7, 2.2), (7, 1.2), 
                            arrowstyle='->,head_width=0.2',
                            color=COLORS['arrow'], linewidth=1.5)
    ax.add_patch(arrow8)
    
    # ============ SECTION 3: ROLLING MEANS & ALERTS ============
    # Section title
    ax.add_patch(Rectangle((10.5, 6.4), 9, 0.4, 
                          facecolor=COLORS['title_bg'], alpha=0.8, ec='none'))
    ax.text(15, 6.6, '⚠️ 3. ROLLING MEANS, AQI & FORECASTS',
           fontsize=16, fontweight='bold', color='white', ha='center', va='center')
    
    # Rolling Means Script
    bbox_rolling = FancyBboxPatch((11, 4.8), 4, 1.2,
                                boxstyle="round,pad=0.1,rounding_size=0.2",
                                facecolor=COLORS['process'], ec='#e65100', linewidth=2)
    ax.add_patch(bbox_rolling)
    ax.text(13, 5.4, 'rolling_means_alerts.py', 
           fontsize=10, fontweight='bold', ha='center', va='center')
    ax.text(13, 5.0, '6/12/24h means • AQI • 24h forecast', 
           fontsize=8, ha='center', va='center', color='#e65100')
    
    # Arrow District DB → Rolling
    arrow9 = FancyArrowPatch((9, 1.1), (12, 4.8), 
                            arrowstyle='->,head_width=0.2',
                            color=COLORS['arrow'], linewidth=1.5,
                            connectionstyle="arc3,rad=-0.3")
    ax.add_patch(arrow9)
    
    # Parameters Box
    bbox_params = FancyBboxPatch((16, 3.8), 2.5, 1.8,
                               boxstyle="round,pad=0.1,rounding_size=0.2",
                               facecolor='#fff9c4', ec='#ff6f00', linewidth=2)
    ax.add_patch(bbox_params)
    ax.text(17.25, 5.0, 'Parameters', 
           fontsize=11, fontweight='bold', ha='center', va='center')
    ax.text(17.25, 4.6, '• Rolling: 6/12/24h\n• Forecast: 3-24h (3h steps)\n• AQI: EPA PM10\n• Iraq limit: 100μg/m³', 
           fontsize=8, ha='center', va='center')
    
    # JSON Output
    bbox_json = FancyBboxPatch((11, 2.8), 4, 1.2,
                             boxstyle="round,pad=0.1,rounding_size=0.2",
                             facecolor=COLORS['output'], ec='#1b5e20', linewidth=2)
    ax.add_patch(bbox_json)
    ax.text(13, 3.4, 'iraq_aqi_alerts_with_forecast.json', 
           fontsize=9, fontweight='bold', ha='center', va='center')
    ax.text(13, 3.0, 'Current + 8 forecast windows', 
           fontsize=8, ha='center', va='center', color='#1b5e20')
    
    # Arrow Rolling → JSON
    arrow10 = FancyArrowPatch((13, 4.8), (13, 4.0), 
                             arrowstyle='->,head_width=0.2',
                             color=COLORS['arrow'], linewidth=1.5)
    ax.add_patch(arrow10)
    
    # ============ SECTION 4: WEB VISUALIZATION ============
    # Section title
    ax.add_patch(Rectangle((10.5, 1.8), 9, 0.4, 
                          facecolor=COLORS['title_bg'], alpha=0.8, ec='none'))
    ax.text(15, 2.0, '🌐 4. MAP PRODUCTION & VISUALIZATION',
           fontsize=16, fontweight='bold', color='white', ha='center', va='center')
    
    # Leaflet Map
    bbox_map = FancyBboxPatch((11, 0.5), 4, 1.2,
                            boxstyle="round,pad=0.1,rounding_size=0.2",
                            facecolor=COLORS['web'], ec='#b71c1c', linewidth=2)
    ax.add_patch(bbox_map)
    ax.text(13, 1.1, 'Leaflet.js Interactive Map', 
           fontsize=11, fontweight='bold', ha='center', va='center')
    ax.text(13, 0.7, 'District polygons • AQI colors • Popups', 
           fontsize=8, ha='center', va='center', color='#b71c1c')
    
    # Arrow JSON → Map
    arrow11 = FancyArrowPatch((13, 2.8), (13, 1.2), 
                             arrowstyle='->,head_width=0.2',
                             color=COLORS['arrow'], linewidth=1.5)
    ax.add_patch(arrow11)
    
    # AQI Legend
    bbox_legend = FancyBboxPatch((16, 0.5), 2.5, 1.2,
                               boxstyle="round,pad=0.1,rounding_size=0.2",
                               facecolor='white', ec='#b71c1c', linewidth=2)
    ax.add_patch(bbox_legend)
    ax.text(17.25, 1.1, 'AQI Color Code', 
           fontsize=10, fontweight='bold', ha='center', va='center')
    colors = ['#00e400', '#ffff00', '#ff7e00', '#ff0000', '#8f3f97', '#7e0023']
    labels = ['Good', 'Moderate', 'USG', 'Unhealthy', 'Very Unhealthy', 'Hazardous']
    for i, (c, l) in enumerate(zip(colors, labels)):
        ax.add_patch(Rectangle((16.2, 0.8 - i*0.1), 0.15, 0.08, 
                              facecolor=c, ec='none'))
        ax.text(16.4, 0.83 - i*0.1, l, fontsize=7, va='center')
    
    # ============ LEGEND ============
    legend_x, legend_y = 0.5, 13.2
    
    # Data Source
    ax.add_patch(Rectangle((legend_x, legend_y), 0.2, 0.1, 
                          facecolor=COLORS['data'], ec='#01579b', linewidth=1))
    ax.text(legend_x + 0.3, legend_y + 0.05, 'Data Source', fontsize=9, va='center')
    
    # Process
    ax.add_patch(Rectangle((legend_x + 2.5, legend_y), 0.2, 0.1, 
                          facecolor=COLORS['process'], ec='#e65100', linewidth=1))
    ax.text(legend_x + 2.8, legend_y + 0.05, 'Processing', fontsize=9, va='center')
    
    # Database
    ax.add_patch(Rectangle((legend_x + 4.5, legend_y), 0.2, 0.1, 
                          facecolor=COLORS['database'], ec='#4a148c', linewidth=1))
    ax.text(legend_x + 4.8, legend_y + 0.05, 'Database', fontsize=9, va='center')
    
    # Output
    ax.add_patch(Rectangle((legend_x + 6.5, legend_y), 0.2, 0.1, 
                          facecolor=COLORS['output'], ec='#1b5e20', linewidth=1))
    ax.text(legend_x + 6.8, legend_y + 0.05, 'Output', fontsize=9, va='center')
    
    # Web
    ax.add_patch(Rectangle((legend_x + 8.5, legend_y), 0.2, 0.1, 
                          facecolor=COLORS['web'], ec='#b71c1c', linewidth=1))
    ax.text(legend_x + 8.8, legend_y + 0.05, 'Visualization', fontsize=9, va='center')
    
    # ============ FOOTER ============
    footer = ax.text(10, -0.2, 
                    '© 2026 Iraq Air Quality Platform • Data: Barcelona Dust Regional Center • Method: Constrained IDW (55km, 4 neighbors)',
                    fontsize=8, color='#777777', ha='center', va='center',
                    style='italic')
    
    plt.tight_layout()
    return fig

# ============================================================
# ALTERNATIVE: NETWORKX VERSION (Simpler)
# ============================================================
def create_networkx_flowchart():
    """Create a simpler flowchart using networkx"""
    
    fig, ax = plt.subplots(1, 1, figsize=(16, 10), facecolor='white')
    ax.axis('off')
    
    # Create graph
    G = nx.DiGraph()
    
    # Add nodes with positions
    nodes = {
        # Data sources
        'Barcelona': (1, 9),
        'NC Files': (3, 9),
        'Year DB': (5, 9),
        'Shapefile': (7, 7),
        
        # Processing
        'Grid Processor': (4, 7),
        'Backfill': (2, 5),
        'IDW': (5, 5),
        
        # Databases
        'Realtime DB': (4, 3),
        'District DB': (5, 1),
        
        # Analytics
        'Rolling Means': (8, 5),
        'JSON Output': (8, 3),
        
        # Visualization
        'Leaflet Map': (8, 1),
    }
    
    # Add nodes
    for name, pos in nodes.items():
        G.add_node(name, pos=pos)
    
    # Add edges
    edges = [
        ('Barcelona', 'NC Files'),
        ('NC Files', 'Grid Processor'),
        ('Year DB', 'Grid Processor'),
        ('Grid Processor', 'Realtime DB'),
        ('Realtime DB', 'Backfill'),
        ('Realtime DB', 'IDW'),
        ('Backfill', 'IDW'),
        ('Shapefile', 'IDW'),
        ('IDW', 'District DB'),
        ('District DB', 'Rolling Means'),
        ('Rolling Means', 'JSON Output'),
        ('JSON Output', 'Leaflet Map'),
    ]
    G.add_edges_from(edges)
    
    # Get positions
    pos = nx.get_node_attributes(G, 'pos')
    
    # Draw nodes
    node_colors = {
        'Barcelona': COLORS['data'],
        'NC Files': COLORS['data'],
        'Year DB': COLORS['database'],
        'Shapefile': COLORS['data'],
        'Grid Processor': COLORS['process'],
        'Backfill': COLORS['process'],
        'IDW': COLORS['process'],
        'Realtime DB': COLORS['database'],
        'District DB': COLORS['database'],
        'Rolling Means': COLORS['process'],
        'JSON Output': COLORS['output'],
        'Leaflet Map': COLORS['web'],
    }
    
    for node in G.nodes():
        color = node_colors.get(node, '#cccccc')
        nx.draw_networkx_nodes(G, pos, nodelist=[node], 
                              node_color=color, node_size=3000,
                              node_shape='s', ax=ax, 
                              edgecolors='black', linewidths=1)
    
    # Draw edges
    nx.draw_networkx_edges(G, pos, edge_color='#666666', 
                          arrows=True, arrowstyle='->',
                          arrowsize=20, width=1.5, ax=ax,
                          connectionstyle='arc3,rad=0.1')
    
    # Draw labels
    nx.draw_networkx_labels(G, pos, font_size=9, font_weight='bold', ax=ax)
    
    # Title
    ax.text(4.5, 10, 'Iraq Air Quality Platform - Data Pipeline',
           fontsize=20, fontweight='bold', ha='center')
    ax.text(4.5, 9.5, 'From Barcelona Dust Center to Interactive Map',
           fontsize=12, color='#555555', ha='center', style='italic')
    
    plt.tight_layout()
    return fig

# ============================================================
# MAIN
# ============================================================
def main():
    """Generate both versions and save as PNG"""
    
    print("=" * 60)
    print("IRAQ AQI PIPELINE FLOWCHART GENERATOR")
    print("=" * 60)
    
    # Version 1: Detailed matplotlib version
    print("\n[1/2] Generating detailed flowchart...")
    fig1 = create_flowchart()
    output1 = "iraq_aqi_pipeline_detailed.png"
    fig1.savefig(output1, dpi=DPI, bbox_inches='tight', facecolor=BG_COLOR)
    plt.close(fig1)
    print(f"   ✅ Saved: {output1}")
    
    # Version 2: Simplified networkx version
    print("\n[2/2] Generating simplified flowchart...")
    fig2 = create_networkx_flowchart()
    output2 = "iraq_aqi_pipeline_simplified.png"
    fig2.savefig(output2, dpi=DPI, bbox_inches='tight', facecolor='white')
    plt.close(fig2)
    print(f"   ✅ Saved: {output2}")
    
    print("\n" + "=" * 60)
    print("✅ COMPLETE! Two flowchart versions created:")
    print(f"   1. {output1} - Detailed, color-coded sections")
    print(f"   2. {output2} - Simplified, network diagram")
    print("\n📁 Both files saved in current directory")
    print("=" * 60)

if __name__ == "__main__":
    main()
