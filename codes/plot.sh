#!/bin/bash
# plot.sh - Run the historical data preparation with per-year JSON output

echo "============================================================"
echo "PREPARE IRAQ HISTORICAL DUST DATA"
echo "============================================================"
echo "Start time: $(date)"
echo ""

# Create output directory if it doesn't exist
mkdir -p iraqairquality/data/historical

# First, let's check what databases are available
echo "Checking available databases..."
echo ""

# Find all year databases
AVAILABLE_YEARS=()
for db in databases/*_dust.db; do
    if [ -f "$db" ]; then
        year=$(basename "$db" _dust.db)
        AVAILABLE_YEARS+=($year)
    fi
done

if [ ${#AVAILABLE_YEARS[@]} -eq 0 ]; then
    echo "ERROR: No database files found in databases/ directory!"
    echo "Expected format: YYYY_dust.db (e.g., 2025_dust.db)"
    exit 1
fi

echo "Found databases for years: ${AVAILABLE_YEARS[*]}"
echo ""

# Ask user which years to process
echo "Which years would you like to process?"
echo "1) All available years (${AVAILABLE_YEARS[*]})"
echo "2) Select specific years"
echo "3) Test mode (first year only, limited timestamps)"
read -p "Choose (1/2/3): " year_choice

case $year_choice in
    1)
        YEARS_TO_PROCESS=("${AVAILABLE_YEARS[@]}")
        TEST_MODE=""
        ;;
    2)
        echo "Available years: ${AVAILABLE_YEARS[*]}"
        read -p "Enter years to process (space-separated, e.g., 2020 2021 2025): " -a YEARS_TO_PROCESS
        TEST_MODE=""
        ;;
    3)
        YEARS_TO_PROCESS=("${AVAILABLE_YEARS[0]}")
        TEST_MODE="--test"
        echo "Test mode: Will process ${YEARS_TO_PROCESS[0]} with limited timestamps"
        ;;
    *)
        echo "Invalid choice, exiting."
        exit 1
        ;;
esac

# Ask which aggregation periods
echo ""
echo "Which aggregation periods would you like to generate?"
echo "1) All periods (daily, weekly, monthly)"
echo "2) Daily only"
echo "3) Weekly only"
echo "4) Monthly only"
read -p "Choose (1/2/3/4): " period_choice

case $period_choice in
    1)
        PERIODS=("daily" "weekly" "monthly")
        ;;
    2)
        PERIODS=("daily")
        ;;
    3)
        PERIODS=("weekly")
        ;;
    4)
        PERIODS=("monthly")
        ;;
    *)
        echo "Invalid choice, exiting."
        exit 1
        ;;
esac

echo ""
echo "============================================================"
echo "PROCESSING CONFIGURATION"
echo "============================================================"
echo "Years: ${YEARS_TO_PROCESS[*]}"
echo "Periods: ${PERIODS[*]}"
echo "Test mode: ${TEST_MODE:-false}"
echo "Output directory: iraqairquality/data/historical/"
echo ""

# Check if shapefile exists
if [ ! -f "IRQ_adm/IRQ_districts.shp" ]; then
    echo "WARNING: Shapefile not found at IRQ_adm/IRQ_districts.shp"
    echo "Will use default districts with hierarchical IDs"
    echo ""
fi

# Confirm before proceeding
read -p "Press Enter to start processing or Ctrl+C to cancel..."

# Process each period
for period in "${PERIODS[@]}"; do
    echo ""
    echo "============================================================"
    echo "PROCESSING $period AGGREGATION"
    echo "============================================================"
    
    # Process each year
    for year in "${YEARS_TO_PROCESS[@]}"; do
        echo ""
        echo "------------------------------------------------------------"
        echo "Year: $year"
        echo "------------------------------------------------------------"
        
        python codes_DONT_TOUCH/prepare_dust_history.py \
          --shapefile IRQ_adm/IRQ_districts.shp \
          --data-dir databases/ \
          --output-dir iraqairquality/data/historical \
          --period "$period" \
          --years "$year" \
          $TEST_MODE
        
        if [ $? -eq 0 ]; then
            echo "  ✓ Completed $year for $period period"
        else
            echo "  ✗ Failed $year for $period period"
        fi
    done
done

# ============================================================
# CREATE SUMMARY REPORT
# ============================================================
echo ""
echo "============================================================"
echo "GENERATING SUMMARY REPORT"
echo "============================================================"

# Create a summary JSON
python3 -c "
import json
import os
from datetime import datetime

output_dir = 'iraqairquality/data/historical'
summary = {
    'generated': datetime.now().isoformat(),
    'configuration': {
        'years_processed': ${#YEARS_TO_PROCESS[@]},
        'periods_processed': ${#PERIODS[@]}
    },
    'files': {},
    'statistics': {}
}

# Load index if it exists
index_path = os.path.join(output_dir, 'index.json')
if os.path.exists(index_path):
    with open(index_path, 'r') as f:
        summary['index'] = json.load(f)

# Check districts
districts_path = os.path.join(output_dir, 'districts.json')
if os.path.exists(districts_path):
    with open(districts_path, 'r') as f:
        districts = json.load(f)
    summary['statistics']['total_districts'] = len(districts)
    
    # Group by province
    provinces = {}
    for d in districts:
        province_id = d.get('province_id', 'Unknown')
        if province_id not in provinces:
            provinces[province_id] = {
                'name': d.get('province_name', 'Unknown'),
                'districts': []
            }
        provinces[province_id]['districts'].append(d['id'])
    
    summary['statistics']['provinces'] = {
        pid: {'name': info['name'], 'district_count': len(info['districts'])}
        for pid, info in provinces.items()
    }

# Check each year
summary['years'] = {}
for year in ${YEARS_TO_PROCESS[@]}:  # Fixed: proper array syntax for Python
    year_dir = os.path.join(output_dir, str(year))
    if os.path.exists(year_dir):
        year_files = os.listdir(year_dir)
        summary['years'][str(year)] = year_files
        
        # Count periods for this year
        for period in ${PERIODS[@]}:  # Fixed: proper array syntax for Python
            period_file = os.path.join(year_dir, f'{period}.json')
            if os.path.exists(period_file):
                with open(period_file, 'r') as f:
                    data = json.load(f)
                if 'periods' not in summary['statistics']:
                    summary['statistics']['periods'] = {}
                if period not in summary['statistics']['periods']:
                    summary['statistics']['periods'][period] = 0
                summary['statistics']['periods'][period] += len(data)

# Save summary
summary_path = os.path.join(output_dir, 'processing_summary.json')
with open(summary_path, 'w') as f:
    json.dump(summary, f, indent=2)

print(f'\nSummary saved to {summary_path}')
"

# ============================================================
# DISPLAY FILE STRUCTURE
# ============================================================
echo ""
echo "============================================================"
echo "GENERATED FILE STRUCTURE"
echo "============================================================"
echo ""
echo "iraqairquality/data/historical/"
echo "├── index.json                    # Master index of all available data"
echo "├── districts.json                 # District metadata with hierarchical IDs"
echo "├── provinces.json                 # Province metadata"
echo "├── processing_summary.json        # This processing run summary"
echo ""

for year in "${YEARS_TO_PROCESS[@]}"; do
    if [ -d "iraqairquality/data/historical/$year" ]; then
        echo "├── $year/"
        
        # Show files for each period
        for period in "${PERIODS[@]}"; do
            if [ -f "iraqairquality/data/historical/$year/${period}.json" ]; then
                size=$(ls -lh "iraqairquality/data/historical/$year/${period}.json" | awk '{print $5}')
                echo "│   ├── ${period}.json (${size})"
            fi
            if [ -f "iraqairquality/data/historical/$year/${period}_compact.json" ]; then
                size=$(ls -lh "iraqairquality/data/historical/$year/${period}_compact.json" | awk '{print $5}')
                echo "│   ├── ${period}_compact.json (${size})"
            fi
            if [ -f "iraqairquality/data/historical/$year/${period}_by_province.json" ]; then
                size=$(ls -lh "iraqairquality/data/historical/$year/${period}_by_province.json" | awk '{print $5}')
                echo "│   ├── ${period}_by_province.json (${size})"
            fi
        done
    fi
done

# ============================================================
# DATA COVERAGE SUMMARY
# ============================================================
echo ""
echo "============================================================"
echo "DATA COVERAGE SUMMARY"
echo "============================================================"

# Count total periods
total_periods=0
for year in "${YEARS_TO_PROCESS[@]}"; do
    for period in "${PERIODS[@]}"; do
        compact_file="iraqairquality/data/historical/$year/${period}_compact.json"
        if [ -f "$compact_file" ]; then
            count=$(python3 -c "import json; print(len(json.load(open('$compact_file'))))")
            echo "  $year - $period: $count periods"
            total_periods=$((total_periods + count))
        fi
    done
done

echo ""
echo "Total data points: $total_periods measurements across all periods"

# Show district/province info
if [ -f "iraqairquality/data/historical/districts.json" ]; then
    district_count=$(python3 -c "import json; print(len(json.load(open('iraqairquality/data/historical/districts.json'))))")
    province_count=$(python3 -c "import json; print(len(json.load(open('iraqairquality/data/historical/provinces.json'))))")
    echo "Geographic coverage: $district_count districts in $province_count provinces"
fi

# ============================================================
# HTML INTEGRATION NOTES
# ============================================================
echo ""
echo "============================================================"
echo "HTML INTEGRATION GUIDE"
echo "============================================================"
echo ""
echo "Your HTML can now access data using hierarchical IDs:"
echo ""
echo "  // Load available years from index"
echo "  fetch('data/historical/index.json')"
echo "    .then(r => r.json())"
echo "    .then(index => {"
echo "      const years = index.years;"
echo "    });"
echo ""
echo "  // Load districts with province relationships"
echo "  fetch('data/historical/districts.json')"
echo "    .then(r => r.json())"
echo "    .then(districts => {"
echo "      // Filter by province"
echo "      const baghdadDistricts = districts.filter(d => d.province_id === 'P01');"
echo "    });"
echo ""
echo "  // Load data for specific year and period"
echo "  fetch('data/historical/2025/daily_compact.json')"
echo "    .then(r => r.json())"
echo "    .join();"
echo ""
echo "============================================================"
echo "PROCESSING COMPLETE!"
echo "============================================================"
echo "End time: $(date)"
echo ""
echo "Total size:"
du -sh iraqairquality/data/historical/
echo ""
echo "To view in your Flask app: http://localhost:5000/historical"
echo "============================================================"