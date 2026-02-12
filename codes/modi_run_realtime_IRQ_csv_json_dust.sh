#!/bin/bash

echo "=== Running Dust Analysis Pipeline ==="
echo ""
#!/bin/bash

# ====== CONFIG ======
EMAIL="scadac@outlook.com"
LOG="/home/omar/Documents/Dust/cron.log"

# ====== ENV SETUP ======
export PATH="/home/omar/miniconda3/bin:$/home/omar/miniconda3/envs/pythonenv/bin/python"
source /home/omar/miniconda3/etc/profile.d/conda.sh
conda activate pythonenv

# ====== WORKING DIR ======
cd /home/omar/Documents/Dust || exit 1

# ====== LOGGING ======
exec >> "$LOG" 2>&1
echo "======================================"
echo "Run started at: $(date)"

# Step 1: Backfill district data for past 72 hours
echo "Backfilling district PM10 data..."
python3 /home/omar/Documents/Dust/codes/backfill_district_pm10.py \
  --dust-db /home/omar/Documents/Dust/databases/dust_realtime.db \
  --shapefile /home/omar/Documents/Dust/IRQ_adm/IRQ_districts.shp \
  --store-db /home/omar/Documents/Dust/databases/district_pm10_hourly.sqlite \
  --hours 72

echo ""
echo "=== Generating current PM10 data ==="
echo ""

# Step 2: Generate current PM10 data (now includes realtime interpolation)
python3 /home/omar/Documents/Dust/codes/realtime_IRQ_csv_json_dust.py \
  --dust-db /home/omar/Documents/Dust/databases/dust_realtime.db \
  --shapefile /home/omar/Documents/Dust/IRQ_adm/IRQ_districts.shp \
  --output /home/omar/Documents/Dust/iraqairquality/data/pm10_now.json \
  --store-db /home/omar/Documents/Dust/databases/district_pm10_hourly.sqlite

echo ""
echo "=== Calculating rolling means and alerts ==="
echo ""

# Step 3: Rolling means + alerts
python3 /home/omar/Documents/Dust/codes/rolling_means_alerts.py \
  --store-db /home/omar/Documents/Dust/databases/district_pm10_hourly.sqlite \
  --output /home/omar/Documents/Dust/iraqairquality/data/pm10_alerts.json

echo ""
echo "=== Done ==="
