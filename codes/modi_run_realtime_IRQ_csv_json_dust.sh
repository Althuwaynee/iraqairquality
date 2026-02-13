#!/bin/bash

echo "=== Running Dust Analysis Pipeline ==="
echo ""

# ================= CONFIG =================
EMAIL="scadac@outlook.com"
CONDA_BASE="/home/omar/miniconda3"

# ================= DETECT WORKDIR =================
if [ -d "/home/omar/Dust" ]; then
    WORKDIR="/home/omar/Dust"
elif [ -d "/home/omar/Documents/Dust" ]; then
    WORKDIR="/home/omar/Documents/Dust"
else
    echo "Cannot determine working directory"
    exit 1
fi

LOG="$WORKDIR/cron.log"

# ================= LOAD CONDA =================
export PATH="$CONDA_BASE/bin:$PATH"
source "$CONDA_BASE/etc/profile.d/conda.sh"

# ================= DETECT ENV =================
if [ -d "/home/omar/Dust/envs/pythonenv" ]; then
    # Server (path-based env)
    ENV_OPTION="-p /home/omar/Dust/envs/pythonenv"
elif conda env list | grep -q "pythonenv"; then
    # PC (named env)
    ENV_OPTION="-n pythonenv"
else
    echo "pythonenv not found" | mail -s "❌ Dust pipeline FAILED (env missing)" "$EMAIL"
    exit 1
fi

# ================= START =================
cd "$WORKDIR" || exit 1

exec >> "$LOG" 2>&1

echo "======================================"
echo "Run started at: $(date)"
echo "Working directory: $(pwd)"
echo "Using environment option: $ENV_OPTION"

# ====================================================
# STEP 1 — Backfill district PM10 data
# ====================================================
echo "Backfilling district PM10 data..."

conda run $ENV_OPTION python iraqairquality/codes/backfill_district_pm10.py \
  --dust-db databases/dust_realtime.db \
  --shapefile IRQ_adm/IRQ_districts.shp \
  --store-db databases/district_pm10_hourly.sqlite \
  --hours 72

if [ $? -ne 0 ]; then
    echo "backfill_district_pm10.py FAILED" | mail -s "❌ Dust pipeline FAILED (backfill)" "$EMAIL"
    exit 1
fi

# ====================================================
# STEP 2 — Generate current PM10 JSON
# ====================================================
echo "Generating current PM10 data..."

conda run $ENV_OPTION python iraqairquality/codes/realtime_IRQ_csv_json_dust.py \
  --dust-db databases/dust_realtime.db \
  --shapefile IRQ_adm/IRQ_districts.shp \
  --output iraqairquality/data/pm10_now.json \
  --store-db databases/district_pm10_hourly.sqlite

if [ $? -ne 0 ]; then
    echo "realtime_IRQ_csv_json_dust.py FAILED" | mail -s "❌ Dust pipeline FAILED (realtime JSON)" "$EMAIL"
    exit 1
fi

# ====================================================
# STEP 3 — Rolling means & alerts
# ====================================================
echo "Calculating rolling means and alerts..."

conda run $ENV_OPTION python iraqairquality/codes/rolling_means_alerts.py \
  --store-db databases/district_pm10_hourly.sqlite \
  --output iraqairquality/data/pm10_alerts.json

if [ $? -ne 0 ]; then
    echo "rolling_means_alerts.py FAILED" | mail -s "❌ Dust pipeline FAILED (alerts)" "$EMAIL"
    exit 1
fi

echo "Run finished successfully at: $(date)"
echo "======================================"
