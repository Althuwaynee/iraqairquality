#!/bin/bash
# run_download_nc_update_fullYear_database.sh

echo "=== Running Download recent years nc file and update recent year db for Dust Analysis ==="
echo ""

# ====== CONFIG ======
EMAIL="scadac@outlook.com"

# ====== DETECT ENVIRONMENT ======
# Check if we're on server or local PC
if [ -d "/home/omar/Dust/envs/pythonenv" ]; then
    # Server path
    LOG="/home/omar/Dust/cron.log"
    CONDA_BASE="/home/omar/miniconda3"
    ENV_PATH="/home/omar/Dust/envs/pythonenv"
    WORKDIR="/home/omar/Dust"
elif [ -d "/home/omar/Documents/Dust" ]; then
    # Local PC path
    LOG="/home/omar/Documents/Dust/cron.log"
    CONDA_BASE="/home/omar/miniconda3"
    ENV_NAME="pythonenv"  # Use environment name on PC
    WORKDIR="/home/omar/Documents/Dust"
else
    echo "Unknown environment, cannot determine paths"
    exit 1
fi

# ====== ENV SETUP ======
export PATH="$CONDA_BASE/bin:$PATH"
source "$CONDA_BASE/etc/profile.d/conda.sh"

# Activate based on what's available
if [ -n "$ENV_PATH" ]; then
    # Activate by full path (server)
    conda activate "$ENV_PATH"
else
    # Activate by name (local PC)
    conda activate "$ENV_NAME"
fi

# ====== WORKING DIR ======
cd "$WORKDIR" || exit 1

# ====== LOGGING ======
exec >> "$LOG" 2>&1
echo "======================================"
echo "Run started at: $(date)"
echo "Environment: $(conda info --envs | grep '*')"

# ====== RUN 1 ======
python iraqairquality/codes/download_nc_by_multi_YEAR.py
if [ $? -ne 0 ]; then
    echo "download_nc_by_multi_YEAR.py FAILED" | mail -s "❌ Dust cron FAILED (download)" "$EMAIL"
    exit 1
fi

# ====== RUN 2 ======
python iraqairquality/codes/update_any_year_database.py --year 2026 
if [ $? -ne 0 ]; then
    echo "update_any_year_database.py FAILED" | mail -s "❌ Dust cron FAILED (database)" "$EMAIL"
    exit 1
fi

echo "Run finished successfully at: $(date)"