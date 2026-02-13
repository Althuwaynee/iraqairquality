#!/bin/bash
# run_download_nc_update_fullYear_database.sh

echo "=== Running Download recent years nc file and update recent year db for Dust Analysis ==="
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
    echo "pythonenv not found" | mail -s "❌ Dust cron FAILED (env missing)" "$EMAIL"
    exit 1
fi

# ================= START =================
cd "$WORKDIR" || exit 1

exec >> "$LOG" 2>&1

echo "======================================"
echo "Run started at: $(date)"
echo "Working directory: $(pwd)"
echo "Using environment option: $ENV_OPTION"

# ================= RUN 1 =================
conda run $ENV_OPTION python iraqairquality/codes/download_nc_by_multi_YEAR.py
if [ $? -ne 0 ]; then
    echo "download_nc_by_multi_YEAR.py FAILED" | mail -s "❌ Dust cron FAILED (download multi-year)" "$EMAIL"
    exit 1
fi

# ================= RUN 2 =================
conda run $ENV_OPTION python iraqairquality/codes/update_any_year_database.py --year 2026
if [ $? -ne 0 ]; then
    echo "update_any_year_database.py FAILED" | mail -s "❌ Dust cron FAILED (update database)" "$EMAIL"
    exit 1
fi

echo "Run finished successfully at: $(date)"
echo "======================================"
