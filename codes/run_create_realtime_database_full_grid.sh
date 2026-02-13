#!/bin/bash

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
    # Server (path-based environment)
    ENV_OPTION="-p /home/omar/Dust/envs/pythonenv"
elif conda env list | grep -q "pythonenv"; then
    # PC (named environment)
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
conda run $ENV_OPTION python iraqairquality/codes/download_latest_nc.py
if [ $? -ne 0 ]; then
    echo "download_latest_nc.py FAILED" | mail -s "❌ Dust cron FAILED (download)" "$EMAIL"
    exit 1
fi

# ================= RUN 2 =================
conda run $ENV_OPTION python iraqairquality/codes/create_realtime_database_full_grid.py --input . --db databases/dust_realtime.db
if [ $? -ne 0 ]; then
    echo "create_realtime_database_full_grid.py FAILED" | mail -s "❌ Dust cron FAILED (database)" "$EMAIL"
    exit 1
fi

echo "Run finished successfully at: $(date)"
echo "======================================"
