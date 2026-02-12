#!/bin/bash

# ====== CONFIG ======
EMAIL="scadac@outlook.com"
LOG="$HOME/Documents/Dust/cron.log"

# ====== ENV SETUP ======
export PATH="$HOME/miniconda3/bin:$HOME/miniconda3/envs/pythonenv/bin/python"
source $HOME/miniconda3/etc/profile.d/conda.sh
conda activate pythonenv

# ====== WORKING DIR ======
cd $HOME/Documents/Dust || exit 1

# ====== LOGGING ======
exec >> "$LOG" 2>&1
echo "======================================"
echo "Run started at: $(date)"

# ====== RUN 1 ======
python codes/download_latest_nc.py
if [ $? -ne 0 ]; then
    echo "download_latest_nc.py FAILED" | mail -s "❌ Dust cron FAILED (download)" "$EMAIL"
    exit 1
fi

# ====== RUN 2 ======
python codes/create_realtime_database_full_grid.py --input . --db databases/dust_realtime.db
if [ $? -ne 0 ]; then
    echo "create_realtime_database_full_grid.py FAILED" | mail -s "❌ Dust cron FAILED (database)" "$EMAIL"
    exit 1
fi

echo "Run finished successfully at: $(date)"
