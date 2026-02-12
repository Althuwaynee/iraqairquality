#!/bin/bash
# run_download_nc_update_fullYear_database.sh

echo "=== Running Download recent years nc file and update recent year db for Dust Analysis ==="
echo ""


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
download_nc_by_multi_YEAR.py
# ====== RUN 1 ======
python codes/download_nc_by_multi_YEAR.py
if [ $? -ne 0 ]; then
    echo "download_nc_by_multi_YEAR.py FAILED" | mail -s "❌ Dust cron FAILED (download)" "$EMAIL"
    exit 1
fi

# ====== RUN 2 ======
python codes/update_any_year_database.py  --year 2026 
if [ $? -ne 0 ]; then
    echo "update_any_year_database.py FAILED" | mail -s "❌ Dust cron FAILED (database)" "$EMAIL"
    exit 1
fi

echo "Run finished successfully at: $(date)"
