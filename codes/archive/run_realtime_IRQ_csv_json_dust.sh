#!/bin/bash
# run_realtime_IRQ_csv_json_dust.sh

echo "=== Running Current-Time Dust Analysis ==="
echo ""
python3 /home/omar/Documents/Dust/codes/realtime_IRQ_csv_json_dust.py \
  --dust-db /home/omar/Documents/Dust/databases/dust_realtime.db \
  --shapefile /home/omar/Documents/Dust/IRQ_adm/IRQ_districts.shp \
  --output /home/omar/Documents/Dust/publication/pm10_now.json \
  --store-db /home/omar/Documents/Dust/databases/district_pm10_hourly.sqlite


# Rolling means + alerts
python3 /home/omar/Documents/Dust/codes/rolling_means_alerts.py \
  --store-db /home/omar/Documents/Dust/databases/district_pm10_hourly.sqlite \
  --output /home/omar/Documents/Dust/publication/pm10_alerts.json

echo ""
echo "=== Done ==="
