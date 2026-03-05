#!/usr/bin/env bash
set -euo pipefail

SCRIPT="Steam-Data-Analysis-v2/scraper/steam_scraper_advanced.py"

for i in 0 1 2; do
  nohup python "$SCRIPT" \
    --phase details \
    --db-file "Steam-Data-Analysis-v2/scraper/steam_details_parallel3_shard${i}.db" \
    --min-delay 1.5 \
    --max-delay 2.5 \
    > "Steam-Data-Analysis-v2/scraper/details_parallel3_shard${i}.log" 2>&1 &
done

echo "Started 3 parallel details workers."
echo "Check with: python Steam-Data-Analysis-v2/scraper/check_details_parallel3_progress.py"
