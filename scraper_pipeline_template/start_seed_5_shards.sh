#!/usr/bin/env bash
set -euo pipefail

# Run from repo root or any location.
# This script launches 5 HASH-based seed shard workers in background using isolated DB/log files.

SCRIPT="Steam-Data-Analysis-v2/scraper/steam_scraper_advanced.py"

for i in 0 1 2 3 4; do
  nohup python "$SCRIPT" \
    --phase seed \
    --shard-index "$i" \
    --shard-count 5 \
    --shard-method hash \
    --db-file "Steam-Data-Analysis-v2/scraper/steam_data_cache_5hash_shard${i}.db" \
    > "Steam-Data-Analysis-v2/scraper/seed_5hash_shard${i}.log" 2>&1 &
done

echo "Started 5 shard seed jobs."
echo "Check processes: ps -ef | grep -- \"--shard-count 5\" | grep -v grep"
echo "Check summary:   python Steam-Data-Analysis-v2/scraper/shard_progress.py --shard-count 5 --db-prefix steam_data_cache_5hash_shard --with-total"
