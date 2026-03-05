import argparse
import sqlite3
from pathlib import Path

import requests


def get_total_apps() -> int | None:
    url = "https://raw.githubusercontent.com/dgibbs64/SteamCMD-AppID-List/master/steamcmd_appid.json"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        apps = resp.json().get("applist", {}).get("apps", [])
        return len(apps)
    except Exception:
        return None


def read_shard_counts(db_file: Path) -> tuple[int, int]:
    if not db_file.exists():
        return 0, 0

    conn = sqlite3.connect(db_file)
    cur = conn.cursor()

    tables = {row[0] for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    scanned = 0
    seeded = 0

    if "scanned_apps" in tables:
        scanned = cur.execute("SELECT COUNT(*) FROM scanned_apps").fetchone()[0]
    if "games" in tables:
        seeded = cur.execute("SELECT COUNT(*) FROM games").fetchone()[0]

    conn.close()
    return scanned, seeded


def main() -> None:
    parser = argparse.ArgumentParser(description="Show progress across seed shards")
    parser.add_argument("--shard-count", type=int, default=3)
    parser.add_argument("--db-dir", type=str, default=str(Path(__file__).resolve().parent))
    parser.add_argument("--db-prefix", type=str, default="steam_data_cache_shard")
    parser.add_argument("--with-total", action="store_true", help="Fetch total Steam app count for progress %%")
    args = parser.parse_args()

    db_dir = Path(args.db_dir)

    grand_scanned = 0
    grand_seeded = 0

    print("Shard Progress")
    print("-" * 44)
    for idx in range(args.shard_count):
        db_file = db_dir / f"{args.db_prefix}{idx}.db"
        scanned, seeded = read_shard_counts(db_file)
        grand_scanned += scanned
        grand_seeded += seeded
        print(f"shard {idx}: scanned={scanned:<8} seeded={seeded:<8} db={db_file.name}")

    print("-" * 44)
    print(f"TOTAL  : scanned={grand_scanned:<8} seeded={grand_seeded:<8}")

    if args.with_total:
        total_apps = get_total_apps()
        if total_apps:
            pct = (grand_scanned / total_apps) * 100 if total_apps else 0
            print(f"ALL APPS: {total_apps} | scanned pct: {pct:.2f}%")
        else:
            print("ALL APPS: unavailable (network/source temporarily failed)")


if __name__ == "__main__":
    main()
