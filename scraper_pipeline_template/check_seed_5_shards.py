import argparse
import platform
import sqlite3
import subprocess
from pathlib import Path

import requests


def get_total_apps() -> int | None:
    url = "https://raw.githubusercontent.com/dgibbs64/SteamCMD-AppID-List/master/steamcmd_appid.json"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return len(resp.json().get("applist", {}).get("apps", []))
    except Exception:
        return None


def get_db_counts(db_path: Path) -> tuple[int, int]:
    if not db_path.exists():
        return 0, 0

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    tables = {row[0] for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    scanned = cur.execute("SELECT COUNT(*) FROM scanned_apps").fetchone()[0] if "scanned_apps" in tables else 0
    seeded = cur.execute("SELECT COUNT(*) FROM games").fetchone()[0] if "games" in tables else 0
    conn.close()
    return scanned, seeded


def process_lines(shard_count: int) -> list[str]:
    system = platform.system().lower()

    if "windows" in system:
        # Windows/Trae 里通常没有 bash，改用 PowerShell 查询进程命令行。
        ps_cmd = (
            "Get-CimInstance Win32_Process "
            f"| Where-Object {{ $_.CommandLine -like '*--shard-count {shard_count}*' }} "
            "| ForEach-Object { \"PID=$($_.ProcessId) :: $($_.CommandLine)\" }"
        )
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True,
            text=True,
            check=False,
        )
        return [line for line in completed.stdout.splitlines() if line.strip()]

    cmd = ["bash", "-lc", f"ps -ef | grep -- '--shard-count {shard_count}' | grep -v grep || true"]
    completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return [line for line in completed.stdout.splitlines() if line.strip()]


def tail_lines(log_path: Path, n: int = 5) -> list[str]:
    if not log_path.exists():
        return ["log not found"]
    lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    return lines[-n:] if lines else ["(empty log)"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Check 5-shard seed progress")
    parser.add_argument("--base-dir", default=str(Path(__file__).resolve().parent))
    parser.add_argument("--shard-count", type=int, default=5)
    parser.add_argument("--db-prefix", type=str, default="steam_data_cache_5hash_shard")
    parser.add_argument("--log-prefix", type=str, default="seed_5hash_shard")
    parser.add_argument("--tail", type=int, default=5)
    parser.add_argument("--no-total", action="store_true")
    args = parser.parse_args()

    base_dir = Path(args.base_dir)

    print("=== 5-Shard Seed Processes ===")
    lines = process_lines(args.shard_count)
    if not lines:
        print("No running --shard-count 5 process found")
    else:
        for line in lines:
            print(line)
    print()

    print("=== Aggregate Progress ===")
    total_scanned = 0
    total_seeded = 0
    for i in range(args.shard_count):
        db = base_dir / f"{args.db_prefix}{i}.db"
        scanned, seeded = get_db_counts(db)
        total_scanned += scanned
        total_seeded += seeded
        print(f"shard {i}: scanned={scanned:<8} seeded={seeded:<8} db={db.name}")

    print("-" * 44)
    print(f"TOTAL  : scanned={total_scanned:<8} seeded={total_seeded:<8}")
    if not args.no_total:
        all_apps = get_total_apps()
        if all_apps:
            pct = (total_scanned / all_apps) * 100
            print(f"ALL APPS: {all_apps} | scanned pct: {pct:.2f}%")
        else:
            print("ALL APPS: unavailable")
    print()

    print("=== Latest Log Tail (each shard) ===")
    for i in range(args.shard_count):
        log = base_dir / f"{args.log_prefix}{i}.log"
        print(f"--- shard {i} ({log.name}) ---")
        for line in tail_lines(log, args.tail):
            print(line)
        print()


if __name__ == "__main__":
    main()
