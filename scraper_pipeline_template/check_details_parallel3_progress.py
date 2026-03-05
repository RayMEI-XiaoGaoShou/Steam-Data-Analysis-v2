import argparse
import os
import platform
import sqlite3
import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def process_lines(keyword: str) -> list[str]:
    system = platform.system().lower()
    if "windows" in system:
        ps_cmd = (
            "Get-CimInstance Win32_Process "
            f"| Where-Object {{ $_.CommandLine -like '*{keyword}*' }} "
            "| ForEach-Object { \"PID=$($_.ProcessId) :: $($_.CommandLine)\" }"
        )
        completed = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True,
            text=True,
            check=False,
        )
        return [line for line in completed.stdout.splitlines() if line.strip()]

    cmd = ["bash", "-lc", f"ps -ef | grep -- '{keyword}' | grep -v grep || true"]
    completed = subprocess.run(cmd, capture_output=True, text=True, check=False)
    return [line for line in completed.stdout.splitlines() if line.strip()]


def tail_lines(log_path: Path, n: int = 5) -> list[str]:
    if not log_path.exists():
        return ["log not found"]
    lines = log_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    return lines[-n:] if lines else ["(empty log)"]


def safe_print(text: str) -> None:
    encoding = getattr(sys.stdout, "encoding", None) or os.getenv("PYTHONIOENCODING") or "utf-8"
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode(encoding, errors="replace").decode(encoding, errors="replace"))


def shard_stats(db_path: Path) -> tuple[int, int, int, float]:
    if not db_path.exists():
        return 0, 0, 0, 0.0
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) FROM games").fetchone()[0]
    done = cur.execute("SELECT COUNT(*) FROM games WHERE last_updated IS NOT NULL").fetchone()[0]
    pending = total - done
    conn.close()
    pct = (done / total * 100) if total else 0.0
    return int(total), int(done), int(pending), pct


def main() -> None:
    parser = argparse.ArgumentParser(description="Progress dashboard for details parallel-3 workers")
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument("--db-prefix", type=str, default="steam_details_parallel3_shard")
    parser.add_argument("--log-prefix", type=str, default="details_parallel3_shard")
    parser.add_argument("--tail", type=int, default=5)
    args = parser.parse_args()

    print("=== Details Parallel Workers ===")
    procs = process_lines("steam_scraper_advanced.py --phase details --db-file Steam-Data-Analysis-v2/scraper/steam_details_parallel3_shard")
    if procs:
        for line in procs:
            safe_print(line)
    else:
        print("No running parallel details worker found")
    print()

    print("=== Details Parallel Progress ===")
    total_all = 0
    done_all = 0
    for i in range(args.count):
        db = BASE_DIR / f"{args.db_prefix}{i}.db"
        total, done, pending, pct = shard_stats(db)
        total_all += total
        done_all += done
        print(f"shard {i}: total={total:<6} done={done:<6} pending={pending:<6} pct={pct:>6.2f}%")
    print("-" * 58)
    overall_pct = (done_all / total_all * 100) if total_all else 0.0
    print(f"TOTAL  : total={total_all:<6} done={done_all:<6} pending={total_all-done_all:<6} pct={overall_pct:>6.2f}%")
    print()

    print("=== Latest Logs ===")
    for i in range(args.count):
        log = BASE_DIR / f"{args.log_prefix}{i}.log"
        print(f"--- shard {i} ({log.name}) ---")
        for line in tail_lines(log, args.tail):
            safe_print(line)
        print()


if __name__ == "__main__":
    main()
