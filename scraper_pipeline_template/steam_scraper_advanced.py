import argparse
import csv
import hashlib
import logging
import random
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup


BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "steam_data_cache.db"
DEFAULT_EXPORT_FILE = BASE_DIR / "steam_data_final.csv"
MIN_REVIEWS = 200

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

REVIEW_SUMMARY_PARAMS = {
    "json": 1,
    "language": "all",
    "review_type": "all",
    "purchase_type": "all",
    "filter": "all",
    "num_per_page": 0,
}


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS games (
            appid INTEGER PRIMARY KEY,
            name TEXT,
            release_date TEXT,
            review_count INTEGER,
            positive_rate REAL,
            tag1 TEXT,
            tag2 TEXT,
            tag3 TEXT,
            tag4 TEXT,
            tag5 TEXT,
            last_updated TEXT
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS scanned_apps (
            appid INTEGER PRIMARY KEY,
            scanned_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def get_counts(conn: sqlite3.Connection) -> tuple[int, int]:
    cursor = conn.cursor()
    total = cursor.execute("SELECT COUNT(*) FROM games").fetchone()[0]
    pending = cursor.execute("SELECT COUNT(*) FROM games WHERE last_updated IS NULL").fetchone()[0]
    return total, pending


def fetch_and_filter_appids(
    conn: sqlite3.Connection,
    app_scan_limit: int | None = None,
    shard_index: int = 0,
    shard_count: int = 1,
    shard_method: str = "hash",
    shuffle: bool = False,
) -> None:
    cursor = conn.cursor()
    app_list_url = "https://raw.githubusercontent.com/dgibbs64/SteamCMD-AppID-List/master/steamcmd_appid.json"
    logger.info("Fetching full AppID list from mirror...")

    resp = requests.get(app_list_url, timeout=15)
    resp.raise_for_status()
    apps = resp.json().get("applist", {}).get("apps", [])
    logger.info("Fetched %s apps", len(apps))

    if shuffle:
        random.shuffle(apps)

    def bucket_for_appid(appid: int) -> int:
        if shard_method == "mod":
            return appid % shard_count
        digest = hashlib.blake2b(str(appid).encode("utf-8"), digest_size=8).digest()
        value = int.from_bytes(digest, byteorder="big", signed=False)
        return value % shard_count

    if shard_count > 1:
        apps = [app for app in apps if app.get("appid") and bucket_for_appid(int(app["appid"])) == shard_index]
        logger.info(
            "Shard mode (%s): shard %s/%s owns %s apps",
            shard_method,
            shard_index,
            shard_count,
            len(apps),
        )

    if app_scan_limit is not None:
        apps = apps[:app_scan_limit]
        logger.info("Seed mode: scanning first %s apps", app_scan_limit)

    scanned_count = 0
    inserted = 0
    for app in apps:
        appid = app.get("appid")
        name = (app.get("name") or "").strip()
        if not appid or not name:
            continue

        scanned_row = cursor.execute("SELECT 1 FROM scanned_apps WHERE appid = ?", (appid,)).fetchone()
        if scanned_row:
            continue

        cursor.execute(
            "INSERT OR IGNORE INTO scanned_apps (appid, scanned_at) VALUES (?, ?)",
            (appid, datetime.now().isoformat()),
        )
        conn.commit()

        review_url = f"https://store.steampowered.com/appreviews/{appid}"
        try:
            review_resp = requests.get(
                review_url,
                params=REVIEW_SUMMARY_PARAMS,
                headers=HEADERS,
                timeout=10,
            )
            data = review_resp.json()
        except Exception:
            time.sleep(0.5)
            continue

        if data.get("success") != 1:
            time.sleep(0.5)
            continue

        qs = data.get("query_summary", {})
        total_reviews = int(qs.get("total_reviews", 0))
        if total_reviews > MIN_REVIEWS:
            positive = int(qs.get("total_positive", 0))
            positive_rate = round(positive / total_reviews, 4)
            cursor.execute(
                """
                INSERT OR IGNORE INTO games (appid, name, review_count, positive_rate)
                VALUES (?, ?, ?, ?)
                """,
                (appid, name, total_reviews, positive_rate),
            )
            conn.commit()
            inserted += 1
            logger.info("Seeded: %s (%s reviews)", name, total_reviews)

        scanned_count += 1
        if scanned_count % 100 == 0:
            logger.info("Seed progress: scanned=%s inserted=%s", scanned_count, inserted)
        time.sleep(0.5)

    logger.info("Seed complete: scanned=%s inserted=%s", scanned_count, inserted)


def scrape_store_details(
    conn: sqlite3.Connection,
    details_limit: int | None = None,
    min_delay: float = 1.5,
    max_delay: float = 2.5,
) -> None:
    cursor = conn.cursor()
    pending_games = cursor.execute(
        """
        SELECT appid, name
        FROM games
        WHERE last_updated IS NULL
        ORDER BY review_count DESC
        """
    ).fetchall()

    if not pending_games:
        logger.info("No pending rows. Details stage already complete.")
        return

    if details_limit is not None:
        pending_games = pending_games[:details_limit]

    logger.info("Details stage: processing %s pending games", len(pending_games))
    cookies = {"birthtime": "283993201", "lastagecheckage": "1-0-1979"}

    for idx, (appid, name) in enumerate(pending_games, start=1):
        url = f"https://store.steampowered.com/app/{appid}/"
        try:
            resp = requests.get(url, headers=HEADERS, cookies=cookies, timeout=12)

            if resp.url != url and not resp.url.endswith(f"/{appid}/"):
                cursor.execute(
                    "UPDATE games SET last_updated = ? WHERE appid = ?",
                    (datetime.now().isoformat(), appid),
                )
                conn.commit()
                logger.warning("Skipped redirected app: %s", name)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            tags = [t.text.strip() for t in soup.select("a.app_tag") if t.text.strip() != "+"][:5]
            date_el = soup.select_one(".release_date .date")
            release_date = date_el.text.strip() if date_el else ""
            row_tags = tags + [""] * (5 - len(tags))

            cursor.execute(
                """
                UPDATE games
                SET release_date = ?,
                    tag1 = ?,
                    tag2 = ?,
                    tag3 = ?,
                    tag4 = ?,
                    tag5 = ?,
                    last_updated = ?
                WHERE appid = ?
                """,
                (
                    release_date,
                    row_tags[0],
                    row_tags[1],
                    row_tags[2],
                    row_tags[3],
                    row_tags[4],
                    datetime.now().isoformat(),
                    appid,
                ),
            )
            conn.commit()

            logger.info(
                "[%s/%s] Details done: %s | date=%s | tags=%s",
                idx,
                len(pending_games),
                name,
                release_date,
                ", ".join(tags) if tags else "none",
            )
            time.sleep(random.uniform(min_delay, max_delay))
        except Exception as exc:
            logger.warning("Details failed for %s (%s): %s", name, appid, exc)
            time.sleep(2)


def export_to_csv(conn: sqlite3.Connection, output_file: Path) -> None:
    cursor = conn.cursor()
    rows = cursor.execute(
        """
        SELECT appid, name, release_date, review_count, positive_rate,
               tag1, tag2, tag3, tag4, tag5
        FROM games
        WHERE last_updated IS NOT NULL
          AND release_date IS NOT NULL
          AND release_date != ''
        ORDER BY review_count DESC
        """
    ).fetchall()

    if not rows:
        logger.info("No rows available for export yet.")
        return

    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "AppID",
                "Game",
                "Release Date",
                "Review Count",
                "Positive Rate",
                "Tag1",
                "Tag2",
                "Tag3",
                "Tag4",
                "Tag5",
            ]
        )
        writer.writerows(rows)

    logger.info("Exported %s rows to %s", len(rows), output_file)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Steam data scraper with SQLite resume support")
    parser.add_argument("--phase", choices=["all", "seed", "details", "export"], default="all")
    parser.add_argument("--seed-limit", type=int, default=None, help="Only scan first N apps during seed")
    parser.add_argument("--details-limit", type=int, default=None, help="Only process first N pending rows")
    parser.add_argument("--db-file", type=str, default=str(DB_FILE), help="SQLite DB file path")
    parser.add_argument("--shard-index", type=int, default=0, help="Current shard index (0-based)")
    parser.add_argument("--shard-count", type=int, default=1, help="Total shard count")
    parser.add_argument(
        "--shard-method",
        choices=["hash", "mod"],
        default="hash",
        help="Shard assignment method (hash recommended)",
    )
    parser.add_argument("--shuffle", action="store_true", help="Shuffle app list before seed")
    parser.add_argument("--min-delay", type=float, default=1.5)
    parser.add_argument("--max-delay", type=float, default=2.5)
    parser.add_argument("--output", type=str, default=str(DEFAULT_EXPORT_FILE))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    global DB_FILE
    DB_FILE = Path(args.db_file)

    if args.shard_count < 1:
        raise ValueError("--shard-count must be >= 1")
    if args.shard_index < 0 or args.shard_index >= args.shard_count:
        raise ValueError("--shard-index must be in [0, shard-count)")

    conn = init_db()

    total_before, pending_before = get_counts(conn)
    logger.info("DB status before run: total=%s pending=%s", total_before, pending_before)

    if args.phase in ("all", "seed"):
        logger.info("=== Phase 1: Seed by official reviews API ===")
        fetch_and_filter_appids(
            conn,
            app_scan_limit=args.seed_limit,
            shard_index=args.shard_index,
            shard_count=args.shard_count,
            shard_method=args.shard_method,
            shuffle=args.shuffle,
        )

    if args.phase in ("all", "details"):
        logger.info("=== Phase 2: Fetch store details with resume ===")
        scrape_store_details(
            conn,
            details_limit=args.details_limit,
            min_delay=args.min_delay,
            max_delay=args.max_delay,
        )

    if args.phase in ("all", "export"):
        logger.info("=== Phase 3: Export dataset ===")
        export_to_csv(conn, Path(args.output))

    total_after, pending_after = get_counts(conn)
    logger.info("DB status after run: total=%s pending=%s", total_after, pending_after)

    conn.close()


if __name__ == "__main__":
    main()
