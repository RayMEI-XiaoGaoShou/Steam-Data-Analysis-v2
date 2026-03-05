import argparse
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def init_games_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA busy_timeout=5000")
    cur.execute(
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
    conn.commit()
    conn.close()


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row)


def build_completed_db(completed_sources: list[Path], completed_db: Path) -> set[int]:
    init_games_db(completed_db)
    out_conn = sqlite3.connect(completed_db)
    out_cur = out_conn.cursor()
    out_cur.execute("DELETE FROM games")

    completed_appids: set[int] = set()

    for src in completed_sources:
        if not src.exists():
            continue
        conn = sqlite3.connect(src)
        if not table_exists(conn, "games"):
            conn.close()
            continue

        rows = conn.execute(
            """
            SELECT appid, name, release_date, review_count, positive_rate,
                   tag1, tag2, tag3, tag4, tag5, last_updated
            FROM games
            WHERE last_updated IS NOT NULL
            """
        ).fetchall()

        for row in rows:
            appid = int(row[0])
            out_cur.execute(
                """
                INSERT OR REPLACE INTO games (
                    appid, name, release_date, review_count, positive_rate,
                    tag1, tag2, tag3, tag4, tag5, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
            completed_appids.add(appid)

        conn.close()

    out_conn.commit()
    out_conn.close()
    return completed_appids


def collect_seeded_pending(seed_sources: list[Path], exclude_appids: set[int]) -> list[tuple]:
    best_by_appid: dict[int, tuple] = {}

    for src in seed_sources:
        if not src.exists():
            continue
        conn = sqlite3.connect(src)
        if not table_exists(conn, "games"):
            conn.close()
            continue

        rows = conn.execute(
            """
            SELECT appid, name, review_count, positive_rate
            FROM games
            """
        ).fetchall()

        for appid, name, review_count, positive_rate in rows:
            appid = int(appid)
            if appid in exclude_appids:
                continue
            existing = best_by_appid.get(appid)
            current_reviews = int(review_count or 0)
            if existing is None or current_reviews > int(existing[2] or 0):
                best_by_appid[appid] = (
                    appid,
                    name,
                    current_reviews,
                    float(positive_rate or 0.0),
                )

        conn.close()

    return sorted(best_by_appid.values(), key=lambda x: x[2], reverse=True)


def write_parallel_batches(rows: list[tuple], output_prefix: str, output_count: int) -> list[Path]:
    buckets = [[] for _ in range(output_count)]
    for idx, row in enumerate(rows):
        buckets[idx % output_count].append(row)

    output_paths: list[Path] = []
    for i, bucket_rows in enumerate(buckets):
        db_path = BASE_DIR / f"{output_prefix}{i}.db"
        init_games_db(db_path)
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("DELETE FROM games")
        for appid, name, review_count, positive_rate in bucket_rows:
            cur.execute(
                """
                INSERT OR REPLACE INTO games (
                    appid, name, release_date, review_count, positive_rate,
                    tag1, tag2, tag3, tag4, tag5, last_updated
                ) VALUES (?, ?, '', ?, ?, '', '', '', '', '', NULL)
                """,
                (appid, name, review_count, positive_rate),
            )
        conn.commit()
        conn.close()
        output_paths.append(db_path)

    return output_paths


def count_rows(db_path: Path) -> int:
    conn = sqlite3.connect(db_path)
    val = conn.execute("SELECT COUNT(*) FROM games").fetchone()[0]
    conn.close()
    return int(val)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare completed DB and split remaining details work into parallel batches")
    parser.add_argument("--seed-prefix", type=str, default="steam_data_cache_5hash_shard")
    parser.add_argument("--seed-count", type=int, default=5)
    parser.add_argument(
        "--completed-sources",
        nargs="+",
        default=[
            str(BASE_DIR / "steam_details_batch_5000.db"),
            str(BASE_DIR / "steam_details_batch_6000.db"),
        ],
    )
    parser.add_argument("--completed-db", type=str, default=str(BASE_DIR / "steam_details_completed.db"))
    parser.add_argument("--parallel-count", type=int, default=3)
    parser.add_argument("--output-prefix", type=str, default="steam_details_parallel3_shard")
    args = parser.parse_args()

    seed_sources = [BASE_DIR / f"{args.seed_prefix}{i}.db" for i in range(args.seed_count)]
    completed_sources = [Path(p) for p in args.completed_sources]
    completed_db = Path(args.completed_db)

    completed_appids = build_completed_db(completed_sources, completed_db)
    remaining_rows = collect_seeded_pending(seed_sources, completed_appids)
    output_paths = write_parallel_batches(remaining_rows, args.output_prefix, args.parallel_count)

    print(f"Completed details DB: {completed_db} | rows={count_rows(completed_db)}")
    print(f"Remaining unique rows after exclusion: {len(remaining_rows)}")
    for p in output_paths:
        print(f"Batch DB: {p} | rows={count_rows(p)}")


if __name__ == "__main__":
    main()
