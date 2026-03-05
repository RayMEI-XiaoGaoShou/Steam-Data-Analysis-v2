import csv
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def init_final_db(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
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


def merge_hash_shards_to_final(
    final_db: Path,
    shard_prefix: str = "steam_data_cache_5hash_shard",
    shard_count: int = 5,
) -> tuple[int, int]:
    init_final_db(final_db)
    fconn = sqlite3.connect(final_db)
    fcur = fconn.cursor()
    fcur.execute("DELETE FROM games")

    inserted = 0
    for i in range(shard_count):
        shard_db = BASE_DIR / f"{shard_prefix}{i}.db"
        if not shard_db.exists():
            continue

        sconn = sqlite3.connect(shard_db)
        scur = sconn.cursor()
        rows = scur.execute(
            """
            SELECT appid, name, release_date, review_count, positive_rate,
                   tag1, tag2, tag3, tag4, tag5, last_updated
            FROM games
            """
        ).fetchall()

        for row in rows:
            fcur.execute(
                """
                INSERT OR REPLACE INTO games (
                    appid, name, release_date, review_count, positive_rate,
                    tag1, tag2, tag3, tag4, tag5, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
            inserted += 1

        sconn.close()

    fconn.commit()
    total = fcur.execute("SELECT COUNT(*) FROM games").fetchone()[0]
    fconn.close()
    return inserted, int(total)


def export_final_csv(final_db: Path, output_csv: Path) -> tuple[int, int]:
    conn = sqlite3.connect(final_db)
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT appid, name, release_date, review_count, positive_rate,
               tag1, tag2, tag3, tag4, tag5, last_updated
        FROM games
        ORDER BY review_count DESC
        """
    ).fetchall()

    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "appid",
                "game",
                "release_date",
                "review_count",
                "positive_rate",
                "tag1",
                "tag2",
                "tag3",
                "tag4",
                "tag5",
                "last_updated",
            ]
        )
        writer.writerows(rows)

    complete = sum(1 for r in rows if r[10] is not None)
    conn.close()
    return len(rows), complete


def main() -> None:
    final_db = BASE_DIR / "steam_data_final_merged.db"
    final_csv = BASE_DIR / "steam_data_final_merged.csv"

    inserted, total = merge_hash_shards_to_final(final_db)
    csv_total, details_done = export_final_csv(final_db, final_csv)

    print(f"Merged rows processed from shards: {inserted}")
    print(f"Unique rows in final DB: {total}")
    print(f"Exported CSV rows: {csv_total}")
    print(f"Rows with details status timestamp: {details_done}")
    print(f"Final DB: {final_db}")
    print(f"Final CSV: {final_csv}")


if __name__ == "__main__":
    main()
