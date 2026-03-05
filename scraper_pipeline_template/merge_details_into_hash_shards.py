import argparse
import hashlib
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def hash_bucket(appid: int, shard_count: int) -> int:
    digest = hashlib.blake2b(str(appid).encode("utf-8"), digest_size=8).digest()
    value = int.from_bytes(digest, byteorder="big", signed=False)
    return value % shard_count


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge completed details batch DB back into hash shard DBs")
    parser.add_argument("--details-db", type=str, default=str(BASE_DIR / "steam_details_batch_5000.db"))
    parser.add_argument("--target-prefix", type=str, default="steam_data_cache_5hash_shard")
    parser.add_argument("--target-count", type=int, default=5)
    args = parser.parse_args()

    details_db = Path(args.details_db)
    if not details_db.exists():
        raise FileNotFoundError(f"Details DB not found: {details_db}")

    dconn = sqlite3.connect(details_db)
    dcur = dconn.cursor()
    rows = dcur.execute(
        """
        SELECT appid, release_date, tag1, tag2, tag3, tag4, tag5, last_updated
        FROM games
        WHERE last_updated IS NOT NULL
        """
    ).fetchall()
    dconn.close()

    target_conns = []
    for i in range(args.target_count):
        db = BASE_DIR / f"{args.target_prefix}{i}.db"
        conn = sqlite3.connect(db)
        conn.execute("PRAGMA busy_timeout=5000")
        target_conns.append(conn)

    merged = 0
    for appid, release_date, tag1, tag2, tag3, tag4, tag5, last_updated in rows:
        b = hash_bucket(int(appid), args.target_count)
        cur = target_conns[b].cursor()
        cur.execute(
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
            (release_date, tag1, tag2, tag3, tag4, tag5, last_updated, int(appid)),
        )
        merged += cur.rowcount

    for conn in target_conns:
        conn.commit()
        conn.close()

    print(f"Details rows read: {len(rows)}")
    print(f"Shard rows updated: {merged}")


if __name__ == "__main__":
    main()
