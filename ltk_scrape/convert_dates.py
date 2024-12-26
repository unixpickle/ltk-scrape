"""
I accidentally inserted dates as strings instead of epoch integers.

This script corrects for that.
"""

from tqdm.auto import tqdm
import sqlite3
import argparse
from .client import parse_timestamp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db_path", type=str, default="db.db")
    args = parser.parse_args()

    db = sqlite3.connect(args.db_path)
    print("reading rows...")
    all_rows = db.execute(
        "SELECT id, date_created, date_updated, date_published FROM ltks"
    ).fetchall()
    print("converting...")
    cursor = db.cursor()
    for id, date_created, date_updated, date_published in tqdm(all_rows):
        date_created = parse_timestamp(date_created)
        date_updated = parse_timestamp(date_updated)
        date_published = parse_timestamp(date_published)
        cursor.execute(
            """
            UPDATE ltks
            SET date_created = ?, date_updated = ?, date_published = ?
            WHERE id = ?
            """,
            (date_created, date_updated, date_published, id),
        )
    db.commit()


if __name__ == "__main__":
    main()
