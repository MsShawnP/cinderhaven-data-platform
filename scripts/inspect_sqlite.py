"""Inspect the Cinderhaven SQLite database schema and row counts."""
import sqlite3
import sys

DB_PATH = r"C:\Users\mssha\projects\active\cinderhaven-data\data\cinderhaven_product_master.db"

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = [r[0] for r in cursor.fetchall()]

    print(f"Found {len(tables)} tables\n")

    for t in tables:
        cursor.execute(f'PRAGMA table_info("{t}")')
        cols = cursor.fetchall()
        cursor.execute(f'SELECT COUNT(*) FROM "{t}"')
        count = cursor.fetchone()[0]
        print(f"=== {t} ({count:,} rows) ===")
        for c in cols:
            cid, name, dtype, notnull, default, pk = c
            flags = []
            if pk:
                flags.append("PK")
            if notnull:
                flags.append("NOT NULL")
            if default is not None:
                flags.append(f"DEFAULT {default}")
            flag_str = f"  [{', '.join(flags)}]" if flags else ""
            print(f"  {name:35s} {dtype or 'TEXT':15s}{flag_str}")
        print()

    conn.close()

if __name__ == "__main__":
    main()
