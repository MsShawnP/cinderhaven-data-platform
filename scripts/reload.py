"""Full pipeline reload: DDL → ingest → dbt build.

Prerequisites:
    1. flyctl proxy running: flyctl proxy 5432 -a cinderhaven-db
    2. .env file with POSTGRES_PASSWORD
    3. For large tables (scan_data), Fly.io machine should be at 1GB:
       flyctl machine update <id> --memory 1024 -a cinderhaven-db

Usage:
    python scripts/reload.py
"""

import os
import subprocess
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_DIR = os.path.join(PROJECT_ROOT, "cinderhaven")


def run(cmd, description, cwd=None):
    print(f"\n{'='*60}")
    print(f"  {description}")
    print(f"  {' '.join(cmd)}")
    print(f"{'='*60}\n")

    start = time.time()
    result = subprocess.run(cmd, cwd=cwd or PROJECT_ROOT)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\nFAILED: {description} (exit code {result.returncode}, {elapsed:.1f}s)")
        sys.exit(result.returncode)

    print(f"\nOK: {description} ({elapsed:.1f}s)")
    return result


def main():
    print("Cinderhaven Data Platform — Full Reload")
    print(f"Project root: {PROJECT_ROOT}")
    print(f"dbt directory: {DBT_DIR}")

    overall_start = time.time()

    # Step 1: Ingest SQLite → Postgres
    run(
        [sys.executable, os.path.join("scripts", "ingest_sqlite_to_postgres.py")],
        "Step 1/3: Ingest SQLite → Postgres (raw schema)",
    )

    # Step 2: Install dbt packages
    run(
        [sys.executable, "-m", "dbt", "deps"],
        "Step 2/3: Install dbt packages",
        cwd=DBT_DIR,
    )

    # Step 3: dbt build (models + tests)
    run(
        [sys.executable, "-m", "dbt", "build"],
        "Step 3/3: dbt build (models + tests)",
        cwd=DBT_DIR,
    )

    total = time.time() - overall_start
    print(f"\n{'='*60}")
    print(f"  RELOAD COMPLETE ({total:.1f}s)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
