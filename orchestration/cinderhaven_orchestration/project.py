"""Dagster project constants — paths and configuration."""

import shutil
from pathlib import Path

ORCHESTRATION_DIR = Path(__file__).parent
PROJECT_ROOT = ORCHESTRATION_DIR.parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "cinderhaven"
DBT_PROFILES_DIR = Path.home() / ".dbt"

DBT_EXECUTABLE = shutil.which("dbt") or "dbt"
