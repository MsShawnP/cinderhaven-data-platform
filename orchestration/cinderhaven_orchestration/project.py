"""Dagster project constants — paths and configuration."""

from pathlib import Path

ORCHESTRATION_DIR = Path(__file__).parent
PROJECT_ROOT = ORCHESTRATION_DIR.parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "cinderhaven"
DBT_PROFILES_DIR = Path.home() / ".dbt"

# dbt.exe is not on PATH; specify full path.
DBT_EXECUTABLE = str(
    Path.home()
    / "AppData/Local/Packages/PythonSoftwareFoundation.Python.3.13_qbz5n2kfra8p0"
    / "LocalCache/local-packages/Python313/Scripts/dbt.exe"
)
