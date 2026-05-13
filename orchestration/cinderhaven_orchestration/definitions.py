"""Dagster Definitions — the entry point for `dagster dev`."""

from dagster import Definitions, ScheduleDefinition, define_asset_job
from dagster_dbt import DbtCliResource

from .assets import cinderhaven_dbt_assets
from .project import DBT_PROJECT_DIR, DBT_PROFILES_DIR, DBT_EXECUTABLE


# Job that materializes all dbt assets.
dbt_full_refresh = define_asset_job(
    name="dbt_full_refresh",
    selection="*",
    description="Run full dbt build: staging → intermediate → marts with tests.",
)

# Daily schedule — 6 AM UTC.
daily_refresh = ScheduleDefinition(
    job=dbt_full_refresh,
    cron_schedule="0 6 * * *",
    description="Daily full pipeline refresh at 6 AM UTC.",
)

defs = Definitions(
    assets=[cinderhaven_dbt_assets],
    schedules=[daily_refresh],
    resources={
        "dbt": DbtCliResource(
            project_dir=str(DBT_PROJECT_DIR),
            profiles_dir=str(DBT_PROFILES_DIR),
            dbt_executable=DBT_EXECUTABLE,
        ),
    },
)
