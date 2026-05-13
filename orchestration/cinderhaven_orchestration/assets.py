"""dbt assets loaded into Dagster.

Uses dagster-dbt to automatically create one Dagster asset per dbt model,
preserving the full dependency graph (staging → intermediate → marts).
"""

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

from .project import DBT_PROJECT_DIR, DBT_PROFILES_DIR, DBT_EXECUTABLE


dbt_resource = DbtCliResource(
    project_dir=str(DBT_PROJECT_DIR),
    profiles_dir=str(DBT_PROFILES_DIR),
    dbt_executable=DBT_EXECUTABLE,
)

# Parse the dbt manifest at load time so Dagster knows the asset graph.
dbt_manifest_path = dbt_resource.cli(
    ["--quiet", "parse"],
    target_path=DBT_PROJECT_DIR / "target",
).target_path.joinpath("manifest.json")


@dbt_assets(manifest=dbt_manifest_path)
def cinderhaven_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """Materialize all dbt models as Dagster assets."""
    yield from dbt.cli(["build"], context=context).stream()
