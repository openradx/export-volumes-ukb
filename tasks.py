import shutil
from pathlib import Path

from adit_radis_shared import invoke_tasks
from adit_radis_shared.invoke_tasks import (  # noqa: F401
    Utility,
    compose_down,
    compose_up,
    format,
    lint,
    show_outdated,
)
from invoke.context import Context
from invoke.tasks import task

invoke_tasks.PROJECT_NAME = "radiome"
invoke_tasks.PROJECT_DIR = Path(__file__).resolve().parent


@task
def clean_dagster_home(ctx: Context, production: bool = False):
    """Clean dagster_home folder"""
    if production:
        dagster_home_dir = Utility.get_project_dir() / "dagster_home_prod"
    else:
        dagster_home_dir = Utility.get_project_dir() / "dagster_home_dev"

    for item in dagster_home_dir.glob("*"):
        if not item.name == "dagster.yaml":
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
            else:
                raise ValueError(f"Unexpected item: {item}")
