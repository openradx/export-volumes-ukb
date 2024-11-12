import os
import shutil
from pathlib import Path
from typing import Literal

from invoke.context import Context
from invoke.tasks import task

Environments = Literal["dev", "prod"]

stack_name_dev = "report_etl_pipeline_dev"
stack_name_prod = "report_etl_pipeline_prod"

project_dir = Path(__file__).resolve().parent
compose_dir = project_dir / "compose"

compose_file_base = compose_dir / "docker-compose.base.yml"
compose_file_dev = compose_dir / "docker-compose.dev.yml"
compose_file_prod = compose_dir / "docker-compose.prod.yml"

###
# Helper functions
###


def get_stack_name(env: Environments):
    if env == "dev":
        return stack_name_dev
    elif env == "prod":
        return stack_name_prod
    else:
        raise ValueError(f"Unknown environment: {env}")


def build_compose_cmd(env: Environments):
    base_compose_cmd = f"docker compose -f '{compose_file_base}'"
    stack_name = get_stack_name(env)
    if env == "dev":
        return f"{base_compose_cmd} -f '{compose_file_dev}' -p {stack_name}"
    elif env == "prod":
        return f"{base_compose_cmd} -f '{compose_file_prod}' -p {stack_name}"
    else:
        raise ValueError(f"Unknown environment: {env}")


###
# Tasks
###


@task
def lint(ctx: Context):
    """Lint the source code (ruff, pyright)"""
    cmd_ruff = "poetry run ruff check ."
    ctx.run(cmd_ruff, pty=True)
    cmd_pyright = "poetry run pyright"
    ctx.run(cmd_pyright, pty=True)


@task
def compose_up(
    ctx: Context,
    env: Environments = "dev",
    port: int | None = None,
    no_build: bool = False,
):
    """Start Dagster pipeline

    Args:
        env (Environments, optional): Environment to use. Defaults to "dev".
        port (int, optional): Port to access the web UI. Defaults to 3000.
        no_build (bool, optional): Skip build step. Defaults to False.
    """
    build_opt = "--no-build" if no_build else "--build"
    cmd: list[str] = []
    if port is not None:
        if env == "dev":
            cmd.append(f"export DAGSTER_DEV_PORT={port} && ")
        elif env == "prod":
            cmd.append(f"export DAGSTER_NGINX_PORT={port} && ")
        else:
            raise ValueError(f"Unknown environment: {env}")

    cmd.append(f"{build_compose_cmd(env)} up {build_opt} --detach")
    ctx.run(" ".join(cmd), pty=True, env={"UID": str(os.getuid()), "GID": str(os.getgid())})


@task
def compose_down(
    ctx: Context,
    env: Environments = "dev",
    cleanup: bool = False,
):
    """Stop Dagster pipeline"""
    cmd = f"{build_compose_cmd(env)} down"
    if cleanup:
        cmd += " --remove-orphans --volumes"
    ctx.run(cmd, pty=True, env={"UID": str(os.getuid()), "GID": str(os.getgid())})


@task
def clean_dagster_home(ctx: Context, env: Environments = "dev"):
    """Clean dagster_home folder"""
    if env == "dev":
        dagster_home_dir = project_dir / "dagster_home_dev"
    elif env == "prod":
        dagster_home_dir = project_dir / "dagster_home_prod"
    else:
        raise ValueError(f"Unknown environment: {env}")

    for item in dagster_home_dir.glob("*"):
        if not item.name == "dagster.yaml":
            shutil.rmtree(item)


@task
def show_outdated(ctx: Context):
    """Show outdated dependencies"""
    ctx.run("poetry show --outdated --top-level")
