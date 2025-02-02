#!/usr/bin/env python3

import argparse
import shutil
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clean the datagster home directory")
    parser.add_argument("--production", action="store_true", help="Clean the production folder")
    args = parser.parse_args()

    if args.production:
        dagster_home_dir = PROJECT_DIR / "dagster_home_prod"
    else:
        dagster_home_dir = PROJECT_DIR / "dagster_home_dev"

    for item in dagster_home_dir.glob("*"):
        if not item.name == "dagster.yaml":
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
            else:
                raise ValueError(f"Unexpected item: {item}")
