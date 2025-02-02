#!/usr/bin/env python3

import argparse
import subprocess
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the docker-compose services")
    parser.add_argument("--production", action="store_true", help="Start the production services")
    parser.add_argument("--cleanup", action="store_true", help="Remove orphans and volumes")
    args = parser.parse_args()

    compose_base_file = PROJECT_DIR / "docker-compose.base.yml"

    if args.production:
        compose_env_file = PROJECT_DIR / "docker-compose.prod.yml"
    else:
        compose_env_file = PROJECT_DIR / "docker-compose.dev.yml"

    if args.production:
        project_name = "radiome_prod"
    else:
        project_name = "radiome_dev"

    cmd = [
        "docker",
        "compose",
        "-f",
        str(compose_base_file),
        "-f",
        str(compose_env_file),
        "-p",
        project_name,
        "down",
    ]

    if args.cleanup:
        cmd.append("--remove-orphans")
        cmd.append("--volumes")

    print(" ".join(cmd))
    subprocess.run(cmd, check=True)
