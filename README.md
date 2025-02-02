# Image Export Pipeline

## About

Image Export Pipeline is a [Dagster](https://dagster.io/) pipeline to export and anonymize radiological images from a PACS.

## Setup

- Both development and production uses Docker Compose to setup the Dagster server
- `dagster_home_dev` resp. `dagster_home_prod` folder in the workspace is mounted as `DAGSTER_HOME` folder. Every data output by the pipelines is stored in those folders.
- Copy `example.env` to `.env` and adjust the variables accordingly.
- Artifacts are stored according to `ARTIFACTS_DIR`. If `ARTIFACTS_DIR` is not set then the files are stored in the `DAGSTER_HOME` folder under `storage`.
- A relative `ARTIFACTS_DIR` path is stored relative to `DAGSTER_HOME` which is `dagster_home_dev` folder in development and `dagster_home_prod` folder in production.
- Production uses Nginx for basic auth and SSL encryption.
  - Execute `./nginx/setup_production.sh` to setup SSL certificates and basic auth.
- Attach the virtual environment with `poetry shell` and then start the stack with `inv compose-up`.
- Forward port `3500` in development resp. `3600` in production to Dagster UI in VS Code ports tab.
- Alternatively (for testing purposes), run a single job from command line, e.g. `python ./scripts/materialize_assets.py -d ./artifacts/ 2023-01-01`.
