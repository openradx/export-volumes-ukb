from dagster import Definitions, EnvVar

from export_volumes_pipeline import io_managers, resources

from . import assets

defs = Definitions(
    assets=assets.all_assets,
    resources={
        "io_manager": io_managers.ConfigurableVolumesIOManager(
            export_dir=EnvVar("EXPORT_DIR"),
        ),
        "adit": resources.AditResource(
            host=EnvVar("ADIT_HOST"),
            auth_token=EnvVar("ADIT_AUTH_TOKEN"),
            verify_ssl=EnvVar("VERIFY_SSL"),
            ca_bundle=EnvVar("CA_BUNDLE"),
        ),
    },
)
