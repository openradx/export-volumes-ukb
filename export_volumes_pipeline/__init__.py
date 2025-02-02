from dagster import Definitions, EnvVar

from export_volumes_pipeline import io_managers, resources

from . import assets

defs = Definitions(
    assets=assets.all_assets,
    resources={
        "io_manager": io_managers.ConfigurableVolumesIOManager(
            export_dir=EnvVar("EXPORT_DIR"),
        ),
        "pacs": resources.PacsResource(
            calling_ae_title=EnvVar("CALLING_AE_TITLE"),
            pacs_ae_title=EnvVar("PACS_AE_TITLE"),
            pacs_host=EnvVar("PACS_HOST"),
            pacs_port=EnvVar.int("PACS_PORT"),
        ),
    },
)
