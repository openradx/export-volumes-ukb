import datetime
from pathlib import Path

import shortuuid
from dagster import (
    AssetExecutionContext,
    Config,
    EnvVar,
    asset,
    load_assets_from_current_module,
)
from pydantic import Field
from pydicom import Dataset

from export_volumes_pipeline.io_managers import VolumesIOManager
from export_volumes_pipeline.models import Volume

from .partitions import daily_partition
from .resources import AditResource


class VolumesConfig(Config):
    pacs_ae_title: str = Field(
        default=EnvVar("PACS_AE_TITLE"),
        description=("The AE title of the PACS to download volumes from."),
    )
    modalities: str = Field(
        default=EnvVar("MODALITIES"),
        description="Comma separated list of modalities we want to download.",
    )
    min_volume_size: int = Field(
        default=EnvVar.int("MIN_VOLUME_SIZE"),
        description="Minimum number of images in the volume.",
    )
    excluded_study_description_prefix: str = Field(
        default=EnvVar("EXCLUDED_STUDY_DESCRIPTION_PREFIX"),
        description="An optional prefix to exclude studies by description.",
    )


@asset(partitions_def=daily_partition)
def found_volumes(
    context: AssetExecutionContext, config: VolumesConfig, adit: AditResource
) -> list[Volume]:
    time_window = context.partition_time_window
    start = time_window.start
    end = time_window.end - datetime.timedelta(seconds=1)

    found_studies: list[Dataset] = []
    modalities = [m.strip() for m in config.modalities.split(",")]
    for modality in modalities:
        studies = adit.find_studies(config.pacs_ae_title, start, end, modality)
        for study in studies:
            if study.StudyDescription.startswith(config.excluded_study_description_prefix):
                continue

            found_studies.append(study)

    found_volumes: list[Volume] = []
    for study in found_studies:
        series_list = adit.find_series(config.pacs_ae_title, study.StudyInstanceUID)
        for series in series_list:
            if series.Modality not in modalities:
                continue

            if series.NumberOfSeriesRelatedInstances < config.min_volume_size:
                continue

            found_volumes.append(
                Volume(
                    db_id=None,
                    patient_id=study.PatientID,
                    study_instance_uid=study.StudyInstanceUID,
                    series_instance_uid=series.SeriesInstanceUID,
                    accession_number=study.AccessionNumber,
                    modality=series.Modality,
                    study_description=study.StudyDescription,
                    series_description=series.SeriesDescription,
                    series_number=int(series.SeriesNumber),
                    study_date=study.StudyDate,
                    study_time=study.StudyTime,
                    number_of_series_related_instances=series.NumberOfSeriesRelatedInstances,
                    folder=None,
                    pseudonym=None,
                )
            )

    context.log.info(f"{len(found_volumes)} volumes found in {len(found_studies)} studies.")

    return found_volumes


@asset(partitions_def=daily_partition)
def exported_volumes(
    context: AssetExecutionContext,
    config: VolumesConfig,
    adit: AditResource,
    found_volumes: list[Volume],
) -> None:
    io_manager: VolumesIOManager = context.resources.io_manager
    export_path = Path(io_manager.export_dir)

    for volume in found_volumes:
        assert volume.db_id is not None

        pseudonym = shortuuid.uuid()
        output_path = export_path / pseudonym
        output_path.mkdir()

        dicoms = adit.download_series(
            config.pacs_ae_title, volume.study_instance_uid, volume.series_instance_uid, pseudonym
        )

        for dicom in dicoms:
            dicom.save_as(output_path / f"{dicom.SOPInstanceUID}.dcm")

        io_manager.update_volume(volume.db_id, output_path.absolute().as_posix(), pseudonym)


all_assets = load_assets_from_current_module()
