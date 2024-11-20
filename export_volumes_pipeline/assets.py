import re
from datetime import datetime, timedelta
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
    institution_name_regex: str = Field(
        default=EnvVar("INSTITUTION_NAME_REGEX"),
        description="An optional regex to exclude studies by institution name.",
    )
    institution_address_regex: str = Field(
        default=EnvVar("INSTITUTION_ADDRESS_REGEX"),
        description="An optional regex to exclude studies by institution address.",
    )


@asset(partitions_def=daily_partition)
def found_volumes(
    context: AssetExecutionContext, config: VolumesConfig, adit: AditResource
) -> list[Volume]:
    time_window = context.partition_time_window
    start = time_window.start
    end = time_window.end - timedelta(seconds=1)

    found_studies: list[Dataset] = []
    modalities = [m.strip() for m in config.modalities.split(",")]
    for modality in modalities:
        studies = adit.find_studies(config.pacs_ae_title, start, end, modality)
        if config.institution_name_regex:
            studies = [
                study
                for study in studies
                if re.search(config.institution_name_regex, study.StudyDescription)
            ]
        if config.institution_address_regex:
            studies = [
                study
                for study in studies
                if re.search(config.institution_address_regex, study.StudyDescription)
            ]
        found_studies.extend(studies)

    found_volumes: list[Volume] = []
    for study in studies:
        series_list = adit.find_series(config.pacs_ae_title, study.StudyInstanceUID)
        for series in series_list:
            if series.Modality not in modalities:
                continue

            study_datetime = datetime.combine(study.StudyDate, study.StudyTime)
            found_volumes.append(
                Volume(
                    db_id=None,
                    patient_id=study.PatientID,
                    study_instance_uid=study.StudyInstanceUID,
                    series_instance_uid=series.SeriesInstanceUID,
                    modality=series.Modality,
                    study_description=study.StudyDescription,
                    series_description=series.SeriesDescription,
                    series_number=int(series.SeriesNumber),
                    study_datetime=study_datetime,
                    number_of_series_related_instances=series.NumberOfSeriesRelatedInstances,
                    folder=None,
                    pseudonym=None,
                )
            )

    context.log.info(f"{len(found_volumes)} volumes found.")

    return found_volumes


@asset(partitions_def=daily_partition)
def exported_volumes(
    context: AssetExecutionContext,
    config: VolumesConfig,
    adit: AditResource,
    found_volumes: list[Volume],
) -> None:
    io_manager: VolumesIOManager = context.resources.io_manager
    volumes_path = Path(io_manager.volumes_dir)

    for volume in found_volumes:
        assert volume.db_id is not None

        pseudonym = shortuuid.uuid()
        output_path = volumes_path / pseudonym
        output_path.mkdir()

        dicoms = adit.download_series(
            config.pacs_ae_title, volume.study_instance_uid, volume.series_instance_uid, pseudonym
        )

        for dicom in dicoms:
            dicom.save_as(output_path / f"{dicom.SOPInstanceUID}.dcm")

        io_manager.update_volume(volume.db_id, output_path.absolute().as_posix(), pseudonym)


all_assets = load_assets_from_current_module()
