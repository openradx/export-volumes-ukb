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
from dicognito.anonymizer import Anonymizer
from pydantic import Field
from pydicom import Dataset

from export_volumes_pipeline.io_managers import VolumesIOManager
from export_volumes_pipeline.models import Volume
from export_volumes_pipeline.utils import sanitize_filename

from .partitions import daily_partition
from .resources import PacsResource


class VolumesConfig(Config):
    modalities: str = Field(
        default=EnvVar("MODALITIES"),
        description="Comma separated list of modalities we want to download.",
    )
    min_volume_size: int = Field(
        default=EnvVar.int("MIN_VOLUME_SIZE"),
        description="Minimum number of images in the volume.",
    )
    institution_name: str = Field(
        default=EnvVar("INSTITUTION_NAME"),
        description="A string to filter studies by institution name.",
    )


@asset(partitions_def=daily_partition)
def found_volumes(
    context: AssetExecutionContext, config: VolumesConfig, pacs: PacsResource
) -> list[Volume]:
    time_window = context.partition_time_window
    start = time_window.start
    end = time_window.end - datetime.timedelta(seconds=1)

    found_studies: list[Dataset] = []
    modalities = [m.strip() for m in config.modalities.split(",")]
    for modality in modalities:
        studies = pacs.find_studies(start, end, modality)
        institution_name: str = config.institution_name
        for study in studies:
            if not pacs.check_institution_name(
                study.StudyInstanceUID, modalities, institution_name
            ):
                continue

            found_studies.append(study)

    found_volumes: list[Volume] = []
    for study in found_studies:
        # Each study gets a unique pseudonym
        pseudonym = shortuuid.uuid()
        series_list = pacs.find_series(study.StudyInstanceUID)
        for series in series_list:
            if series.Modality not in modalities:
                continue

            if series.NumberOfSeriesRelatedInstances < config.min_volume_size:
                continue

            found_volumes.append(
                Volume(
                    db_id=None,
                    pseudonym=pseudonym,
                    patient_id=study.PatientID,
                    accession_number=study.AccessionNumber,
                    study_instance_uid=study.StudyInstanceUID,
                    series_instance_uid=series.SeriesInstanceUID,
                    modality=series.Modality,
                    study_description=study.StudyDescription,
                    series_description=series.SeriesDescription,
                    series_number=int(series.SeriesNumber),
                    study_date=study.StudyDate,
                    study_time=study.StudyTime,
                    number_of_series_related_instances=series.NumberOfSeriesRelatedInstances,
                    folder=None,
                )
            )

    context.log.info(f"{len(found_volumes)} volumes found in {len(found_studies)} studies.")

    return found_volumes


@asset(partitions_def=daily_partition)
def exported_volumes(
    context: AssetExecutionContext,
    pacs: PacsResource,
    found_volumes: list[Volume],
) -> None:
    io_manager: VolumesIOManager = context.resources.io_manager
    export_path = Path(io_manager.export_dir)

    # Group volumes by study for anonymization
    volumes_by_study: dict[str, list[Volume]] = {}
    for volume in found_volumes:
        if volume.study_instance_uid not in volumes_by_study:
            volumes_by_study[volume.study_instance_uid] = []
        volumes_by_study[volume.study_instance_uid].append(volume)

    for _, volumes in volumes_by_study.items():
        anonymizer = Anonymizer()

        for volume in volumes:
            assert volume.db_id is not None

            year_and_month = volume.study_date[:6]
            volume_name = f"{volume.series_number}-{sanitize_filename(volume.series_description)}"
            volume_path = export_path / year_and_month / volume.pseudonym / volume_name
            volume_path.mkdir(parents=True, exist_ok=True)

            dicoms = pacs.download_series(volume.study_instance_uid, volume.series_instance_uid)
            for dicom in dicoms:
                anonymizer.anonymize(dicom)
                dicom.save_as(volume_path / f"{dicom.SOPInstanceUID}.dcm")

            io_manager.update_volume(volume.db_id, str(volume_path.absolute()))


all_assets = load_assets_from_current_module()
