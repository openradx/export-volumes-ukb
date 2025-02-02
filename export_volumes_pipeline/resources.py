from datetime import datetime, timedelta
from pathlib import Path

from dagster import ConfigurableResource, DagsterLogManager
from dagster._core.execution.context.init import InitResourceContext
from pydantic import Field, PrivateAttr
from pydicom import Dataset

from export_volumes_pipeline.pacs_client import PacsClient
from export_volumes_pipeline.utils import is_falsy


class PacsResource(ConfigurableResource):
    calling_ae_title: str
    pacs_ae_title: str
    pacs_host: str
    pacs_port: int

    max_search_results: int = Field(
        default=199,
        description=(
            "The maximum number of C-FIND results. Each PACS has a maximum result count. "
            "If the number of results is higher than this number will be automatically split "
            "during search."
        ),
    )

    _client: PacsClient = PrivateAttr()
    _logger: DagsterLogManager = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        verify: str | bool = not is_falsy(self.verify_ssl)
        if verify and self.ca_bundle:
            ca_bundle_path = Path(self.ca_bundle)
            if ca_bundle_path.is_absolute():
                verify = ca_bundle_path.as_posix()
            else:
                instance = context.instance
                assert instance
                verify = (Path(instance.root_directory) / ca_bundle_path).as_posix()

        self._client = PacsClient(
            self.calling_ae_title,
            self.pacs_ae_title,
            self.pacs_host,
            self.pacs_port,
        )

        if not context.log:
            raise ValueError("Missing log manager.")
        self._logger = context.log

    def find_studies(self, start: datetime, end: datetime, modality: str) -> list[Dataset]:
        start_date = start.strftime("%Y%m%d")
        end_date = end.strftime("%Y%m%d")
        start_time = start.strftime("%H%M%S")
        end_time = end.strftime("%H%M%S")

        study_date = f"{start_date}-{end_date}" if start_date != end_date else start_date
        study_time = f"{start_time}-{end_time}" if start_time != end_time else start_time

        query = {
            "QueryRetrieveLevel": "STUDY",
            "StudyDate": study_date,
            "StudyTime": study_time,
            "ModalitiesInStudy": modality,
            "PatientID": "",
            "AccessionNumber": "",
            "StudyInstanceUID": "",
            "StudyDescription": "",
        }

        self._logger.debug(f"Find studies with query: {query}")

        results = list(self._client.find(query))
        num_results = len(results)
        self._logger.debug(f"Number of found studies: {num_results}")

        if num_results > self.max_search_results:
            self._logger.debug("Too many studies found, narrowing time window.")
            delta = end - start

            if delta < timedelta(seconds=1800):  # 30 mins
                raise ValueError(f"Time window too small ({start} to {end}).")

            mid = start + delta / 2
            part1 = self.find_studies(start, mid, modality)
            part2 = self.find_studies(mid, end, modality)
            return part1 + part2

        return results

    def find_series(self, study_instance_uid: str) -> list[Dataset]:
        self._logger.debug(f"Find series of study {study_instance_uid}.")

        query = {
            "QueryRetrieveLevel": "SERIES",
            "StudyInstanceUID": study_instance_uid,
            "SeriesInstanceUID": "",
            "Modality": "",
            "SeriesDescription": "",
            "SeriesNumber": "",
            "NumberOfSeriesRelatedInstances": "",
        }

        self._logger.debug(f"Find series with query: {query}")

        results = list(self._client.find(query))
        self._logger.debug(f"Number of found series: {len(results)}")

        return results

    def check_institution_name(
        self, study_instance_uid: str, modalities: list[str], institution_name
    ) -> bool:
        """Check if the study belongs to the provided institution.

        The PACS in Bonn only allows to query the InstitutionName on IMAGE level.
        """
        query = {
            "QueryRetrieveLevel": "SERIES",
            "StudyInstanceUID": study_instance_uid,
            "SeriesInstanceUID": "",
        }

        for series in list(self._client.find(query)):
            if series.Modality not in modalities:
                continue

            query = {
                "QueryRetrieveLevel": "IMAGE",
                "StudyInstanceUID": series.StudyInstanceUID,
                "SeriesInstanceUID": series.SeriesInstanceUID,
                "InstitutionName": "",
            }

            images = list(self._client.find(query))

            if not images:
                continue

            return institution_name in images[0].InstitutionName

        return False
