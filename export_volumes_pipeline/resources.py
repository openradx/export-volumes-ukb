from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from adit_client import AditClient
from dagster import ConfigurableResource, DagsterLogManager
from dagster._core.execution.context.init import InitResourceContext
from pydantic import Field, PrivateAttr
from pydicom import Dataset


class AditResource(ConfigurableResource):
    host: str
    auth_token: str
    ca_bundle: str
    max_search_results: int = Field(
        default=199,
        description=(
            "The maximum number of C-FIND results. Each PACS has a maximum result count. "
            "If the number of results is higher than this number will be automatically split "
            "during search."
        ),
    )

    _client: AditClient = PrivateAttr()
    _logger: DagsterLogManager = PrivateAttr()

    def setup_for_execution(self, context: InitResourceContext) -> None:
        verify: str | bool = True
        if self.ca_bundle:
            ca_bundle_path = Path(self.ca_bundle)
            if ca_bundle_path.is_absolute():
                verify = ca_bundle_path.as_posix()
            else:
                instance = context.instance
                assert instance
                verify = (Path(instance.root_directory) / ca_bundle_path).as_posix()

        self._client = AditClient(server_url=self.host, auth_token=self.auth_token, verify=verify)

        if not context.log:
            raise ValueError("Missing log manager.")
        self._logger = context.log

    def find_studies(
        self, ae_title: str, start: datetime, end: datetime, modality: str
    ) -> list[Dataset]:
        start_date = start.strftime("%Y%m%d")
        end_date = end.strftime("%Y%m%d")
        start_time = start.strftime("%H%M%S")
        end_time = end.strftime("%H%M%S")

        study_date = f"{start_date} - {end_date}" if start_date != end_date else start_date
        study_time = f"{start_time} - {end_time}" if start_time != end_time else start_time

        query = {
            "StudyDate": study_date,
            "StudyTime": study_time,
            "ModalitiesInStudy": modality,
        }

        self._logger.debug(f"Find studies: {query}")

        results = self._client.search_for_studies(ae_title, query)
        num_results = len(results)

        self._logger.debug(f"Number of found studies: {num_results}")

        if num_results > self.max_search_results:
            self._logger.debug("Too many studies found, narrowing time window.")
            delta = end - start

            if delta < timedelta(seconds=1800):  # 30 mins
                raise ValueError(f"Time window too small ({start} to {end}).")

            mid = start + delta / 2
            part1 = self.find_studies(ae_title, start, mid, modality)
            part2 = self.find_studies(ae_title, mid, end, modality)
            return part1 + part2

        return results

    def find_series(self, ae_title: str, study_instance_uid: str) -> list[Dataset]:
        self._logger.debug(f"Find series of study {study_instance_uid}.")

        results = self._client.search_for_series(ae_title, study_instance_uid, {})

        self._logger.debug(f"Number of found series: {len(results)}")

        return results

    def download_series(
        self,
        ae_title: str,
        study_instance_uid: str,
        series_instance_uid: str,
        pseudonym: str | None,
    ) -> Iterable[Dataset]:
        self._logger.debug(f"Fetching series with SeriesInstanceUID {study_instance_uid}.")

        yield from self._client.iter_series(
            ae_title, study_instance_uid, series_instance_uid, pseudonym
        )
