from datetime import datetime

from pydantic import BaseModel


class Volume(BaseModel):
    db_id: int | None
    patient_id: str
    study_instance_uid: str
    series_instance_uid: str
    modality: str
    study_description: str
    series_description: str
    series_number: int
    study_datetime: datetime
    number_of_series_related_instances: int
    folder: str | None
    pseudonym: str | None
