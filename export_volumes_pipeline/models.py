from pydantic import BaseModel


class Volume(BaseModel):
    db_id: int | None
    patient_id: str
    study_instance_uid: str
    series_instance_uid: str
    accession_number: str
    modality: str
    study_description: str
    series_description: str
    series_number: int
    study_date: str
    study_time: str
    number_of_series_related_instances: int
    folder: str | None
    pseudonym: str | None
