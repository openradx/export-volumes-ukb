from pydantic import BaseModel


class Volume(BaseModel):
    db_id: int | None
    pseudonym: str
    patient_id: str
    accession_number: str
    study_instance_uid: str
    series_instance_uid: str
    modality: str
    study_description: str
    series_description: str
    series_number: int
    study_date: str
    study_time: str
    institution_name: str
    number_of_series_related_instances: int
    folder: str | None
