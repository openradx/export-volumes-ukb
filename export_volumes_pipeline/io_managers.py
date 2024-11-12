import sqlite3
from datetime import datetime
from pathlib import Path

from dagster import (
    ConfigurableIOManagerFactory,
    InitResourceContext,
    InputContext,
    IOManager,
    OutputContext,
)

from .models import Volume


class VolumesIOManager(IOManager):
    def __init__(self, db_file: str, volumes_dir: str):
        self.db_file = db_file
        self.volumes_dir = volumes_dir
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS volumes (
                    id INTEGER PRIMARY KEY,
                    partition_key TEXT,
                    patient_id TEXT,
                    study_instance_uid TEXT,
                    series_instance_uid TEXT,
                    modality TEXT,
                    study_description TEXT,
                    series_description TEXT,
                    series_number INTEGER,
                    study_datetime TEXT,
                    number_of_series_related_instances INTEGER,
                    folder TEXT,
                    pseudonym TEXT
                );
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_partition_key ON volumes(partition_key);
                """
            )
            conn.commit()

    def handle_output(self, context: OutputContext, volumes: list[Volume]) -> None:
        if not context.asset_partition_key:
            raise AssertionError("Missing partition key in IO manager")

        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            for volume in volumes:
                study_datetime_str = volume.study_datetime.isoformat()
                cursor.execute(
                    """
                    INSERT INTO volumes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        partition_key=excluded.partition_key,
                        patient_id=excluded.patient_id,
                        study_instance_uid=excluded.study_instance_uid,
                        series_instance_uid=excluded.series_instance_uid,
                        modality=excluded.modality,
                        study_description=excluded.study_description,
                        series_description=excluded.series_description,
                        series_number=excluded.series_number,
                        study_datetime=excluded.study_datetime,
                        number_of_series_related_instances=excluded.number_of_series_related_instances,
                        folder=excluded.folder,
                        pseudonym=excluded.pseudonym
                    """,
                    (
                        volume.db_id,
                        context.asset_partition_key,
                        volume.patient_id,
                        volume.study_instance_uid,
                        volume.series_instance_uid,
                        volume.modality,
                        volume.study_description,
                        volume.series_description,
                        volume.series_number,
                        study_datetime_str,
                        volume.number_of_series_related_instances,
                        volume.folder,
                        volume.pseudonym,
                    ),
                )
            conn.commit()

    def load_input(self, context: InputContext) -> list[Volume]:
        if not context.asset_partition_key:
            raise AssertionError("Missing partition key in IO manager")

        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM volumes WHERE partition_key = ?", (context.asset_partition_key,)
            )
            rows = cursor.fetchall()

            volumes = []
            for row in rows:
                study_datetime = datetime.fromisoformat(row[9])
                volume = Volume(
                    db_id=row[1],
                    patient_id=row[2],
                    study_instance_uid=row[3],
                    series_instance_uid=row[4],
                    modality=row[5],
                    study_description=row[6],
                    series_description=row[7],
                    series_number=row[8],
                    study_datetime=study_datetime,
                    number_of_series_related_instances=row[10],
                    folder=row[11],
                    pseudonym=row[12],
                )
                volumes.append(volume)

        return volumes

    def update_volume(self, db_id: int, folder: str, pseudonym: str) -> None:
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE volumes SET folder = ?, pseudonym = ? WHERE id = ?",
                (folder, pseudonym, db_id),
            )
            conn.commit()


class ConfigurableVolumesIOManager(ConfigurableIOManagerFactory):
    db_file: str
    volumes_dir: str

    def create_io_manager(self, context: InitResourceContext) -> VolumesIOManager:
        volumes_path = Path(self.volumes_dir)
        volumes_path.mkdir(parents=True, exist_ok=True)

        return VolumesIOManager(self.db_file, self.volumes_dir)
