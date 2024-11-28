import sqlite3
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
    def __init__(self, db_file: str, export_dir: str):
        self.db_file = db_file
        self.export_dir = export_dir
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
                    study_date TEXT,
                    study_time TEXT,
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
                cursor.execute(
                    """
                    INSERT INTO volumes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        partition_key=excluded.partition_key,
                        patient_id=excluded.patient_id,
                        study_instance_uid=excluded.study_instance_uid,
                        series_instance_uid=excluded.series_instance_uid,
                        accession_number=excluded.accession_number,
                        modality=excluded.modality,
                        study_description=excluded.study_description,
                        series_description=excluded.series_description,
                        series_number=excluded.series_number,
                        study_date=excluded.study_date,
                        study_time=excluded.study_time,
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
                        volume.accession_number,
                        volume.modality,
                        volume.study_description,
                        volume.series_description,
                        volume.series_number,
                        volume.study_date,
                        volume.study_time,
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
                volume = Volume(
                    db_id=row[1],
                    patient_id=row[2],
                    study_instance_uid=row[3],
                    series_instance_uid=row[4],
                    accession_number=row[5],
                    modality=row[6],
                    study_description=row[7],
                    series_description=row[8],
                    series_number=row[9],
                    study_date=row[10],
                    study_time=row[11],
                    number_of_series_related_instances=row[12],
                    folder=row[13],
                    pseudonym=row[14],
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
    export_dir: str

    def create_io_manager(self, context: InitResourceContext) -> VolumesIOManager:
        if not context.instance:
            raise AssertionError("Missing instance in IO manager factory")

        storage_path = Path(context.instance.storage_directory())
        storage_path.mkdir(exist_ok=True, parents=True)
        db_path = storage_path / "volumes.sqlite"

        export_path = Path(self.export_dir)
        if not export_path.is_dir():
            raise AssertionError("Invalid volumes directory.")

        return VolumesIOManager(db_path.as_posix(), export_path.as_posix())
