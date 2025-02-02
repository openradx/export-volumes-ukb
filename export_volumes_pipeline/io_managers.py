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
                    pseudonym TEXT,
                    patient_id TEXT,
                    accession_number TEXT,
                    study_instance_uid TEXT,
                    series_instance_uid TEXT UNIQUE,
                    modality TEXT,
                    study_description TEXT,
                    series_description TEXT,
                    series_number INTEGER,
                    study_date TEXT,
                    study_time TEXT,
                    institution_name TEXT,
                    number_of_series_related_instances INTEGER,
                    folder TEXT,
                    found_volumes_run_id TEXT,
                    exported_volumes_run_id TEXT,
                    status TEXT
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
                    INSERT INTO volumes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        partition_key=excluded.partition_key,
                        pseudonym=excluded.pseudonym,
                        patient_id=excluded.patient_id,
                        accession_number=excluded.accession_number,
                        study_instance_uid=excluded.study_instance_uid,
                        series_instance_uid=excluded.series_instance_uid,
                        modality=excluded.modality,
                        study_description=excluded.study_description,
                        series_description=excluded.series_description,
                        series_number=excluded.series_number,
                        study_date=excluded.study_date,
                        study_time=excluded.study_time,
                        institution_name=excluded.institution_name,
                        number_of_series_related_instances=excluded.number_of_series_related_instances,
                        folder=excluded.folder,
                        found_volumes_run_id=excluded.found_volumes_run_id,
                        exported_volumes_run_id=excluded.exported_volumes_run_id,
                        status=excluded.status
                    """,  # noqa: E501
                    (
                        volume.db_id,
                        context.asset_partition_key,
                        volume.pseudonym,
                        volume.patient_id,
                        volume.accession_number,
                        volume.study_instance_uid,
                        volume.series_instance_uid,
                        volume.modality,
                        volume.study_description,
                        volume.series_description,
                        volume.series_number,
                        volume.study_date,
                        volume.study_time,
                        volume.institution_name,
                        volume.number_of_series_related_instances,
                        volume.folder,
                        volume.found_volumes_run_id,
                        volume.exported_volumes_run_id,
                        volume.status,
                    ),
                )
            conn.commit()
            context.log.debug(f"Saved data of {len(volumes)} volumes to db.")

    def load_input(self, context: InputContext) -> list[Volume]:
        if not context.asset_partition_key:
            raise AssertionError("Missing partition key in IO manager")

        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT * FROM volumes WHERE partition_key = ?", (context.asset_partition_key,)
            )
            rows = cursor.fetchall()
            context.log.debug(f"Loaded data of {len(rows)} volumes from db.")

            volumes = []
            for row in rows:
                volume = Volume(
                    db_id=row[0],
                    # skip partition_key row[1]
                    pseudonym=row[2],
                    patient_id=row[3],
                    accession_number=row[4],
                    study_instance_uid=row[5],
                    series_instance_uid=row[6],
                    modality=row[7],
                    study_description=row[8],
                    series_description=row[9],
                    series_number=row[10],
                    study_date=row[11],
                    study_time=row[12],
                    institution_name=row[13],
                    number_of_series_related_instances=row[14],
                    folder=row[15],
                    found_volumes_run_id=row[16],
                    exported_volumes_run_id=row[17],
                    status=row[18],
                )
                volumes.append(volume)

        return volumes

    def update_volume(self, db_id: int, folder: str | None, run_id: str, status: str) -> None:
        with sqlite3.connect(self.db_file) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE volumes
                SET folder = ?, exported_volumes_run_id = ?, status = ?
                WHERE id = ?
                """,
                (folder, run_id, status, db_id),
            )
            conn.commit()


class ConfigurableVolumesIOManager(ConfigurableIOManagerFactory):
    export_dir: str

    def create_io_manager(self, context: InitResourceContext) -> VolumesIOManager:
        if not context.instance:
            raise AssertionError("Missing instance in IO manager factory")

        root_dir = Path(context.instance.root_directory)
        db_path = root_dir / "volumes.sqlite"

        export_path = Path(self.export_dir)
        if not export_path.is_dir():
            raise AssertionError("Invalid volumes directory.")

        return VolumesIOManager(db_path.as_posix(), export_path.as_posix())
