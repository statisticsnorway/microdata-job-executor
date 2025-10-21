import shutil
from pathlib import Path
from typing import Protocol

from job_executor.adapter.fs.datastore_files import DatastoreDirectory
from job_executor.adapter.fs.input_files import InputDirectory
from job_executor.adapter.fs.working_files import WorkingDirectory


class FileSystemAdapter(Protocol):
    datastore_dir: DatastoreDirectory
    working_dir: WorkingDirectory
    input_dir: InputDirectory

    def move_working_dir_parquet_to_datastore(
        self, dataset_name: str
    ) -> None: ...


class LocalStorageAdapter:
    datastore_dir: DatastoreDirectory
    working_dir: WorkingDirectory
    input_dir: InputDirectory

    def __init__(self, datastore_dir_path: Path) -> None:
        self.datastore_dir = DatastoreDirectory(datastore_dir_path)
        self.working_dir = WorkingDirectory(
            Path(f"{datastore_dir_path}_working")
        )
        self.input_dir = InputDirectory(Path(f"{datastore_dir_path}_input"))

    def move_working_dir_parquet_to_datastore(self, dataset_name: str) -> None:
        """
        Moves the given parquet DRAFT file from the working directory to
        the appropriate datastore sub directory.

        * dataset_name: str - name of dataset
        """
        working_dir_parquet_path = self.working_dir._get_draft_parquet_path(
            dataset_name
        )
        shutil.move(
            working_dir_parquet_path,
            (
                self.datastore_dir.data_dir
                / dataset_name
                / working_dir_parquet_path.parts[-1]
            ),
        )
