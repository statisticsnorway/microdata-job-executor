import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

from job_executor.adapter.fs.models.metadata import Metadata


@dataclass
class WorkingDirectory:
    path: Path

    def _get_draft_parquet_path(self, dataset_name: str) -> Path:
        parquet_file_path = self.path / f"{dataset_name}__DRAFT.parquet"
        partitioned_parquet_path = self.path / f"{dataset_name}__DRAFT"
        if partitioned_parquet_path.is_dir():
            return partitioned_parquet_path
        elif parquet_file_path.is_file():
            return parquet_file_path
        else:
            raise FileExistsError(
                f"Invalid parquet path in {self.path} for {dataset_name}"
            )

    def write_metadata(self, dataset_name: str, metadata: Metadata) -> None:
        """
        Writes a json to a the working directory as the processed metadata file
        named: {dataset_name}__DRAFT.json

        * dataset_name: str - name of dataset
        * metadata: Metadata - Metadata to write as json
        """
        file_path = self.path / f"{dataset_name}__DRAFT.json"
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(
                metadata.model_dump(by_alias=True, exclude_none=True),
                f,
            )

    def get_metadata(self, dataset_name: str) -> Metadata:
        """
        Returns the working dir metadata json file for given dataset_name
        as a Metadata object.

        * dataset_name: str - name of dataset
        """
        file_path = self.path / f"{dataset_name}__DRAFT.json"
        with open(file_path, "r", encoding="utf-8") as f:
            return Metadata.model_validate(json.load(f))

    def delete_metadata(self, dataset_name: str) -> None:
        """
        Deletes the metadata in working directory with postfix __DRAFT.json

        * dataset_name: str - name of dataset
        """
        metadata_path = self.path / f"{dataset_name}__DRAFT.json"
        if os.path.isfile(metadata_path):
            os.remove(metadata_path)

    def get_input_metadata(self, dataset_name: str) -> dict:
        """
        Returns the working dir metadata json file for given dataset_name.

        * dataset_name: str - name of dataset
        """
        file_path = self.path / f"{dataset_name}.json"
        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def delete_input_metadata(self, dataset_name: str) -> None:
        """
        Deletes the metadata in working directory with postfix __DRAFT.json

        * dataset_name: str - name of dataset
        """
        metadata_path = self.path / f"{dataset_name}.json"
        if os.path.isfile(metadata_path):
            os.remove(metadata_path)

    def delete_file(self, file_name: str) -> None:
        """
        Deletes a file from the working directory.
        Intended to clean up left-over files.

        * file_name: str - name of temporary file
        """
        file_path = self.path / file_name
        if file_path.is_file():
            os.remove(file_path)

    def delete_sub_directory(self, directory_name: str) -> None:
        """
        Deletes a directory from the working directory.
        Intended to clean up left-over directories.
        Raises a LocalStorageError if dirpath is not in
        the working directory.

        * dir_path: str - name of temporary directory
        """
        dir_path = self.path / directory_name
        if dir_path.is_dir():
            shutil.rmtree(dir_path)
