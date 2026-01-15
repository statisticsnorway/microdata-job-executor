import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from job_executor.adapter.fs.models.datastore_versions import (
    DatastoreVersions,
    DraftVersion,
)
from job_executor.adapter.fs.models.metadata import (
    MetadataAll,
    MetadataAllDraft,
)
from job_executor.common.exceptions import LocalStorageError


class DatastoreDirectory:
    root_dir: Path
    data_dir: Path
    metadata_dir: Path
    draft_metadata_all_path: Path
    datastore_versions_path: Path
    draft_version_path: Path
    archive_dir: Path

    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir
        self.data_dir = root_dir / "data"
        self.metadata_dir = root_dir / "datastore"
        self.draft_version_path = self.metadata_dir / "draft_version.json"
        self.archive_dir = self.root_dir / "archive"
        self.draft_metadata_all_path = (
            self.metadata_dir / "metadata_all__DRAFT.json"
        )
        self.datastore_versions_path = (
            self.metadata_dir / "datastore_versions.json"
        )

    def _get_draft_parquet_path(self, dataset_name: str) -> Path:
        parquet_file_path = (
            self.data_dir / dataset_name / f"{dataset_name}__DRAFT.parquet"
        )
        partitioned_parquet_path = (
            self.data_dir / dataset_name / f"{dataset_name}__DRAFT"
        )
        if partitioned_parquet_path.is_dir():
            return partitioned_parquet_path
        elif parquet_file_path.is_file():
            return parquet_file_path
        else:
            raise FileExistsError(
                f"Invalid parquet path in {self.data_dir} for {dataset_name}"
            )

    def make_dataset_dir(self, dataset_name: str) -> None:
        """
        Creates sub-directories for dataset_name in the datastore
        /data directory.

        * dataset_name: str - name of dataset
        """
        os.makedirs(self.data_dir / dataset_name, exist_ok=True)

    def get_data_versions(self, version: str | None) -> dict:
        """
        Returns the data_versions json file for the given version as a dict.
        Returns an empty dictionary if given version is None.

        * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
        """
        if version is None:
            return {}
        file_version = "_".join(version.split("_")[:-1])
        file_path = self.metadata_dir / f"data_versions__{file_version}.json"
        with open(file_path, "r") as f:
            return json.load(f)

    def write_data_versions(self, data_versions: dict, version: str) -> None:
        """
        Writes given dict to a new data versions json file to the appropriate
        datastore directory named with the given version.

        * data_versions: dict - data versions dict
        * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
        """
        file_version = "_".join(version.split("_")[:-1])
        file_path = (
            self.root_dir / f"datastore/data_versions__{file_version}.json"
        )
        with open(file_path, "w") as f:
            return json.dump(data_versions, f, indent=2)

    def get_draft_version(self) -> DraftVersion:
        """
        Reads the draft version file from the datastore.
        """
        file_path = self.draft_version_path
        with open(file_path, "r") as f:
            return DraftVersion.model_validate(json.load(f))

    def write_draft_version(self, draft_version: DraftVersion) -> None:
        """
        Writes json representation of object to the draft version json file
        by alias.
        """
        file_path = self.draft_version_path
        with open(file_path, "w") as f:
            return json.dump(
                draft_version.model_dump(by_alias=True), f, indent=2
            )

    def get_datastore_versions(self) -> DatastoreVersions:
        """
        Returns the contents of the datastore versions json file as
        DatastoreVersions object.
        """
        file_path = self.datastore_versions_path
        with open(file_path, "r") as f:
            return DatastoreVersions.model_validate(json.load(f))

    def write_datastore_versions(
        self, datastore_versions: DatastoreVersions
    ) -> None:
        """
        Writes json representation of object to the draft version json file
        by alias.
        """
        file_path = self.datastore_versions_path
        with open(file_path, "w") as f:
            return json.dump(
                datastore_versions.model_dump(by_alias=True), f, indent=2
            )

    def get_metadata_all(self, version: str) -> MetadataAll:
        """
        Returns the metadata all json file for the given version.

        * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
        """
        file_path = self.metadata_dir / f"metadata_all__{version}.json"
        with open(file_path, "r") as f:
            return MetadataAll.model_validate(json.load(f))

    def write_metadata_all(
        self, metadata_all: MetadataAll, version: str
    ) -> None:
        """
        Writes given metadata all to the appropriate json file in the
        datastore directory named with the given version.

        * metadata_all: MetadataAll - A MetadataAll object
        * version: str - '<MAJOR>_<MINOR>_<PATCH>'
        """
        file_path = self.metadata_dir / f"metadata_all__{version}.json"
        with open(file_path, "w") as f:
            return json.dump(
                metadata_all.model_dump(by_alias=True, exclude_none=True),
                f,
                indent=2,
            )

    def get_metadata_all_draft(self) -> MetadataAllDraft:
        """
        Returns the metadata all draft json file.
        """
        file_path = self.draft_metadata_all_path
        with open(file_path, "r") as f:
            return MetadataAllDraft.model_validate(json.load(f))

    def write_metadata_all_draft(
        self, metadata_all_draft: MetadataAllDraft
    ) -> None:
        """
        Writes json representation of object to the metadata all draft json file
        by alias. A tmp file will be written to first
        to avoid downtime in consuming services due to incomplete json while
        writing.
        """
        tmp_file_path = f"{self.draft_metadata_all_path}.tmp"
        with open(tmp_file_path, "w", encoding="utf-8") as f:
            json.dump(
                metadata_all_draft.model_dump(by_alias=True, exclude_none=True),
                f,
                indent=2,
            )
        os.remove(self.draft_metadata_all_path)
        shutil.move(tmp_file_path, self.draft_metadata_all_path)

    def rename_parquet_draft_to_release(
        self, dataset_name: str, version: str
    ) -> str:
        """
        Renames the parquet DRAFT file or directory for the given dataset_name,
        with the given version.

        * dataset_name: str - name of dataset
        * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
        """
        draft_path = self._get_draft_parquet_path(dataset_name)
        file_version = "_".join(version.split("_")[:-1])
        file_name = (
            draft_path.stem.replace("DRAFT", file_version) + draft_path.suffix
        )
        release_path = draft_path.parent / file_name
        shutil.move(draft_path, release_path)
        return release_path.name

    def delete_parquet_draft(self, dataset_name: str) -> None:
        """
        Deletes the parquet draft file or directory for the given dataset_name.

        * dataset_name: str - name of dataset
        """
        data_dir = self.root_dir / f"data/{dataset_name}"
        partitioned_parquet_path = data_dir / f"{dataset_name}__DRAFT"
        parquet_file_path = data_dir / f"{dataset_name}__DRAFT.parquet"
        if partitioned_parquet_path.is_dir():
            shutil.rmtree(partitioned_parquet_path)
        elif parquet_file_path.is_file():
            os.remove(parquet_file_path)

    def save_temporary_backup(self) -> None:
        """
        Backs up metadata_all__DRAFT.json, datastore_versions.json and
        draft_version.json from the datastore to a tmp directory
        inside the datastore directory.
        Raises `LocalStorageError` if tmp directory already exists.
        """
        with open(
            f"{self.root_dir}/datastore/datastore_versions.json",
            encoding="utf-8",
        ) as f:
            datastore_versions = json.load(f)
        with open(self.draft_version_path, encoding="utf-8") as f:
            draft_version = json.load(f)
        with open(self.draft_metadata_all_path, encoding="utf-8") as f:
            metadata_all_draft = json.load(f)
        tmp_dir = self.metadata_dir / "tmp"
        if os.path.isdir(tmp_dir):
            raise LocalStorageError("tmp directory already exists")
        os.mkdir(tmp_dir)
        with open(tmp_dir / "draft_version.json", "w", encoding="utf-8") as f:
            json.dump(draft_version, f, indent=2)
        with open(
            tmp_dir / "metadata_all__DRAFT.json", "w", encoding="utf-8"
        ) as f:
            json.dump(metadata_all_draft, f, indent=2)
        with open(
            tmp_dir / "datastore_versions.json", "w", encoding="utf-8"
        ) as f:
            json.dump(datastore_versions, f, indent=2)

    def restore_from_temporary_backup(self) -> str | None:
        """
        Restores the datastore from the tmp directory.
        Raises `LocalStorageError`if there are any missing backup files.

        Returns None if no released version in backup, else returns the
        latest release version number as dotted four part version.
        """
        tmp_dir = self.metadata_dir / "tmp"
        draft_version_backup = tmp_dir / "draft_version.json"
        metadata_all_draft_backup = tmp_dir / "metadata_all__DRAFT.json"
        datastore_versions_backup = tmp_dir / "datastore_versions.json"
        backup_exists = (
            os.path.isdir(tmp_dir)
            and os.path.isfile(draft_version_backup)
            and os.path.isfile(metadata_all_draft_backup)
            and os.path.isfile(datastore_versions_backup)
        )
        if not backup_exists:
            raise LocalStorageError("Missing tmp backup files")
        try:
            with open(datastore_versions_backup, "r") as f:
                datastore_versions = json.load(f)
            shutil.move(draft_version_backup, self.draft_version_path)
            shutil.move(metadata_all_draft_backup, self.draft_metadata_all_path)
            shutil.move(datastore_versions_backup, self.datastore_versions_path)
            if datastore_versions["versions"] == []:
                return None
            else:
                return datastore_versions["versions"][0]["version"]
        except ValidationError as e:
            raise LocalStorageError("Invalid backup file") from e

    def archive_temporary_backup(self) -> None:
        """
        Archives the tmp directory within the datastore if the directory
        exists. Raises `LocalStorageError` if there are any unrecognized files
        in the directory.
        """
        tmp_dir = self.metadata_dir / "tmp"
        os.makedirs(self.archive_dir, exist_ok=True)

        if not os.path.isdir(tmp_dir):
            raise LocalStorageError(
                "Could not find a tmp directory to archive."
            )
        for content in os.listdir(tmp_dir):
            if content not in [
                "datastore_versions.json",
                "metadata_all__DRAFT.json",
                "draft_version.json",
            ]:
                raise LocalStorageError(
                    "Found unrecognized files and/or directories in the tmp "
                    "directory. Aborting tmp archiving."
                )
        timestamp = datetime.now(UTC).replace(tzinfo=None)
        shutil.move(tmp_dir, self.archive_dir / f"tmp_{timestamp}")

    def delete_temporary_backup(self) -> None:
        """
        Deletes the tmp directory within the datastore if the directory
        exists. Raises `LocalStorageError` if there are any unrecognized files
        in the directory.
        """
        tmp_dir = self.metadata_dir / "tmp"
        os.makedirs(self.archive_dir, exist_ok=True)

        if not os.path.isdir(tmp_dir):
            raise LocalStorageError("Could not find a tmp directory to delete.")
        for content in os.listdir(tmp_dir):
            if content not in [
                "datastore_versions.json",
                "metadata_all__DRAFT.json",
                "draft_version.json",
            ]:
                raise LocalStorageError(
                    "Found unrecognized files and/or directories in the tmp "
                    "directory. Aborting tmp deleting."
                )
        shutil.rmtree(tmp_dir)

    def temporary_backup_exists(self) -> bool:
        """
        Returns a boolean representing if the tmp directory exists.
        """
        tmp_dir = self.metadata_dir / "tmp"
        return os.path.isdir(tmp_dir)

    def archive_draft_version(self, version: str) -> None:
        """
        Archives the current draft json
        * dataset_name: str - name of dataset draft
        * version: str - version of the archived draft
        """
        os.makedirs(self.archive_dir, exist_ok=True)

        timestamp = datetime.now(UTC).replace(tzinfo=None)

        archived_draft_version_path = (
            self.archive_dir / f"draft_version_{version}_{timestamp}.json"
        )
        shutil.copyfile(self.draft_version_path, archived_draft_version_path)
