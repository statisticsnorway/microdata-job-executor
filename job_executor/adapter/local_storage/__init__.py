import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from job_executor.common.exceptions import LocalStorageError
from job_executor.config import environment

WORKING_DIR = Path(environment.working_dir)
DATASTORE_DIR = Path(environment.datastore_dir)
INPUT_DIR = Path(environment.input_dir)

DATASTORE_VERSIONS_PATH = DATASTORE_DIR / "datastore/datastore_versions.json"
DRAFT_METADATA_ALL_PATH = DATASTORE_DIR / "datastore/metadata_all__DRAFT.json"
DRAFT_VERSION_PATH = DATASTORE_DIR / "datastore/draft_version.json"
ARCHIVE_DIR = DATASTORE_DIR / "archive"


def _read_json(file_path: Path) -> dict:
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(
    content: dict, file_path: Path, indent: int | None = None
) -> None:
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(content, f, indent=indent)


def _write_json_with_tmp(
    content: dict, file_path: Path, indent: int | None = None
) -> None:
    tmp_file_path = f"{file_path}.tmp"
    with open(tmp_file_path, "w", encoding="utf-8") as f:
        json.dump(content, f, indent=indent)
    os.remove(file_path)
    shutil.move(tmp_file_path, file_path)


def _get_parquet_path(directory: Path, dataset_name: str) -> Path:
    parquet_file_path = directory / f"{dataset_name}__DRAFT.parquet"
    partitioned_parquet_path = directory / f"{dataset_name}__DRAFT"
    if partitioned_parquet_path.is_dir():
        return partitioned_parquet_path
    elif parquet_file_path.is_file():
        return parquet_file_path
    else:
        raise FileExistsError(
            f"Invalid parquet path in {directory} for {dataset_name}"
        )


def _get_datastore_draft_parquet_path(dataset_name: str) -> Path:
    return _get_parquet_path(
        DATASTORE_DIR / f"data/{dataset_name}", dataset_name
    )


def _get_working_dir_draft_parquet_path(dataset_name: str) -> Path:
    return _get_parquet_path(WORKING_DIR, dataset_name)


def make_dataset_dir(dataset_name: str) -> None:
    """
    Creates sub-directories for dataset_name in the datastore /data directory.

    * dataset_name: str - name of dataset
    """
    os.makedirs(DATASTORE_DIR / f"data/{dataset_name}", exist_ok=True)


def get_data_versions(version: str | None) -> dict:
    """
    Returns the data_versions json file for the given version as a dict.
    Returns an empty dictionary if given version is None.

    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
    """
    if version is None:
        return {}
    file_version = "_".join(version.split("_")[:-1])
    return _read_json(
        DATASTORE_DIR / f"datastore/data_versions__{file_version}.json"
    )


def write_data_versions(data_versions: dict, version: str) -> None:
    """
    Writes given dict to a new data versions json file to the appropriate
    datastore directory named with the given version.

    * data_versions: dict - data versions dict
    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
    """
    file_version = "_".join(version.split("_")[:-1])
    _write_json(
        data_versions,
        DATASTORE_DIR / f"datastore/data_versions__{file_version}.json",
        indent=2,
    )


def get_draft_version() -> dict:
    """
    Returns the contents of the draft version json file as dict.
    """
    return _read_json(DRAFT_VERSION_PATH)


def write_draft_version(draft_version: dict) -> None:
    """
    Writes given dict to the draft version json file.

    * draft_version: dict - draft version dict
    """
    _write_json(draft_version, DRAFT_VERSION_PATH, indent=2)


def get_datastore_versions() -> dict:
    """
    Returns the contents of the datastore versions json file as dict.
    """
    return _read_json(DATASTORE_VERSIONS_PATH)


def write_datastore_versions(datastore_versions: dict) -> None:
    """
    Writes given dict to the datastore versions json file.

    * datastore_versions: dict - datastore_versions dict
    """
    _write_json(
        datastore_versions,
        DATASTORE_VERSIONS_PATH,
        indent=2,
    )


def get_metadata_all(version: str) -> dict:
    """
    Returns the metadata all json file for the given version as a dict.

    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
    return _read_json(DATASTORE_DIR / f"datastore/metadata_all__{version}.json")


def write_metadata_all(metadata_all: dict, version: str) -> None:
    """
    Writes given dict to a metadata all json file to the appropriate
    datastore directory named with the given version.
    If supplied version is 'DRAFT' a tmp file will be written to first
    to avoid downtime in consuming services due to incomplete json while
    writing.
    * metadata_all: dict - metadata all dict
    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
    file_path = DATASTORE_DIR / f"datastore/metadata_all__{version}.json"
    if version == "DRAFT":
        _write_json_with_tmp(metadata_all, file_path)
    else:
        _write_json(metadata_all, file_path)


def write_working_dir_metadata(dataset_name: str, metadata: dict) -> None:
    """
    Writes a json to a the working directory as the processed metadata file
    named: {dataset_name}__DRAFT.json

    * dataset_name: str - name of dataset
    * metadata: dict - dictionary to write as json
    """
    _write_json(metadata, WORKING_DIR / f"{dataset_name}__DRAFT.json")


def get_working_dir_metadata(dataset_name: str) -> dict:
    """
    Returns the working dir metadata json file for given dataset_name.

    * dataset_name: str - name of dataset
    """
    return _read_json(WORKING_DIR / f"{dataset_name}__DRAFT.json")


def delete_working_dir_metadata(dataset_name: str) -> None:
    """
    Deletes the metadata in working directory with postfix __DRAFT.json

    * dataset_name: str - name of dataset
    """
    metadata_path = WORKING_DIR / f"{dataset_name}__DRAFT.json"
    if os.path.isfile(metadata_path):
        os.remove(metadata_path)


def get_working_dir_input_metadata(dataset_name: str) -> dict:
    """
    Returns the working dir metadata json file for given dataset_name.

    * dataset_name: str - name of dataset
    """
    return _read_json(WORKING_DIR / f"{dataset_name}.json")


def rename_parquet_draft_to_release(dataset_name: str, version: str) -> str:
    """
    Renames the parquet DRAFT file or directory for the given dataset_name,
    with the given version.

    * dataset_name: str - name of dataset
    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
    """
    draft_path = _get_datastore_draft_parquet_path(dataset_name)
    file_version = "_".join(version.split("_")[:-1])
    file_name = (
        draft_path.stem.replace("DRAFT", file_version) + draft_path.suffix
    )
    release_path = draft_path.parent / file_name
    shutil.move(draft_path, release_path)
    return release_path.name


def move_working_dir_parquet_to_datastore(dataset_name: str) -> None:
    """
    Moves the given parquet DRAFT file from the working directory to
    the appropriate datastore sub directory.

    * dataset_name: str - name of dataset
    """
    working_dir_parquet_path = _get_working_dir_draft_parquet_path(dataset_name)
    shutil.move(
        working_dir_parquet_path,
        (
            DATASTORE_DIR / f"data/{dataset_name}/"
            f"{working_dir_parquet_path.parts[-1]}"
        ),
    )


def delete_parquet_draft(dataset_name: str) -> None:
    """
    Deletes the parquet draft file or directory for the given dataset_name.

    * dataset_name: str - name of dataset
    """
    data_dir = DATASTORE_DIR / f"data/{dataset_name}"
    partitioned_parquet_path = data_dir / f"{dataset_name}__DRAFT"
    parquet_file_path = data_dir / f"{dataset_name}__DRAFT.parquet"
    if partitioned_parquet_path.is_dir():
        shutil.rmtree(partitioned_parquet_path)
    elif parquet_file_path.is_file():
        os.remove(parquet_file_path)


def delete_working_dir_file(file_path: Path) -> None:
    """
    Deletes a file from the working directory.
    Intended to clean up left-over files.
    Raises a LocalStorageError if filepath is not in
    the working directory.

    * file_name: str - name of temporary file
    """
    if not str(file_path).startswith(str(WORKING_DIR)):
        raise LocalStorageError(f"Filepath {file_path} is not in {WORKING_DIR}")
    if file_path.is_file():
        os.remove(file_path)


def delete_working_dir_dir(dir_path: Path) -> None:
    """
    Deletes a directory from the working directory.
    Intended to clean up left-over directories.
    Raises a LocalStorageError if dirpath is not in
    the working directory.

    * dir_path: str - name of temporary directory
    """
    if not str(dir_path).startswith(str(WORKING_DIR)):
        raise LocalStorageError(f"Dirpath {dir_path} is not in {WORKING_DIR}")
    if dir_path.is_dir():
        shutil.rmtree(dir_path)


def save_temporary_backup() -> None:
    """
    Backs up metadata_all__DRAFT.json, datastore_versions.json and
    draft_version.json from the datastore to a tmp directory
    inside the datastore directory.
    Raises `LocalStorageError` if tmp directory already exists.
    """
    with open(
        f"{DATASTORE_DIR}/datastore/datastore_versions.json", encoding="utf-8"
    ) as f:
        datastore_versions = json.load(f)
    with open(DRAFT_VERSION_PATH, encoding="utf-8") as f:
        draft_version = json.load(f)
    with open(DRAFT_METADATA_ALL_PATH, encoding="utf-8") as f:
        metadata_all_draft = json.load(f)
    tmp_dir = Path(DATASTORE_DIR) / "tmp"
    if os.path.isdir(tmp_dir):
        raise LocalStorageError("tmp directory already exists")
    os.mkdir(tmp_dir)
    _write_json(draft_version, tmp_dir / "draft_version.json", indent=2)
    _write_json(metadata_all_draft, tmp_dir / "metadata_all__DRAFT.json")
    _write_json(
        datastore_versions, tmp_dir / "datastore_versions.json", indent=2
    )


def restore_from_temporary_backup() -> str | None:
    """
    Restores the datastore from the tmp directory.
    Raises `LocalStorageError`if there are any missing backup files.

    Returns None if no released version in backup, else returns the
    latest release version number as dotted four part version.
    """
    tmp_dir = Path(DATASTORE_DIR) / "tmp"
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
        datastore_versions = _read_json(datastore_versions_backup)
        shutil.move(draft_version_backup, DRAFT_VERSION_PATH)
        shutil.move(metadata_all_draft_backup, DRAFT_METADATA_ALL_PATH)
        shutil.move(datastore_versions_backup, DATASTORE_VERSIONS_PATH)
        if datastore_versions["versions"] == []:
            return None
        else:
            return datastore_versions["versions"][0]["version"]
    except ValidationError as e:
        raise LocalStorageError("Invalid backup file") from e


def archive_temporary_backup() -> None:
    """
    Archives the tmp directory within the datastore if the directory
    exists. Raises `LocalStorageError` if there are any unrecognized files
    in the directory.
    """
    tmp_dir = Path(DATASTORE_DIR) / "tmp"
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    if not os.path.isdir(Path(DATASTORE_DIR) / "tmp"):
        raise LocalStorageError("Could not find a tmp directory to archive.")
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
    shutil.move(DATASTORE_DIR / "tmp", ARCHIVE_DIR / f"tmp_{timestamp}")


def delete_temporary_backup() -> None:
    """
    Deletes the tmp directory within the datastore if the directory
    exists. Raises `LocalStorageError` if there are any unrecognized files
    in the directory.
    """
    tmp_dir = Path(DATASTORE_DIR) / "tmp"
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    if not os.path.isdir(Path(DATASTORE_DIR) / "tmp"):
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
    shutil.rmtree(DATASTORE_DIR / "tmp")


def temporary_backup_exists() -> bool:
    """
    Returns a boolean representing if the tmp directory exists.
    """
    tmp_dir = Path(DATASTORE_DIR) / "tmp"
    return os.path.isdir(tmp_dir)


def archive_draft_version(version: str) -> None:
    """
    Archives the current draft json
    * dataset_name: str - name of dataset draft
    * version: str - version of the archived draft
    """
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    timestamp = datetime.now(UTC).replace(tzinfo=None)

    archived_draft_version_path = (
        ARCHIVE_DIR / f"draft_version_{version}_{timestamp}.json"
    )
    shutil.copyfile(DRAFT_VERSION_PATH, archived_draft_version_path)


def archive_input_files(dataset_name: str) -> None:
    """
    Archives the input .tar files if not already archived
    """
    archive_dir = INPUT_DIR / "archive"
    tar_filename = f"{dataset_name}.tar"
    archived_tar_file = archive_dir / tar_filename
    tar_file = INPUT_DIR / tar_filename
    if not archive_dir.exists():
        os.makedirs(archive_dir, exist_ok=True)
    if tar_file.exists() and not os.path.isfile(archived_tar_file):
        shutil.move(str(tar_file), str(archive_dir))


def delete_archived_input(dataset_name: str) -> None:
    """
    Delete the archived .tar file from the archive directory.
    """
    archived_file: Path = INPUT_DIR / f"archive/{dataset_name}.tar"
    if archived_file.is_file():
        os.remove(archived_file)


def get_input_tar_size_in_bytes(dataset_name: str) -> int:
    """
    Checks the size in bytes of the dataset.tar file.
    Returns size in bytes or 0 if the file does not exist.
    """
    tar_path = INPUT_DIR / f"{dataset_name}.tar"

    if not tar_path.exists():
        tar_path = INPUT_DIR / "archive" / f"{dataset_name}.tar"

    if tar_path.exists():
        return os.path.getsize(tar_path)
    return 0
