import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Union

from pydantic import ValidationError

from job_executor.config import environment
from job_executor.exception import LocalStorageError
from job_executor.model import DraftVersion, MetadataAll, DatastoreVersions


WORKING_DIR = environment.get('WORKING_DIR')
DATASTORE_DIR = environment.get('DATASTORE_DIR')
INPUT_DIR = environment.get('INPUT_DIR')

DATASTORE_VERSIONS_PATH = f'{DATASTORE_DIR}/datastore/datastore_versions.json'
DRAFT_METADATA_ALL_PATH = f'{DATASTORE_DIR}/datastore/metadata_all__draft.json'
DRAFT_VERSION_PATH = f'{DATASTORE_DIR}/datastore/draft_version.json'


def _read_json(file_path: str) -> dict:
    with open(file_path, encoding='utf-8') as f:
        return json.load(f)


def _write_json(content: dict, file_path: str) -> None:
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(content, f)


def _get_parquet_path(directory: str, dataset_name: str) -> str:
    parquet_file_path = f'{directory}/{dataset_name}__DRAFT.parquet'
    partitioned_parquet_path = f'{directory}/{dataset_name}__DRAFT'
    if os.path.isdir(partitioned_parquet_path):
        return partitioned_parquet_path
    elif os.path.isfile(parquet_file_path):
        return parquet_file_path
    else:
        raise FileExistsError(
            f'Invalid parquet path in {directory} for {dataset_name}'
        )


def _get_datastore_draft_parquet_path(dataset_name: str):
    return _get_parquet_path(
        f'{DATASTORE_DIR}/data/{dataset_name}',
        dataset_name
    )


def _get_working_dir_draft_parquet_path(dataset_name: str):
    return _get_parquet_path(
        f'{WORKING_DIR}',
        dataset_name
    )


def make_dataset_dir(dataset_name: str) -> None:
    """
    Creates sub-directories for dataset_name in the datastore /data and
    /metadata directories.

    * dataset_name: str - name of dataset
    """
    os.makedirs(f'{DATASTORE_DIR}/metadata/{dataset_name}')
    os.makedirs(f'{DATASTORE_DIR}/data/{dataset_name}')


def get_data_versions(version: Union[str, None]) -> dict:
    """
    Returns the data_versions json file for the given version as a dict.
    Returns an empty dictionary if given version is None.

    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
    if version is None:
        return {}
    return _read_json(
        f'{DATASTORE_DIR}/datastore/data_versions__{version[:3]}.json'
    )


def write_data_versions(data_versions: dict, version: str):
    """
    Writes given dict to a new data versions json file to the appropriate
    datastore directory named with the given version.

    * data_versions: dict - data versions dict
    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
    _write_json(
        data_versions,
        f'{DATASTORE_DIR}/datastore/data_versions__{version[:3]}.json'
    )


def get_draft_version() -> dict:
    """
    Returns the contents of the draft version json file as dict.
    """
    return _read_json(f'{DATASTORE_DIR}/datastore/draft_version.json')


def write_draft_version(draft_version: dict) -> dict:
    """
    Writes given dict to the draft version json file.

    * draft_version: dict - draft version dict
    """
    _write_json(
        draft_version,
        f'{DATASTORE_DIR}/datastore/draft_version.json'
    )


def get_datastore_versions() -> dict:
    """
    Returns the contents of the datastore versions json file as dict.
    """
    return _read_json(
        f'{DATASTORE_DIR}/datastore/datastore_versions.json'
    )


def write_datastore_versions(datastore_versions: dict) -> dict:
    """
    Writes given dict to the datastore versions json file.

    * datastore_versions: dict - datastore_versions dict
    """
    _write_json(
        datastore_versions,
        f'{DATASTORE_DIR}/datastore/datastore_versions.json'
    )


def get_metadata_all(version: str) -> dict:
    """
    Returns the metadata all json file for the given version as a dict.

    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
    return _read_json(
        f'{DATASTORE_DIR}/datastore/metadata_all__{version}.json'
    )


def write_metadata_all(metadata_all: dict, version: str):
    """
    Writes given dict to a metadata all json file to the appropriate
    datastore directory named with the given version.

    * metadata_all: dict - metadata all dict
    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
    _write_json(
        metadata_all,
        f'{DATASTORE_DIR}/datastore/metadata_all__{version}.json'
    )


def get_working_dir_metadata(dataset_name: str) -> dict:
    """
    Returns the working dir metadata json file for given dataset_name.

    * dataset_name: str - name of dataset
    """
    return _read_json(f'{WORKING_DIR}/{dataset_name}__DRAFT.json')


def get_working_dir_input_metadata(dataset_name: str) -> dict:
    """
    Returns the working dir metadata json file for given dataset_name.

    * dataset_name: str - name of dataset
    """
    return _read_json(f'{WORKING_DIR}/{dataset_name}.json')


def get_metadata(dataset_name: str, version: str) -> dict:
    """
    Returns the datastore metadata json file for the given version as a dict.

    * dataset_name: str - name of dataset
    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
    return _read_json(
        f'{DATASTORE_DIR}/metadata/{dataset_name}/'
        f'{dataset_name}__{version}.json'
    )


def write_metadata(metadata: dict, dataset_name: str, version: str):
    """
    Writes given dict to a metadata json file to the appropriate
    datastore directory named with the given version.

    * metadata: dict - metadata dict
    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
    _write_json(
        metadata,
        f'{DATASTORE_DIR}/metadata/{dataset_name}/'
        f'{dataset_name}__{version}.json'
    )


def delete_metadata_draft(dataset_name: str) -> None:
    """
    Deletes the metadata draft file for the given dataset_name.

    * dataset_name: str - name of dataset
    """
    metadata_path = (
        f'{DATASTORE_DIR}/metadata/{dataset_name}/{dataset_name}__DRAFT.json'
    )
    if os.path.isfile(metadata_path):
        os.remove(metadata_path)


def rename_metadata_draft_to_release(dataset_name: str, version: str) -> dict:
    """
    Renames the metadata DRAFT file for the given dataset_name, with the
    given version.

    * dataset_name: str - name of dataset
    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
    metadata_draft_path = (
        f'{DATASTORE_DIR}/metadata/{dataset_name}/'
        f'{dataset_name}__DRAFT.json'
    )
    metadata_release_path = metadata_draft_path.replace(
        'DRAFT', version
    )
    shutil.move(metadata_draft_path, metadata_release_path)
    with open(metadata_release_path, encoding='utf-8') as f:
        return json.load(f)


def rename_parquet_draft_to_release(dataset_name: str, version: str) -> str:
    """
    Renames the parquet DRAFT file or directory for the given dataset_name,
    with the given version.

    * dataset_name: str - name of dataset
    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
    draft_path = _get_datastore_draft_parquet_path(dataset_name)
    release_path = draft_path.replace(
        'DRAFT', version[:3]
    )
    shutil.move(draft_path, release_path)
    return release_path.split('/')[-1]


def move_working_dir_parquet_to_datastore(dataset_name) -> None:
    """
    Moves the given parquet DRAFT file from the working directory to
    the appropriate datastore sub directory.

    * dataset_name: str - name of dataset
    """
    working_dir_parquet_path = _get_working_dir_draft_parquet_path(
        dataset_name
    )
    shutil.move(
        working_dir_parquet_path,
        (
            f'{DATASTORE_DIR}/data/{dataset_name}/'
            f'{working_dir_parquet_path.split("/")[-1]}'
        )
    )


def delete_parquet_draft(dataset_name: str) -> None:
    """
    Deletes the parquet draft file or directory for the given dataset_name.

    * dataset_name: str - name of dataset
    """
    parquet_path = (
        f'{DATASTORE_DIR}/data/{dataset_name}/{dataset_name}__DRAFT'
    )
    if os.path.isdir(parquet_path):
        shutil.rmtree(parquet_path)
    elif os.path.isfile(f'{parquet_path}.parquet'):
        os.remove(f'{parquet_path}.parquet')


def delete_files(file_list: list[str]):
    """
    Deletes a list of files. Intended to clean up left-over csv files
    * file_list: list[str] - list of csv files to delete
    """
    for file in file_list:
        if os.path.isfile(file):
            os.remove(file)


def save_temporary_backup() -> Union[None, LocalStorageError]:
    """
    Backs up metadata_all__DRAFT.json, datastore_versions.json and
    draft_version.json from the datastore to a /tmp directory
    inside the datastore directory.
    Raises `LocalStorageError` if /tmp directory already exists.
    """
    with open(
        f'{DATASTORE_DIR}/datastore/datastore_versions.json',
        encoding='utf-8'
    ) as f:
        datastore_versions = json.load(f)
    with open(
        f'{DATASTORE_DIR}/datastore/draft_version.json',
        encoding='utf-8'
    ) as f:
        draft_version = json.load(f)
    with open(
        f'{DATASTORE_DIR}/datastore/metadata_all__DRAFT.json',
        encoding='utf-8'
    ) as f:
        metadata_all_draft = json.load(f)
    tmp_dir = Path(DATASTORE_DIR) / 'tmp'
    if os.path.isdir(tmp_dir):
        raise LocalStorageError('/tmp directory already exists')
    os.mkdir(tmp_dir)
    with open(tmp_dir / 'draft_version.json', 'w', encoding='utf-8') as f:
        json.dump(draft_version, f)
    with open(
        tmp_dir / 'metadata_all__DRAFT.json', 'w', encoding='utf-8'
    ) as f:
        json.dump(metadata_all_draft, f)
    with open(tmp_dir / 'datastore_versions.json', 'w', encoding='utf-8') as f:
        json.dump(datastore_versions, f)


def restore_from_temporary_backup() -> Union[str, LocalStorageError]:
    """
    Restores the datastore from the /tmp directory.
    Returns version number of restored datastore.
    Raises `LocalStorageError`if there are any missing backup files.
    """
    tmp_dir = Path(DATASTORE_DIR) / 'tmp'
    draft_version_backup = tmp_dir / 'draft_version.json'
    metadata_all_draft_backup = tmp_dir / 'metadata_all__DRAFT.json'
    datastore_versions_backup = tmp_dir / 'datastore_versions.json'
    backup_exists = (
        os.path.isdir(tmp_dir) and
        os.path.isfile(draft_version_backup) and
        os.path.isfile(metadata_all_draft_backup) and
        os.path.isfile(datastore_versions_backup)
    )
    if not backup_exists:
        raise LocalStorageError('Missing /tmp backup files')
    try:
        DraftVersion(_read_json(draft_version_backup))
        MetadataAll(_read_json(metadata_all_draft_backup))
        datastore_versions = DatastoreVersions(
            _read_json(datastore_versions_backup)
        )
        shutil.move(draft_version_backup, DRAFT_VERSION_PATH)
        shutil.move(metadata_all_draft_backup, DRAFT_METADATA_ALL_PATH)
        shutil.move(datastore_versions_backup, DATASTORE_VERSIONS_PATH)
        return datastore_versions.get_latest_version_number()
    except ValidationError as e:
        raise LocalStorageError('Invalid backup file') from e


def delete_temporary_backup() -> Union[None, LocalStorageError]:
    """
    Deletes the /tmp directory within the datastore if the directory
    exists. Raises `LocalStorageError` if there are any unrecognized files
    in the directory.
    """
    tmp_dir = Path(DATASTORE_DIR) / 'tmp'
    if not os.path.isdir(Path(DATASTORE_DIR) / 'tmp'):
        return None
    for content in os.listdir(tmp_dir):
        if content not in [
            'datastore_versions.json',
            'metadata_all__DRAFT.json',
            'draft_version.json'
        ]:
            raise LocalStorageError(
                'Found unrecognized files and/or directories in the /tmp '
                'directory. Aborting tmp deletion.'
            )
    shutil.rmtree(Path(DATASTORE_DIR) / 'tmp')


def archive_draft_version(version: str):
    """
    Archives the current draft json
    * dataset_name: str - name of dataset draft
    * version: str - version of the archived draft
    """

    archive_dir = Path(f'{DATASTORE_DIR}/archive')

    if not archive_dir.exists():
        os.makedirs(archive_dir, exist_ok=False)

    timestamp = datetime.now()

    archived_draft_version_path = (
        archive_dir / f'draft_version_{version}_{timestamp}.json'
    )

    if archive_dir.exists():
        shutil.copyfile(DRAFT_VERSION_PATH, archived_draft_version_path)


def archive_input_files(dataset_name: str):
    """
    Archives the input folder files
    """

    archive_dir = Path(f'{INPUT_DIR}/archive/{dataset_name}')
    os.makedirs(archive_dir, exist_ok=True)
    shutil.copytree(
        f'{INPUT_DIR}/{dataset_name}', archive_dir, dirs_exist_ok=True
    )
