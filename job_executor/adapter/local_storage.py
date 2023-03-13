import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Union

from pydantic import ValidationError

from job_executor.config import environment
from job_executor.exception import LocalStorageError


WORKING_DIR = Path(environment.get('WORKING_DIR'))
DATASTORE_DIR = Path(environment.get('DATASTORE_DIR'))
INPUT_DIR = Path(environment.get('INPUT_DIR'))

DATASTORE_VERSIONS_PATH = DATASTORE_DIR / 'datastore/datastore_versions.json'
DRAFT_METADATA_ALL_PATH = DATASTORE_DIR / 'datastore/metadata_all__DRAFT.json'
DRAFT_VERSION_PATH = DATASTORE_DIR / 'datastore/draft_version.json'


def _read_json(file_path: Path) -> dict:
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _write_json(
    content: dict,
    file_path: Path,
    indent: int = None
) -> None:
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(content, f, indent=indent)


def _get_parquet_path(directory: Path, dataset_name: str) -> str:
    parquet_file_path = directory / f'{dataset_name}__DRAFT.parquet'
    partitioned_parquet_path = directory / f'{dataset_name}__DRAFT'
    if partitioned_parquet_path.is_dir():
        return partitioned_parquet_path
    elif parquet_file_path.is_file():
        return parquet_file_path
    else:
        raise FileExistsError(
            f'Invalid parquet path in {directory} for {dataset_name}'
        )


def _get_datastore_draft_parquet_path(dataset_name: str) -> Path:
    return _get_parquet_path(
        DATASTORE_DIR / f'data/{dataset_name}', dataset_name
    )


def _get_working_dir_draft_parquet_path(dataset_name: str):
    return _get_parquet_path(WORKING_DIR, dataset_name)


def make_dataset_dir(dataset_name: str) -> None:
    """
    Creates sub-directories for dataset_name in the datastore /data and
    /metadata directories.

    * dataset_name: str - name of dataset
    """
    os.makedirs(DATASTORE_DIR / f'metadata/{dataset_name}', exist_ok=True)
    os.makedirs(DATASTORE_DIR / f'data/{dataset_name}', exist_ok=True)


def get_data_versions(version: Union[str, None]) -> dict:
    """
    Returns the data_versions json file for the given version as a dict.
    Returns an empty dictionary if given version is None.

    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
    """
    if version is None:
        return {}
    file_version = '_'.join(version.split('_')[:-1])
    return _read_json(
        DATASTORE_DIR / f'datastore/data_versions__{file_version}.json'
    )


def write_data_versions(data_versions: dict, version: str):
    """
    Writes given dict to a new data versions json file to the appropriate
    datastore directory named with the given version.

    * data_versions: dict - data versions dict
    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
    """
    file_version = '_'.join(version.split('_')[:-1])
    _write_json(
        data_versions,
        DATASTORE_DIR / f'datastore/data_versions__{file_version}.json',
        indent=2
    )


def get_draft_version() -> dict:
    """
    Returns the contents of the draft version json file as dict.
    """
    return _read_json(DATASTORE_DIR / 'datastore/draft_version.json')


def write_draft_version(draft_version: dict) -> None:
    """
    Writes given dict to the draft version json file.

    * draft_version: dict - draft version dict
    """
    _write_json(
        draft_version,
        DATASTORE_DIR / 'datastore/draft_version.json',
        indent=2
    )


def get_datastore_versions() -> dict:
    """
    Returns the contents of the datastore versions json file as dict.
    """
    return _read_json(DATASTORE_DIR / 'datastore/datastore_versions.json')


def write_datastore_versions(datastore_versions: dict) -> None:
    """
    Writes given dict to the datastore versions json file.

    * datastore_versions: dict - datastore_versions dict
    """
    _write_json(
        datastore_versions,
        DATASTORE_DIR / 'datastore/datastore_versions.json',
        indent=2
    )


def get_metadata_all(version: str) -> dict:
    """
    Returns the metadata all json file for the given version as a dict.

    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
    return _read_json(
        DATASTORE_DIR / f'datastore/metadata_all__{version}.json'
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
        DATASTORE_DIR / f'datastore/metadata_all__{version}.json'
    )


def get_working_dir_metadata(dataset_name: str) -> dict:
    """
    Returns the working dir metadata json file for given dataset_name.

    * dataset_name: str - name of dataset
    """
    return _read_json(WORKING_DIR / f'{dataset_name}__DRAFT.json')


def delete_working_dir_metadata(dataset_name: str) -> None:
    """
    Deletes the metadata in working directory with postfix __DRAFT.json

    * dataset_name: str - name of dataset
    """
    metadata_path = WORKING_DIR / f'{dataset_name}__DRAFT.json'
    if os.path.isfile(metadata_path):
        os.remove(metadata_path)


def get_working_dir_input_metadata(dataset_name: str) -> dict:
    """
    Returns the working dir metadata json file for given dataset_name.

    * dataset_name: str - name of dataset
    """
    return _read_json(WORKING_DIR / f'{dataset_name}.json')


def get_metadata(dataset_name: str, version: str) -> dict:
    """
    Returns the datastore metadata json file for the given version as a dict.

    * dataset_name: str - name of dataset
    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
    return _read_json(
        DATASTORE_DIR / (
            f'metadata/{dataset_name}/{dataset_name}__{version}.json'
        )
    )


def write_metadata(metadata: dict, dataset_name: str, version: str):
    """
    Writes given dict to a metadata json file to the appropriate
    datastore directory named with the given version.

    * metadata: dict - metadata dict
    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
    os.makedirs(DATASTORE_DIR / f'metadata/{dataset_name}', exist_ok=True)
    _write_json(
        metadata,
        (
            DATASTORE_DIR /
            f'metadata/{dataset_name}/{dataset_name}__{version}.json'
        )
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
    """
    draft_path = _get_datastore_draft_parquet_path(dataset_name)
    file_version = '_'.join(version.split('_')[:-1])
    file_name = (
        draft_path.stem.replace('DRAFT', file_version) + draft_path.suffix
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
    working_dir_parquet_path = _get_working_dir_draft_parquet_path(
        dataset_name
    )
    shutil.move(
        working_dir_parquet_path,
        (
            DATASTORE_DIR /
            f'data/{dataset_name}/'
            f'{working_dir_parquet_path.parts[-1]}'
        )
    )


def delete_parquet_draft(dataset_name: str) -> None:
    """
    Deletes the parquet draft file or directory for the given dataset_name.

    * dataset_name: str - name of dataset
    """
    data_dir = DATASTORE_DIR / f'data/{dataset_name}'
    partitioned_parquet_path = data_dir / f'{dataset_name}__DRAFT'
    parquet_file_path = data_dir / f'{dataset_name}__DRAFT.parquet'
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
        raise LocalStorageError(f'Filepath {file_path} is not in {WORKING_DIR}')
    if file_path.is_file():
        os.remove(file_path)


def save_temporary_backup() -> None:
    """
    Backs up metadata_all__DRAFT.json, datastore_versions.json and
    draft_version.json from the datastore to a tmp directory
    inside the datastore directory.
    Raises `LocalStorageError` if tmp directory already exists.
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
        raise LocalStorageError('tmp directory already exists')
    os.mkdir(tmp_dir)
    _write_json(
        draft_version, tmp_dir / 'draft_version.json', indent=2
    )
    _write_json(
        metadata_all_draft, tmp_dir / 'metadata_all__DRAFT.json'
    )
    _write_json(
        datastore_versions, tmp_dir / 'datastore_versions.json', indent=2
    )


def restore_from_temporary_backup() -> Union[str, None, LocalStorageError]:
    """
    Restores the datastore from the tmp directory.
    Raises `LocalStorageError`if there are any missing backup files.

    Returns None if no released version in backup, else returns the
    latest release version number as dotted four part version.
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
        raise LocalStorageError('Missing tmp backup files')
    try:
        datastore_versions = _read_json(datastore_versions_backup)
        shutil.move(draft_version_backup, DRAFT_VERSION_PATH)
        shutil.move(metadata_all_draft_backup, DRAFT_METADATA_ALL_PATH)
        shutil.move(datastore_versions_backup, DATASTORE_VERSIONS_PATH)
        if datastore_versions['versions'] == []:
            return None
        else:
            return datastore_versions['versions'][0]['version']
    except ValidationError as e:
        raise LocalStorageError('Invalid backup file') from e


def delete_temporary_backup() -> Union[None, LocalStorageError]:
    """
    Deletes the tmp directory within the datastore if the directory
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
                'Found unrecognized files and/or directories in the tmp '
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
    os.makedirs(archive_dir, exist_ok=True)

    timestamp = datetime.now()

    archived_draft_version_path = (
        archive_dir / f'draft_version_{version}_{timestamp}.json'
    )
    shutil.copyfile(DRAFT_VERSION_PATH, archived_draft_version_path)


def archive_input_files(dataset_name: str):
    """
    Archives the input folder files
    """
    archive_dir = Path(f'{INPUT_DIR}/archive/{dataset_name}')
    move_dir = f'{INPUT_DIR}/{dataset_name}'
    os.makedirs(archive_dir, exist_ok=True)
    shutil.copytree(
        move_dir, archive_dir, dirs_exist_ok=True
    )
    if os.path.isdir(move_dir):
        shutil.rmtree(move_dir)


def move_archived_to_input(dataset_name: str):
    """
    Move the archived dataset to input directory.
    Throws FileExistsError if the dataset exists in input directory.
    """
    archive_dir: Path = INPUT_DIR / f'archive/{dataset_name}'
    input_dir = INPUT_DIR / f'{dataset_name}'
    shutil.copytree(
        archive_dir, input_dir, dirs_exist_ok=False
    )
    if archive_dir.is_dir():
        shutil.rmtree(archive_dir)


def delete_archived_input(dataset_name: str):
    """
    Delete the archived dataset from archive directory.
    """
    archive_dir: Path = INPUT_DIR / f'archive/{dataset_name}'
    if archive_dir.is_dir():
        shutil.rmtree(archive_dir)
