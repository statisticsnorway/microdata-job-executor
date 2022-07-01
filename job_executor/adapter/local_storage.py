import json
import os
import shutil
from job_executor.config import environment


WORKING_DIR = environment.get('WORKING_DIR')
DATASTORE_DIR = environment.get('DATASTORE_DIR')

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


def get_data_versions(version: str) -> dict:
    """
    Returns the data_versions json file for the given version as a dict.

    * version: str - '<MAJOR>_<MINOR>_<PATCH>' formatted semantic version
                     or 'DRAFT'
    """
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
