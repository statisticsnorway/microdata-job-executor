import os
import json
import shutil

from job_executor.config import environment
from job_executor.exception.exception import VersioningException
from job_executor.model.metadata import generate_new_metadata_all
from job_executor.model import (
    DataStructureUpdate,
    DatastoreVersion,
    DraftVersion,
    DatastoreVersions,
    MetadataAll,
    Metadata
)

WORKING_DIR = environment.get('WORKING_DIR')
DATASTORE_DIR = environment.get('DATASTORE_DIR')

_datastore_versions = DatastoreVersions(
    file_path=f'{DATASTORE_DIR}/datastore/datastore_versions.json'
)
_draft_version = DraftVersion(
    file_path=f'{DATASTORE_DIR}/datastore/draft_version.json'
)
_draft_metadata_all = MetadataAll(
    file_path=f'{DATASTORE_DIR}/datastore/metadata_all__DRAFT.json'
)
_latest_metadata_all = MetadataAll(
    file_path=f'{DATASTORE_DIR}/datastore/metadata_all__'
    f'{_datastore_versions.get_latest_version_number()}.json'
)


def _get_release_status(dataset_name: str):
    release_status = _draft_version.get_dataset_release_status(dataset_name)
    if release_status is None:
        return _datastore_versions.get_dataset_release_status(dataset_name)
    else:
        return release_status


def _get_latest_data_versions() -> dict:
    with open(
        f'{DATASTORE_DIR}/datastore/data_versions__'
        f'{_datastore_versions.get_latest_version_number()[:3]}.json',
        encoding='utf-8'
    ) as f:
        return json.load(f)


def _get_parquet_draft_path(dataset_name: str) -> str:
    data_dir = f'{DATASTORE_DIR}/data/{dataset_name}'
    partitioned_draft_path = f'{data_dir}/{dataset_name}__DRAFT'
    draft_path = f'{partitioned_draft_path}.parquet'
    if os.path.isfile(draft_path):
        return draft_path
    elif os.path.isdir(partitioned_draft_path):
        return partitioned_draft_path
    else:
        raise FileExistsError(f'No draft parquet path for {dataset_name}')


def _get_working_dir_parquet_path(dataset_name: str) -> str:
    parquet_path = f'{WORKING_DIR}/{dataset_name}__DRAFT'
    if os.path.isdir(parquet_path):
        return parquet_path
    elif os.path.isfile(f'{parquet_path}.parquet'):
        return f'{parquet_path}.parquet'
    else:
        raise FileExistsError(
            f'No working dir parquet path for {dataset_name}'
        )


def patch_metadata(dataset_name: str, description: str):
    """
    Patch metadata for a released dataset with updated metadata
    file.
    """
    dataset_release_status = _get_release_status(dataset_name)
    if dataset_release_status != 'RELEASED':
        raise VersioningException(
            'Can\'t patch metadata of dataset with status '
            f'{dataset_release_status}'
        )
    with open(
        f'{WORKING_DIR}/{dataset_name}__DRAFT.json', encoding='utf-8'
    ) as f:
        draft_metadata = Metadata(**json.load(f))
    released_metadata = _latest_metadata_all.get(dataset_name)
    patched_metadata = released_metadata.patch(draft_metadata)
    _draft_metadata_all.remove(dataset_name)
    _draft_metadata_all.add(patched_metadata)
    _draft_version.add(
        DataStructureUpdate(
            name=dataset_name,
            operation='PATCH_METADATA',
            description=description,
            releaseStatus='DRAFT'
        )
    )
    patched_metadata.write_to_file(
        f'{DATASTORE_DIR}/metadata/{dataset_name}/{dataset_name}__DRAFT.json'
    )


def add(dataset_name: str, description: str):
    """
    Import metadata and data as draft for a new dataset that
    has not been released in a previous versions.
    """
    dataset_release_status = _get_release_status(dataset_name)
    if dataset_release_status is not None:
        raise VersioningException(
            f'Can\'t add dataset with status {dataset_release_status}'
        )
    working_dir_parquet_path = _get_working_dir_parquet_path(dataset_name)
    with open(
        f'{WORKING_DIR}/{dataset_name}__DRAFT.json', encoding='utf-8'
    ) as f:
        draft_metadata = Metadata(**json.load(f))
    _draft_metadata_all.add(draft_metadata)
    _draft_version.add(
        DataStructureUpdate(
            name=dataset_name,
            operation='ADD',
            description=description,
            releaseStatus='DRAFT'
        )
    )
    os.makedirs(f'{DATASTORE_DIR}/metadata/{dataset_name}')
    draft_metadata.write_to_file(
        f'{DATASTORE_DIR}/metadata/{dataset_name}/{dataset_name}__DRAFT.json'
    )
    os.makedirs(f'{DATASTORE_DIR}/data/{dataset_name}')
    shutil.move(
        working_dir_parquet_path,
        (
            f'{DATASTORE_DIR}/data/{dataset_name}/'
            f'{working_dir_parquet_path.split("/")[-1]}'
        )
    )


def change_data(dataset_name: str, description: str):
    """
    Import metadata and data as draft for as an update
    for a dataset that has already been released in a
    previous version.
    """
    dataset_release_status = _get_release_status(dataset_name)
    if dataset_release_status != 'RELEASED':
        raise VersioningException(
            'Can\'t change data for dataset with release status'
            f'{dataset_release_status}'
        )
    working_dir_parquet_path = _get_working_dir_parquet_path(dataset_name)
    with open(
        f'{WORKING_DIR}/{dataset_name}__DRAFT.json', encoding='utf-8'
    ) as f:
        draft_metadata = Metadata(**json.load(f))
    _draft_metadata_all.remove(dataset_name)
    _draft_metadata_all.add(draft_metadata)
    _draft_version.add(
        DataStructureUpdate(
            name=dataset_name,
            operation='CHANGE_DATA',
            description=description,
            releaseStatus='DRAFT'
        )
    )
    draft_metadata.write_to_file(
        f'{DATASTORE_DIR}/metadata/{dataset_name}/{dataset_name}__DRAFT.json'
    )
    shutil.move(
        working_dir_parquet_path,
        (
            f'{DATASTORE_DIR}/data/{dataset_name}/'
            f'{working_dir_parquet_path.split("/")[-1]}'
        )
    )


def remove(dataset_name: str, description: str):
    """
    Remove a released dataset that has been released in
    a previous version from future versions of the datastore.
    """
    dataset_release_status = _get_release_status(dataset_name)
    if dataset_release_status != 'RELEASED':
        raise VersioningException(
            'Can\'t remove dataset with release status '
            f'{dataset_release_status}'
        )
    _draft_version.add(
        DataStructureUpdate(
            name=dataset_name,
            operation='REMOVE',
            description=description,
            releaseStatus='DRAFT'
        )
    )
    _draft_metadata_all.remove(dataset_name)


def delete_draft(dataset_name: str):
    """
    Delete a dataset from the draft version of the datastore.
    """
    deleted_draft = _draft_version.delete_draft(dataset_name)
    metadata_path = (
        f'{DATASTORE_DIR}/metadata/{dataset_name}/{dataset_name}__DRAFT.json'
    )
    if deleted_draft.operation == 'REMOVE':
        released_metadata = _latest_metadata_all.get(dataset_name)
        _draft_metadata_all.add(released_metadata)
    if deleted_draft.operation in ['ADD', 'CHANGE_DATA', 'PATCH_METADATA']:
        _draft_metadata_all.remove(dataset_name)
        if os.path.isfile(metadata_path):
            os.remove(metadata_path)
    if deleted_draft.operation in ['ADD', 'CHANGE_DATA']:
        parquet_path = (
            f'{DATASTORE_DIR}/data/{dataset_name}/{dataset_name}__DRAFT'
        )
        if os.path.isdir(parquet_path):
            shutil.rmtree(parquet_path)
        elif os.path.isfile(f'{parquet_path}.parquet'):
            os.remove(f'{parquet_path}.parquet')


def set_draft_release_status(dataset_name: str, new_status: str):
    """
    Set a new release status for a dataset in the draft version.
    """
    _draft_version.set_draft_release_status(dataset_name, new_status)


def bump_version(bump_manifesto: DatastoreVersion, description: str):
    """
    Release a new version of the datastore with the pending
    operations in the draft version of the datastore.
    """
    global _latest_metadata_all
    latest_data_versions = _get_latest_data_versions()
    if not _draft_version.validate_bump_manifesto(bump_manifesto):
        raise VersioningException(
            'Invalid Bump: Changes were made to the datastore '
            'after bump was requested'
        )
    release_updates, update_type = _draft_version.release_pending()
    new_version = _datastore_versions.add_new_release_version(
        release_updates, description, update_type
    )
    new_metadata_all = generate_new_metadata_all(
        f'{DATASTORE_DIR}/datastore/metadata_all__{new_version}.json',
        _latest_metadata_all
    )
    new_data_versions = {}
    for dataset_name, path in latest_data_versions.items():
        new_data_versions.update({dataset_name: path})

    for release_update in release_updates:
        operation = release_update.operation
        dataset_name = release_update.name
        if operation == 'REMOVE':
            new_metadata_all.remove(dataset_name)
            del new_data_versions[dataset_name]
        if operation in ['PATCH_METADATA', 'CHANGE_DATA', 'ADD']:
            metadata_draft_path = (
                f'{DATASTORE_DIR}/metadata/{dataset_name}/'
                f'{dataset_name}__DRAFT.json'
            )
            metadata_release_path = metadata_draft_path.replace(
                'DRAFT', new_version
            )
            shutil.move(metadata_draft_path, metadata_release_path)
            new_metadata_all.remove(dataset_name)
            with open(metadata_release_path, encoding='utf-8') as f:
                new_metadata_all.add(Metadata(**json.load(f)))
        if operation in ['ADD', 'CHANGE_DATA']:
            data_draft_path = _get_parquet_draft_path(dataset_name)
            data_release_path = data_draft_path.replace(
                'DRAFT', new_version[:3]
            )
            new_data_versions[dataset_name] = data_release_path.split('/')[-1]
            shutil.move(data_draft_path, data_release_path)
    if update_type in ['MINOR', 'MAJOR']:
        with open(
            f'{DATASTORE_DIR}/datastore/'
            f'data_versions__{new_version[:3]}.json',
            'w', encoding='utf-8'
        ) as f:
            json.dump(new_data_versions, f)
    _latest_metadata_all = new_metadata_all
    _draft_metadata_all.remove_all()
    for metadata in _latest_metadata_all:
        _draft_metadata_all.add(metadata)
    for draft in _draft_version:
        _draft_metadata_all.remove(draft.name)
        if draft.operation == 'REMOVE':
            continue
        else:
            with open(
                f'{DATASTORE_DIR}/metadata/'
                f'{draft.name}/{draft.name}__DRAFT.json',
                encoding='utf-8'
            ) as f:
                _draft_metadata_all.add(Metadata(**json.load(f)))
