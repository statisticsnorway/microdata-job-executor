import os
import json
import shutil

from job_executor.model import DatastoreVersions, DataStructureUpdate


def load_json(file_path):
    return json.load(open(file_path, encoding='utf'))


DATASTORE_DIR = f'{os.environ["DATASTORE_DIR"]}/datastore'
DATASTORE_VERSIONS_PATH = f'{DATASTORE_DIR}/datastore_versions.json'


def setup_function():

    if os.path.isdir('tests/resources_backup'):
        shutil.rmtree('tests/resources_backup')

    shutil.copytree(
        'tests/resources',
        'tests/resources_backup'
    )


def teardown_function():
    shutil.rmtree('tests/resources')
    shutil.move(
        'tests/resources_backup',
        'tests/resources'
    )


def test_datastore_versions():
    datastore_versions = DatastoreVersions()
    assert (
        datastore_versions.dict(by_alias=True)
        == load_json(DATASTORE_VERSIONS_PATH)
    )


def test_add_new_release_version():
    datastore_versions = DatastoreVersions()
    datastore_versions.add_new_release_version(
        [
            DataStructureUpdate(
                name='NEW_DATASET',
                description="Første publisering",
                operation='ADD',
                release_status='PENDING_RELEASE'
            )
        ],
        "new datastore version",
        "MAJOR"
    )
    assert len(datastore_versions.versions) == 3


def test_get_dataset_release_status():
    datastore_versions = DatastoreVersions()
    assert (
        datastore_versions.get_dataset_release_status('SIVSTAND') == 'RELEASED'
    )
    assert (
        datastore_versions.get_dataset_release_status('INNTEKT') == 'DELETED'
    )
    assert (
        datastore_versions.get_dataset_release_status('DOES_NOT_EXIST') is None
    )
