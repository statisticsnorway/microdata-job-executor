import json
import os

from job_executor.model import (
    DataStructureUpdate,
    DatastoreVersion,
    DraftVersion,
    DatastoreVersions
)


def load_json(file_path):
    return json.load(open(file_path, encoding='utf'))


DATA_STRUCTURE_UPDATE = {
    "name": "KJOENN",
    "description": "Første publisering",
    "operation": "ADD",
    "releaseStatus": "RELEASED"
}
DATASTORE_VERSION = {
    "version": "0.1.0.0",
    "description": "Første release",
    "releaseTime": 1635299291,
    "languageCode": "no",
    "dataStructureUpdates": [
        {
            "name": "INNTEKT",
            "description": "Første publisering",
            "operation": "ADD",
            "releaseStatus": "RELEASED"
        },
        {
            "name": "SIVSTAND",
            "description": "Første publisering",
            "operation": "ADD",
            "releaseStatus": "RELEASED"
        }
    ],
    "updateType": "MINOR"
}
DATASTORE_DIR = f'{os.environ["DATASTORE_DIR"]}/datastore'
DRAFT_VERSION_PATH = f'{DATASTORE_DIR}/draft_version.json'
DATASTORE_VERSIONS_PATH = f'{DATASTORE_DIR}/datastore_versions.json'


def test_data_structure_update():
    data_structure_update = DataStructureUpdate(**DATA_STRUCTURE_UPDATE)
    assert data_structure_update.dict() == DATA_STRUCTURE_UPDATE


def test_datastore_version():
    datastore_version = DatastoreVersion(**DATASTORE_VERSION)
    assert datastore_version.dict() == DATASTORE_VERSION


def test_draft_version():
    draft_version = DraftVersion()
    assert draft_version.dict() == load_json(DRAFT_VERSION_PATH)


def test_datastore_versions():
    datastore_versions = DatastoreVersions()
    assert datastore_versions.dict() == load_json(DATASTORE_VERSIONS_PATH)
