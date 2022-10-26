import os
import json
from pathlib import Path
import shutil

from job_executor.model import Datastore
from job_executor.model import DatastoreVersion


DATASTORE_DIR = Path(os.getenv('DATASTORE_DIR'))
UTDANNING_DATA_DIR = DATASTORE_DIR / 'data' / 'UTDANNING'
UTDANNING_METADATA_DIR = DATASTORE_DIR / 'metadata' / 'UTDANNING'
DATASTORE_INFO_DIR = DATASTORE_DIR / 'datastore'
DATASTORE_VERSIONS = DATASTORE_INFO_DIR / 'datastore_versions.json'
DRAFT_VERSION = DATASTORE_INFO_DIR / 'draft_version.json'
METADATA_ALL_DRAFT = DATASTORE_INFO_DIR / 'metadata_all__DRAFT.json'
METADATA_ALL_RELEASED = DATASTORE_INFO_DIR / 'metadata_all__1_0_0.json'
DATA_VERSIONS_RELEASED = DATASTORE_INFO_DIR / 'data_versions__1_0.json'


def setup_module():
    shutil.copytree(
        'tests/resources',
        'tests/resources_backup'
    )
    shutil.rmtree('tests/resources/TEST_DATASTORE')
    shutil.move(
        'tests/resources/EMPTY_DATASTORE',
        'tests/resources/TEST_DATASTORE'
    )


def teardown_module():
    shutil.rmtree('tests/resources')
    shutil.move(
        'tests/resources_backup',
        'tests/resources'
    )


def test_bump_empty_datastore():
    datastore = Datastore()
    with open(DRAFT_VERSION, encoding='utf-8') as f:
        bump_manifesto = DatastoreVersion(**json.load(f))
    datastore.bump_version(bump_manifesto, 'description')

    # check draft version after bump
    with open(DRAFT_VERSION, encoding='utf-8') as f:
        draft_after_bump = json.load(f)
    assert draft_after_bump['dataStructureUpdates'] == []

    # check metadata_all files after bump
    with open(METADATA_ALL_RELEASED, encoding='utf-8') as f:
        released_metadata_all = json.load(f)
    with open(METADATA_ALL_DRAFT, encoding='utf-8') as f:
        draft_metadata_all = json.load(f)
    assert len(draft_metadata_all['dataStructures']) == 1
    assert len(released_metadata_all['dataStructures']) == 1

    # check datastore_versions after bump
    with open(DATASTORE_VERSIONS, encoding='utf-8') as f:
        datastore_versions_json = json.load(f)
    assert len(datastore_versions_json['versions']) == 1
    assert datastore_versions_json['versions'][0]['version'] == '1.0.0.0'
    assert datastore_versions_json['versions'][0]['updateType'] == 'MAJOR'

    # check generated data_versions
    with open(DATA_VERSIONS_RELEASED, encoding='utf-8') as f:
        data_versions = json.load(f)
    assert data_versions == {
        'UTDANNING': 'UTDANNING__1_0.parquet'
    }

    # check renamed files
    assert os.listdir(UTDANNING_DATA_DIR) == ['UTDANNING__1_0.parquet']
    assert os.listdir(UTDANNING_METADATA_DIR) == ['UTDANNING__1_0_0.json']
