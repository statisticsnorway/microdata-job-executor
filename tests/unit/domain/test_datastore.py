import os
import json
import shutil

from job_executor.domain import datastore
from job_executor.model import DatastoreVersion


DATASTORE_DIR = os.environ['DATASTORE_DIR']
DATASTORE_DATA_DIR = f'{DATASTORE_DIR}/data'
DATASTORE_METADATA_DIR = f'{DATASTORE_DIR}/metadata'
DATASTORE_INFO_DIR = f'{DATASTORE_DIR}/datastore'
DATA_VERSIONS = f'{DATASTORE_INFO_DIR}/datastore_versions.json'
DRAFT_VERSION = f'{DATASTORE_INFO_DIR}/draft_version.json'
METADATA_ALL_DRAFT = f'{DATASTORE_INFO_DIR}/metadata_all__DRAFT.json'

draft_metadata_path = (
    lambda name: f'{DATASTORE_METADATA_DIR}/{name}/{name}__DRAFT.json'
)
draft_data_path = (
    lambda name: f'{DATASTORE_DATA_DIR}/{name}/{name}__DRAFT.parquet'
)
partitioned_draft_data_path = (
    lambda name: f'{DATASTORE_DATA_DIR}/{name}/{name}__DRAFT'
)


def setup_module():
    shutil.copytree(
        'tests/resources',
        'tests/resources_backup'
    )


def teardown_module():
    shutil.rmtree('tests/resources')
    shutil.move(
        'tests/resources_backup',
        'tests/resources'
    )


def test_patch_metadata():
    DATASET_NAME = 'SIVSTAND'
    DESCRIPTION = 'oppdaterte metadata'
    datastore.patch_metadata(DATASET_NAME, DESCRIPTION)
    
    with open(draft_metadata_path(DATASET_NAME)) as f:
        sivstand_metadata = json.load(f)
    with open(METADATA_ALL_DRAFT) as f:
        metadata_all_draft = json.load(f)
    with open(DRAFT_VERSION) as f:
        draft_version = json.load(f)
    
    assert {
        'name': DATASET_NAME,
        'description': DESCRIPTION,
        'operation': 'PATCH_METADATA',
        'releaseStatus': 'DRAFT'
    } in draft_version['dataStructureUpdates']
    assert sivstand_metadata in metadata_all_draft['dataStructures']


def test_add():
    DATASET_NAME = 'FOEDESTED'
    DESCRIPTION = 'første publisering'
    datastore.add(DATASET_NAME, DESCRIPTION)

    assert os.path.exists(draft_data_path(DATASET_NAME))
    with open(draft_metadata_path(DATASET_NAME)) as f:
        foedested_metadata = json.load(f)
    with open(METADATA_ALL_DRAFT) as f:
        metadata_all_draft = json.load(f)
    with open(DRAFT_VERSION) as f:
        draft_version = json.load(f)

    assert {
        'name': DATASET_NAME,
        'description': DESCRIPTION,
        'operation': 'ADD',
        'releaseStatus': 'DRAFT'
    } in draft_version['dataStructureUpdates']
    assert foedested_metadata in metadata_all_draft['dataStructures']          


def test_change_data(): 
    DATASET_NAME = 'FOEDSELSVEKT'
    DESCRIPTION = 'oppdaterte data'
    datastore.change_data(DATASET_NAME, DESCRIPTION)

    assert os.path.exists(draft_data_path(DATASET_NAME))
    with open(draft_metadata_path(DATASET_NAME)) as f:
        foedested_metadata = json.load(f)
    with open(METADATA_ALL_DRAFT) as f:
        metadata_all_draft = json.load(f)
    with open(DRAFT_VERSION) as f:
        draft_version = json.load(f)

    assert {
        'name': DATASET_NAME,
        'description': DESCRIPTION,
        'operation': 'CHANGE_DATA',
        'releaseStatus': 'DRAFT'
    } in draft_version['dataStructureUpdates']
    assert foedested_metadata in metadata_all_draft['dataStructures'] 


def test_remove():
    DATASET_NAME = 'INNTEKT'
    DESCRIPTION = 'Fjernet variabel'
    datastore.remove(DATASET_NAME, DESCRIPTION)
    
    with open(DRAFT_VERSION) as f:
        draft_version = json.load(f)

    assert {
        'name': DATASET_NAME,
        'description': DESCRIPTION,
        'operation': 'REMOVE',
        'releaseStatus': 'DRAFT'
    } in draft_version['dataStructureUpdates']


def test_delete_draft():
    DATASET_NAME = 'UTDANNING'
    datastore.delete_draft(DATASET_NAME)

    with open(DRAFT_VERSION) as f:
        draft_version = json.load(f)

    assert not os.path.exists(draft_data_path(DATASET_NAME))
    assert not os.path.exists(partitioned_draft_data_path(DATASET_NAME))
    assert not os.path.exists(draft_metadata_path(DATASET_NAME))
    assert not [
        update for update in draft_version['dataStructureUpdates']
        if update['name'] == DATASET_NAME
    ]


def test_set_draft_release_status():
    DATASET_NAME = 'FOEDESTED'
    DESCRIPTION = 'første publisering'
    NEW_STATUS = 'PENDING_RELEASE'
    datastore.set_draft_release_status(DATASET_NAME, NEW_STATUS)
    
    with open(DRAFT_VERSION) as f:
        draft_version = json.load(f)

    assert {
        'name': DATASET_NAME,
        'description': DESCRIPTION,
        'operation': 'ADD',
        'releaseStatus': 'PENDING_RELEASE'
    } in draft_version['dataStructureUpdates']


def test_bump_datastore():
    with open(DRAFT_VERSION) as f:
        bump_manifesto = DatastoreVersion(**json.load(f))

    datastore.bump_version(bump_manifesto, 'description')
    with open(DRAFT_VERSION) as f:
        draft_after_bump = json.load(f)
    assert draft_after_bump['dataStructureUpdates'] == [
        {
            'description': 'oppdaterte metadata',
            'name': 'SIVSTAND',
            'operation': 'PATCH_METADATA',
            'releaseStatus': 'DRAFT'
        },
        {
            'description': 'oppdaterte data',
            'name': 'FOEDSELSVEKT',
            'operation': 'CHANGE_DATA',
            'releaseStatus': 'DRAFT'
        },
        {
            'description': 'Fjernet variabel',
            'name': 'INNTEKT',
            'operation': 'REMOVE',
            'releaseStatus': 'DRAFT'
        }
    ]
    with open(f'{DATASTORE_DIR}/datastore/metadata_all__0_1_0.json') as f:
        previous_metadata_all = json.load(f) 
    with open(f'{DATASTORE_DIR}/datastore/metadata_all__0_2_0.json') as f:
        released_metadata_all = json.load(f)
    assert (
        len(released_metadata_all['dataStructures']) -
        len(previous_metadata_all['dataStructures'])
    ) == 1
    with open(f'{DATASTORE_DIR}/datastore/data_versions__0_2.json') as f:
        data_versions = json.load(f)
    assert data_versions == {
        'FOEDESTED': 'FOEDESTED__0_2.parquet',
        'FOEDSELSVEKT': 'FOEDSELSVEKT__0_1.parquet',
        'INNTEKT': 'INNTEKT__0_1',
        'KJOENN': 'KJOENN__0_1.parquet',
        'SIVSTAND': 'SIVSTAND__0_1.parquet'
    }