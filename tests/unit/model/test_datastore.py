import os
import json
import shutil

from job_executor.model import Datastore
from job_executor.model import DatastoreVersion

datastore = Datastore()
DATASTORE_DIR = os.environ['DATASTORE_DIR']
DATASTORE_DATA_DIR = f'{DATASTORE_DIR}/data'
DATASTORE_METADATA_DIR = f'{DATASTORE_DIR}/metadata'
DATASTORE_INFO_DIR = f'{DATASTORE_DIR}/datastore'
DATA_VERSIONS = f'{DATASTORE_INFO_DIR}/datastore_versions.json'
DRAFT_VERSION = f'{DATASTORE_INFO_DIR}/draft_version.json'
METADATA_ALL_DRAFT = f'{DATASTORE_INFO_DIR}/metadata_all__DRAFT.json'


def draft_metadata_path(name: str):
    return f'{DATASTORE_METADATA_DIR}/{name}/{name}__DRAFT.json'


def draft_data_path(name: str):
    return f'{DATASTORE_DATA_DIR}/{name}/{name}__DRAFT.parquet'


def partitioned_draft_data_path(name: str):
    return f'{DATASTORE_DATA_DIR}/{name}/{name}__DRAFT'


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

    with open(draft_metadata_path(DATASET_NAME), encoding='utf-8') as f:
        sivstand_metadata = json.load(f)
    with open(METADATA_ALL_DRAFT, encoding='utf-8') as f:
        metadata_all_draft = json.load(f)
    with open(DRAFT_VERSION, encoding='utf-8') as f:
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
    with open(draft_metadata_path(DATASET_NAME), encoding='utf-8') as f:
        foedested_metadata = json.load(f)
    with open(METADATA_ALL_DRAFT, encoding='utf-8') as f:
        metadata_all_draft = json.load(f)
    with open(DRAFT_VERSION, encoding='utf-8') as f:
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
    with open(draft_metadata_path(DATASET_NAME), encoding='utf-8') as f:
        foedested_metadata = json.load(f)
    with open(METADATA_ALL_DRAFT, encoding='utf-8') as f:
        metadata_all_draft = json.load(f)
    with open(DRAFT_VERSION, encoding='utf-8') as f:
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

    with open(DRAFT_VERSION, encoding='utf-8') as f:
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

    with open(DRAFT_VERSION, encoding='utf-8') as f:
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

    with open(DRAFT_VERSION, encoding='utf-8') as f:
        draft_version = json.load(f)

    assert {
        'name': DATASET_NAME,
        'description': DESCRIPTION,
        'operation': 'ADD',
        'releaseStatus': 'PENDING_RELEASE'
    } in draft_version['dataStructureUpdates']


def test_bump_datastore_minor():
    with open(DRAFT_VERSION, encoding='utf-8') as f:
        bump_manifesto = DatastoreVersion(**json.load(f))

    datastore.bump_version(bump_manifesto, 'description')
    with open(DRAFT_VERSION, encoding='utf-8') as f:
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
    with open(
        f'{DATASTORE_DIR}/datastore/metadata_all__0_1_0.json', encoding='utf-8'
    ) as f:
        previous_metadata_all = json.load(f)
    with open(
        f'{DATASTORE_DIR}/datastore/metadata_all__0_2_0.json', encoding='utf-8'
    ) as f:
        released_metadata_all = json.load(f)
    assert (
        len(released_metadata_all['dataStructures']) -
        len(previous_metadata_all['dataStructures'])
    ) == 2
    with open(
        f'{DATASTORE_DIR}/datastore/datastore_versions.json', encoding='utf-8'
    ) as f:
        datastore_versions_json = json.load(f)
    assert datastore_versions_json['versions'][0]['version'] == '0.2.0.0'
    with open(
        f'{DATASTORE_DIR}/datastore/data_versions__0_2.json', encoding='utf-8'
    ) as f:
        data_versions = json.load(f)
    assert data_versions == {
        'BRUTTO_INNTEKT': 'BRUTTO_INNTEKT__0_2',
        'FOEDESTED': 'FOEDESTED__0_2.parquet',
        'FOEDSELSVEKT': 'FOEDSELSVEKT__0_1.parquet',
        'INNTEKT': 'INNTEKT__0_1',
        'KJOENN': 'KJOENN__0_1.parquet',
        'SIVSTAND': 'SIVSTAND__0_1.parquet'
    }


def test_bump_datastore_major():
    datastore.set_draft_release_status('FOEDSELSVEKT', 'PENDING_RELEASE')
    with open(DRAFT_VERSION, encoding='utf-8') as f:
        bump_manifesto = DatastoreVersion(**json.load(f))
    datastore.bump_version(bump_manifesto, 'description')
    
    with open(DRAFT_VERSION, encoding='utf-8') as f:
        draft_after_bump = json.load(f)
    assert draft_after_bump['dataStructureUpdates'] == [
        {
            'description': 'oppdaterte metadata',
            'name': 'SIVSTAND',
            'operation': 'PATCH_METADATA',
            'releaseStatus': 'DRAFT'
        },
        {
            'description': 'Fjernet variabel',
            'name': 'INNTEKT',
            'operation': 'REMOVE',
            'releaseStatus': 'DRAFT'
        }
    ]
    with open(
        f'{DATASTORE_DIR}/datastore/metadata_all__0_2_0.json', encoding='utf-8'
    ) as f:
        previous_metadata_all = json.load(f)
    with open(
        f'{DATASTORE_DIR}/datastore/metadata_all__1_0_0.json', encoding='utf-8'
    ) as f:
        released_metadata_all = json.load(f)
    with open(
        f'{DATASTORE_DIR}/datastore/datastore_versions.json', encoding='utf-8'
    ) as f:
        datastore_versions_json = json.load(f)
    assert datastore_versions_json['versions'][0]['version'] == '1.0.0.0'
    assert (
        len(released_metadata_all['dataStructures']) ==
        len(previous_metadata_all['dataStructures'])
    )
    with open(
        f'{DATASTORE_DIR}/datastore/data_versions__1_0.json', encoding='utf-8'
    ) as f:
        data_versions = json.load(f)
    assert data_versions == {
        'BRUTTO_INNTEKT': 'BRUTTO_INNTEKT__0_2',
        'FOEDESTED': 'FOEDESTED__0_2.parquet',
        'FOEDSELSVEKT': 'FOEDSELSVEKT__1_0.parquet',
        'INNTEKT': 'INNTEKT__0_1',
        'KJOENN': 'KJOENN__0_1.parquet',
        'SIVSTAND': 'SIVSTAND__0_1.parquet'
    }
