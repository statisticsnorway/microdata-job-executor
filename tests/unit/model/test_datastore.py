import os
import json
import shutil
from pathlib import Path

from requests_mock import Mocker as RequestsMocker
from tests.unit.test_util import get_file_list_from_dir

from job_executor.model import Datastore
from job_executor.model import DatastoreVersion


datastore = Datastore()
JOB_SERVICE_URL = os.getenv('JOB_SERVICE_URL')
JOB_ID = '123-123-123-123'
DATASTORE_DIR = os.environ['DATASTORE_DIR']
WORKING_DIR = os.environ['WORKING_DIR']
DATASTORE_DATA_DIR = f'{DATASTORE_DIR}/data'
DATASTORE_METADATA_DIR = f'{DATASTORE_DIR}/metadata'
DATASTORE_INFO_DIR = f'{DATASTORE_DIR}/datastore'
DATA_VERSIONS = f'{DATASTORE_INFO_DIR}/datastore_versions.json'
DRAFT_VERSION = f'{DATASTORE_INFO_DIR}/draft_version.json'
METADATA_ALL_DRAFT = f'{DATASTORE_INFO_DIR}/metadata_all__DRAFT.json'
DATASTORE_ARCHIVE_DIR = f'{DATASTORE_DIR}/archive'


def working_dir_metadata_draft_path(name: str):
    return f'{WORKING_DIR}/{name}__DRAFT.json'


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


def test_patch_metadata(requests_mock: RequestsMocker):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    DATASET_NAME = 'SIVSTAND'
    DESCRIPTION = 'oppdaterte metadata'
    datastore.patch_metadata(JOB_ID, DATASET_NAME, DESCRIPTION)
    assert len(requests_mock.request_history) == 2
    assert not os.path.exists(working_dir_metadata_draft_path(DATASET_NAME))
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
    assert draft_version['releaseTime'] > 1_000_000
    assert sivstand_metadata in metadata_all_draft['dataStructures']


def test_add(requests_mock: RequestsMocker):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    DATASET_NAME = 'FOEDESTED'
    DESCRIPTION = 'første publisering'
    datastore.add(JOB_ID, DATASET_NAME, DESCRIPTION)
    assert len(requests_mock.request_history) == 2
    assert not os.path.exists(working_dir_metadata_draft_path(DATASET_NAME))
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
    assert draft_version['releaseTime'] > 1_000_000
    assert foedested_metadata in metadata_all_draft['dataStructures']


def test_change_data(requests_mock: RequestsMocker):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    DATASET_NAME = 'FOEDSELSVEKT'
    DESCRIPTION = 'oppdaterte data'
    datastore.change_data(JOB_ID, DATASET_NAME, DESCRIPTION)
    assert len(requests_mock.request_history) == 2
    assert not os.path.exists(working_dir_metadata_draft_path(DATASET_NAME))
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
    assert draft_version['releaseTime'] > 1_000_000
    assert foedested_metadata in metadata_all_draft['dataStructures']


def test_remove(requests_mock: RequestsMocker):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    DATASET_NAME = 'INNTEKT'
    DESCRIPTION = 'Fjernet variabel'
    datastore.remove(JOB_ID, DATASET_NAME, DESCRIPTION)
    assert len(requests_mock.request_history) == 2

    with open(DRAFT_VERSION, encoding='utf-8') as f:
        draft_version = json.load(f)

    assert {
        'name': DATASET_NAME,
        'description': DESCRIPTION,
        'operation': 'REMOVE',
        'releaseStatus': 'DRAFT'
    } in draft_version['dataStructureUpdates']
    assert draft_version['releaseTime'] > 1_000_000


def test_delete_draft(requests_mock: RequestsMocker):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    DATASET_NAME = 'UTDANNING'
    datastore.delete_draft(JOB_ID, DATASET_NAME)
    assert len(requests_mock.request_history) == 2

    with open(DRAFT_VERSION, encoding='utf-8') as f:
        draft_version = json.load(f)

    assert not os.path.exists(draft_data_path(DATASET_NAME))
    assert not os.path.exists(partitioned_draft_data_path(DATASET_NAME))
    assert not os.path.exists(draft_metadata_path(DATASET_NAME))
    assert not [
        update for update in draft_version['dataStructureUpdates']
        if update['name'] == DATASET_NAME
    ]
    assert draft_version['releaseTime'] > 1_000_000


def test_set_draft_release_status(requests_mock: RequestsMocker):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    DATASET_NAME = 'FOEDESTED'
    DESCRIPTION = 'første publisering'
    NEW_STATUS = 'PENDING_RELEASE'
    datastore.set_draft_release_status(JOB_ID, DATASET_NAME, NEW_STATUS)
    assert len(requests_mock.request_history) == 2
    with open(DRAFT_VERSION, encoding='utf-8') as f:
        draft_version = json.load(f)

    assert {
        'name': DATASET_NAME,
        'description': DESCRIPTION,
        'operation': 'ADD',
        'releaseStatus': 'PENDING_RELEASE'
    } in draft_version['dataStructureUpdates']
    # Try again after a possible interrupt
    datastore.set_draft_release_status(JOB_ID, DATASET_NAME, NEW_STATUS)
    assert len(requests_mock.request_history) == 4
    assert requests_mock.request_history[3].json() == {
        'status': 'completed',
        'log': 'Status already set to PENDING_RELEASE'
    }
    with open(DRAFT_VERSION, encoding='utf-8') as f:
        draft_version = json.load(f)

    assert {
        'name': DATASET_NAME,
        'description': DESCRIPTION,
        'operation': 'ADD',
        'releaseStatus': 'PENDING_RELEASE'
    } in draft_version['dataStructureUpdates']
    assert draft_version['releaseTime'] > 1_000_000


def test_bump_datastore_minor(requests_mock: RequestsMocker):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    with open(DRAFT_VERSION, encoding='utf-8') as f:
        bump_manifesto = DatastoreVersion(**json.load(f))

    datastore.bump_version(JOB_ID, bump_manifesto, 'description')
    assert len(requests_mock.request_history) == 2

    with open(DRAFT_VERSION, encoding='utf-8') as f:
        draft_after_bump = json.load(f)
    assert draft_after_bump['releaseTime'] > 1_000_000
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
        f'{DATASTORE_DIR}/datastore/metadata_all__1_0_0.json', encoding='utf-8'
    ) as f:
        previous_metadata_all = json.load(f)
    with open(
        f'{DATASTORE_DIR}/datastore/metadata_all__1_1_0.json', encoding='utf-8'
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
    assert datastore_versions_json['versions'][0]['version'] == '1.1.0.0'
    with open(
        f'{DATASTORE_DIR}/datastore/data_versions__1_1.json', encoding='utf-8'
    ) as f:
        data_versions = json.load(f)
    assert data_versions == {
        'BRUTTO_INNTEKT': 'BRUTTO_INNTEKT__1_1',
        'FOEDESTED': 'FOEDESTED__1_1.parquet',
        'FOEDSELSVEKT': 'FOEDSELSVEKT__1_0.parquet',
        'INNTEKT': 'INNTEKT__1_0',
        'KJOENN': 'KJOENN__1_0.parquet',
        'SIVSTAND': 'SIVSTAND__1_0.parquet'
    }
    assert len(get_file_list_from_dir(Path(DATASTORE_ARCHIVE_DIR))) == 1


def test_bump_datastore_major(requests_mock: RequestsMocker):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    datastore.set_draft_release_status(
        JOB_ID, 'FOEDSELSVEKT', 'PENDING_RELEASE'
    )
    assert len(requests_mock.request_history) == 2
    with open(DRAFT_VERSION, encoding='utf-8') as f:
        bump_manifesto = DatastoreVersion(**json.load(f))
    datastore.bump_version(JOB_ID, bump_manifesto, 'description')
    assert len(requests_mock.request_history) == 4

    with open(DRAFT_VERSION, encoding='utf-8') as f:
        draft_after_bump = json.load(f)
    assert draft_after_bump['releaseTime'] > 1_000_000
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
        f'{DATASTORE_DIR}/datastore/metadata_all__1_1_0.json', encoding='utf-8'
    ) as f:
        previous_metadata_all = json.load(f)
    with open(
        f'{DATASTORE_DIR}/datastore/metadata_all__2_0_0.json', encoding='utf-8'
    ) as f:
        released_metadata_all = json.load(f)
    with open(
        f'{DATASTORE_DIR}/datastore/datastore_versions.json', encoding='utf-8'
    ) as f:
        datastore_versions_json = json.load(f)
    assert datastore_versions_json['versions'][0]['version'] == '2.0.0.0'
    assert (
        len(released_metadata_all['dataStructures']) ==
        len(previous_metadata_all['dataStructures'])
    )
    with open(
        f'{DATASTORE_DIR}/datastore/data_versions__2_0.json', encoding='utf-8'
    ) as f:
        data_versions = json.load(f)
    assert data_versions == {
        'BRUTTO_INNTEKT': 'BRUTTO_INNTEKT__1_1',
        'FOEDESTED': 'FOEDESTED__1_1.parquet',
        'FOEDSELSVEKT': 'FOEDSELSVEKT__2_0.parquet',
        'INNTEKT': 'INNTEKT__1_0',
        'KJOENN': 'KJOENN__1_0.parquet',
        'SIVSTAND': 'SIVSTAND__1_0.parquet'
    }
    assert len(get_file_list_from_dir(Path(DATASTORE_ARCHIVE_DIR))) == 2


def test_delete_draft_after_interrupt(requests_mock: RequestsMocker):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    DATASET_NAME = 'SIVSTAND'
    # Previous interrupted run deleted metadata
    os.remove(draft_metadata_path(DATASET_NAME))
    datastore.delete_draft(JOB_ID, DATASET_NAME)
    assert len(requests_mock.request_history) == 2
    assert requests_mock.request_history[1].json() == {
        'status': 'completed'
    }
    with open(DRAFT_VERSION, encoding='utf-8') as f:
        draft_version = json.load(f)

    assert not os.path.exists(draft_metadata_path(DATASET_NAME))
    assert not [
        update for update in draft_version['dataStructureUpdates']
        if update['name'] == DATASET_NAME
    ]
