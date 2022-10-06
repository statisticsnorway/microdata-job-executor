
import json
import os
import shutil

from job_executor.adapter import local_storage

WORKING_DIR = os.environ['WORKING_DIR']
DATASTORE_DIR = os.environ['DATASTORE_DIR']
DATASTORE_DATA_DIR = f'{DATASTORE_DIR}/data'
DATASTORE_METADATA_DIR = f'{DATASTORE_DIR}/metadata'

DATASTORE_VERSIONS_PATH = f'{DATASTORE_DIR}/datastore/datastore_versions.json'
DRAFT_METADATA_ALL_PATH = f'{DATASTORE_DIR}/datastore/metadata_all__draft.json'
DRAFT_VERSION_PATH = f'{DATASTORE_DIR}/datastore/draft_version.json'
DATA_VERSIONS_PATH = f'{DATASTORE_DIR}/datastore/data_versions__0_1.json'
METADATA_ALL_PATH = f'{DATASTORE_DIR}/datastore/metadata_all__0_1_0.json'

DRAFT_DATASET_NAME = 'UTDANNING'
DRAFT_DATA_PATH = (
    f'{DATASTORE_DATA_DIR}/UTDANNING/UTDANNING__DRAFT.parquet'
)
DRAFT_METADATA_PATH = (
    f'{DATASTORE_METADATA_DIR}/UTDANNING/UTDANNING__DRAFT.json'
)

DRAFT2_DATASET_NAME = 'BRUTTO_INNTEKT'
RELEASED_DRAFT2_DATA_PATH = (
    f'{DATASTORE_DATA_DIR}/BRUTTO_INNTEKT/BRUTTO_INNTEKT__0_2'
)
RELEASED_DRAFT2_METADATA_PATH = (
    f'{DATASTORE_METADATA_DIR}/BRUTTO_INNTEKT/BRUTTO_INNTEKT__0_2_0.json'
)

WORKING_DIR_DATASET = 'FOEDESTED'
MOVED_WORKING_DIR_DATASET_DATA_PATH = (
    f'{DATASTORE_DATA_DIR}/FOEDESTED/FOEDESTED__DRAFT.parquet'
)


def setup_module():

    if os.path.isdir('tests/resources_backup'):
        shutil.rmtree('tests/resources_backup')

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


def read_json(file_path: str) -> dict:
    with open(file_path, encoding='utf-8') as f:
        return json.load(f)


def test_make_dataset_dir():
    local_storage.make_dataset_dir(WORKING_DIR_DATASET)
    assert os.path.isdir(f'{DATASTORE_DATA_DIR}/{WORKING_DIR_DATASET}')
    assert os.path.isdir(f'{DATASTORE_METADATA_DIR}/{WORKING_DIR_DATASET}')


def test_get_data_versions():
    assert (
        local_storage.get_data_versions('0_1_0')
        == read_json(DATA_VERSIONS_PATH)
    )


def test_write_data_versions():
    local_storage.write_data_versions({}, '0_1_0')
    assert read_json(DATA_VERSIONS_PATH) == {}


def test_get_draft_version():
    assert (
        local_storage.get_draft_version()
        == read_json(DRAFT_VERSION_PATH)
    )


def test_write_draft_version():
    local_storage.write_draft_version({})
    assert read_json(DRAFT_VERSION_PATH) == {}


def test_get_datastore_versions():
    assert (
        local_storage.get_datastore_versions()
        == read_json(DATASTORE_VERSIONS_PATH)
    )


def test_write_datastore_versions():
    local_storage.write_datastore_versions({})
    assert read_json(DATASTORE_VERSIONS_PATH) == {}


def test_get_metadata_all():
    assert (
        local_storage.get_metadata_all('0_1_0')
        == read_json(METADATA_ALL_PATH)
    )


def test_write_metadata_all():
    local_storage.write_metadata_all({}, '0_1_0')
    assert read_json(METADATA_ALL_PATH) == {}


def test_get_metadata():
    assert (
        local_storage.get_metadata(DRAFT_DATASET_NAME, 'DRAFT')
        == read_json(DRAFT_METADATA_PATH)
    )


def test_write_metadata():
    local_storage.write_metadata(
        {}, DRAFT_DATASET_NAME, 'DRAFT'
    )
    assert read_json(DRAFT_METADATA_PATH) == {}


def test_delete_metadata_draft():
    local_storage.delete_metadata_draft(DRAFT_DATASET_NAME)
    assert not os.path.isfile(DRAFT_METADATA_PATH)


def delete_parquet_draft():
    local_storage.delete_parquet_draft(DRAFT_DATASET_NAME)
    assert not os.path.isfile(DRAFT_DATA_PATH)


def test_rename_metadata_draft_to_release():
    local_storage.rename_metadata_draft_to_release(
        DRAFT2_DATASET_NAME, '0_2_0'
    )
    assert os.path.isfile(RELEASED_DRAFT2_METADATA_PATH)


def test_rename_parquet_draft_to_release():
    release_path = local_storage.rename_parquet_draft_to_release(
        DRAFT2_DATASET_NAME, '0_2_0'
    )
    assert os.path.isdir(RELEASED_DRAFT2_DATA_PATH)
    assert release_path == f'{DRAFT2_DATASET_NAME}__0_2'


def test_move_working_dir_parquet_to_datastore():
    local_storage.move_working_dir_parquet_to_datastore(WORKING_DIR_DATASET)
    assert os.path.isfile(MOVED_WORKING_DIR_DATASET_DATA_PATH)
