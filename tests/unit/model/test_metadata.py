import json
import shutil

from job_executor.model import MetadataAll, Metadata


def load_json(file_path):
    return json.load(open(file_path, encoding='utf'))


TEST_DIR = 'tests/resources/model/metadata'
METADATA_ALL_PATH = f'{TEST_DIR}/metadata_all.json'

ENUMERATED_METADATA = load_json(f'{TEST_DIR}/enumerated_metadata.json')
DESCRIBED_METADATA = load_json(f'{TEST_DIR}/described_metadata.json')

METADATA = load_json(f'{TEST_DIR}/metadata.json')
UPDATED_METADATA = load_json(f'{TEST_DIR}/updated_metadata.json')
PATCHED_METADATA = load_json(f'{TEST_DIR}/patched_metadata.json')


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


def test_metadata_all():
    metadata_all = MetadataAll(**load_json(METADATA_ALL_PATH))
    assert metadata_all.dict(by_alias=True) == load_json(METADATA_ALL_PATH)


def test_metadata():
    enumerated_metadata = Metadata(**ENUMERATED_METADATA)
    assert enumerated_metadata.dict(by_alias=True) == ENUMERATED_METADATA

    described_metadata = Metadata(**DESCRIBED_METADATA)
    assert described_metadata.dict(by_alias=True) == DESCRIBED_METADATA


def test_patch():
    metadata = Metadata(**METADATA)
    updated_metadata = Metadata(**UPDATED_METADATA)
    patched_metadata = metadata.patch(updated_metadata)
    assert patched_metadata.dict(by_alias=True) == PATCHED_METADATA
