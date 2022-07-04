import json
import os

from job_executor.worker.steps import dataset_transformer


WORKING_DIR = 'tests/resources/worker/steps/transformer'
DESCRIBED_FILE_PATH = f'{WORKING_DIR}/input/KREFTREG_DS_described.json'
ENUMERATED_FILE_PATH = f'{WORKING_DIR}/input/KREFTREG_DS_enumerated.json'

DESCRIBED_EXPECTED_PATH = f'{WORKING_DIR}/expected/KREFTREG_DS_described.json'
ENUMERATED_EXPECTED_PATH = (
    f'{WORKING_DIR}/expected/KREFTREG_DS_enumerated.json'
)


def test_dataset_with_enumerated_valuedomain():
    with open(ENUMERATED_EXPECTED_PATH, encoding='utf-8') as f:
        expected_metadata_json = json.load(f)
    actual_metadata = dataset_transformer.run(
        ENUMERATED_FILE_PATH
    )
    assert actual_metadata.dict(by_alias=True) == expected_metadata_json


def test_dataset_with_described_valuedomain():
    with open(DESCRIBED_EXPECTED_PATH, encoding='utf-8') as f:
        expected_metadata_json = json.load(f)
    actual_metadata = dataset_transformer.run(
        DESCRIBED_FILE_PATH
    )
    assert actual_metadata.dict(by_alias=True) == expected_metadata_json


def teardown_function():
    working_files = os.listdir(WORKING_DIR)
    for file in working_files:
        if '_transformed.json' in file:
            os.remove(f"{WORKING_DIR}/{file}")
