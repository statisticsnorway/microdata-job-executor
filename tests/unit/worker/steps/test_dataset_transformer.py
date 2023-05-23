import json
import os
from pathlib import Path

from job_executor.worker.steps import dataset_transformer


WORKING_DIR = Path("tests/resources/worker/steps/transformer")
DESCRIBED_FILE_PATH = WORKING_DIR / "input/KREFTREG_DS_described.json"
ENUMERATED_FILE_PATH = WORKING_DIR / "input/KREFTREG_DS_enumerated.json"
STATUS_FILE_PATH = WORKING_DIR / "input/UTDANNING.json"
STATUS_PATCH_FILE_PATH = WORKING_DIR / "input/UTDANNING_PATCH.json"

DESCRIBED_EXPECTED_PATH = WORKING_DIR / "expected/KREFTREG_DS_described.json"
STATUS_EXPECTED_PATH = WORKING_DIR / "expected/UTDANNING.json"
STATUS_PATCH_EXPECTED_PATH = WORKING_DIR / "expected/UTDANNING_PATCH.json"
ENUMERATED_EXPECTED_PATH = WORKING_DIR / "expected/KREFTREG_DS_enumerated.json"


def test_dataset_with_enumerated_valuedomain():
    with open(ENUMERATED_EXPECTED_PATH, encoding="utf-8") as f:
        expected_metadata_json = json.load(f)
    actual_metadata = dataset_transformer.run(ENUMERATED_FILE_PATH)
    assert actual_metadata.dict(by_alias=True) == expected_metadata_json


def test_dataset_with_described_valuedomain():
    with open(DESCRIBED_EXPECTED_PATH, encoding="utf-8") as f:
        expected_metadata_json = json.load(f)
    actual_metadata = dataset_transformer.run(DESCRIBED_FILE_PATH)
    assert actual_metadata.dict(by_alias=True) == expected_metadata_json


def test_dataset_with_status_type():
    actual_metadata = dataset_transformer.run(STATUS_FILE_PATH)
    with open(STATUS_EXPECTED_PATH, "r", encoding="utf-8") as f:
        expected_metadata_json = json.load(f)
    assert actual_metadata.dict(by_alias=True) == expected_metadata_json


def test_patch_dataset_with_status_type():
    actual_metadata = dataset_transformer.run(STATUS_PATCH_FILE_PATH)
    with open(STATUS_PATCH_EXPECTED_PATH, "r", encoding="utf-8") as f:
        expected_metadata_json = json.load(f)
    assert actual_metadata.dict(by_alias=True) == expected_metadata_json


def teardown_function():
    working_files = os.listdir(f"{WORKING_DIR}/input")
    for file in working_files:
        if "__DRAFT.json" in file:
            os.remove(f"{WORKING_DIR}/input/{file}")
