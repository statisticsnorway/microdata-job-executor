import json
from pathlib import Path

from job_executor.worker.steps import dataset_transformer
from tests.resources.worker.steps.transformer import input_data

EXPECTED_DIR = Path("tests/resources/worker/steps/transformer")
DESCRIBED_EXPECTED_PATH = EXPECTED_DIR / "expected/KREFTREG_DS_described.json"
STATUS_EXPECTED_PATH = EXPECTED_DIR / "expected/UTDANNING.json"
STATUS_PATCH_EXPECTED_PATH = EXPECTED_DIR / "expected/UTDANNING_PATCH.json"
ENUMERATED_EXPECTED_PATH = (
    EXPECTED_DIR / "expected/KREFTREG_DS_enumerated.json"
)


def test_dataset_with_enumerated_valuedomain():
    actual_metadata = dataset_transformer.run(
        input_data.KREFTREG_DS_ENUMERATED
    )
    with open(ENUMERATED_EXPECTED_PATH, encoding="utf-8") as f:
        expected_metadata_json = json.load(f)
    assert actual_metadata == expected_metadata_json


def test_dataset_with_described_valuedomain():
    with open(DESCRIBED_EXPECTED_PATH, encoding="utf-8") as f:
        expected_metadata_json = json.load(f)
    actual_metadata = dataset_transformer.run(input_data.KREFTREG_DS_DESCRIBED)
    assert actual_metadata == expected_metadata_json


def test_dataset_with_status_type():
    actual_metadata = dataset_transformer.run(input_data.UTDANNING)
    with open(STATUS_EXPECTED_PATH, "r", encoding="utf-8") as f:
        expected_metadata_json = json.load(f)
    assert actual_metadata == expected_metadata_json


def test_patch_dataset_with_status_type():
    actual_metadata = dataset_transformer.run(input_data.UTDANNING_PATCH)
    with open(STATUS_PATCH_EXPECTED_PATH, "r", encoding="utf-8") as f:
        expected_metadata_json = json.load(f)
    assert actual_metadata == expected_metadata_json
