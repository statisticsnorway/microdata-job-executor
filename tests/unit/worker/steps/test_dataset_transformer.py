import json
from pathlib import Path

import pytest

from job_executor.worker.steps import dataset_transformer
from tests.resources.worker.steps.transformer import input_data

EXPECTED_DIR = Path("tests/resources/worker/steps/transformer")
DESCRIBED_EXPECTED_PATH = EXPECTED_DIR / "expected/KREFTREG_DS_described.json"
STATUS_EXPECTED_PATH = EXPECTED_DIR / "expected/UTDANNING.json"
STATUS_PATCH_EXPECTED_PATH = EXPECTED_DIR / "expected/UTDANNING_PATCH.json"
ENUMERATED_EXPECTED_PATH = (
    EXPECTED_DIR / "expected/KREFTREG_DS_enumerated.json"
)


def test_transform_identifier():
    # test that pseudonymized variable has dataType Long
    assert input_data.PERSON_IDENTIFIER["dataType"] == "STRING"
    transformed_identifier = dataset_transformer._transform_variable(
        input_data.PERSON_IDENTIFIER, "Identifier", "2020-01-01", "2020-12-31"
    )
    assert transformed_identifier["dataType"] == "Long"

    # test that not pseudonymized variable keeps dataType
    assert (
        input_data.BK_HELSTASJONSKONSULTASJON_IDENTIFIER["dataType"] == "LONG"
    )
    transformed_identifier = dataset_transformer._transform_variable(
        input_data.BK_HELSTASJONSKONSULTASJON_IDENTIFIER,
        "Identifier",
        "2020-01-01",
        "2020-12-31",
    )
    assert transformed_identifier["dataType"] == "Long"


def test_transform_codelist():
    """
    Value domains with codelists get transformed to multiple
    represented periods based on each unique period of codes
    """
    transformed_codelist = (
        dataset_transformer._represented_variables_from_code_list(
            "description", [], input_data.CODELIST
        )
    )
    assert transformed_codelist == input_data.TRANSFORMED_CODELIST

    """
    SentinelAndMissingValues are included in each represented variables
    code list. And marked as a missing value in the missingValues list
    """
    transformed_codelist_with_missing = (
        dataset_transformer._represented_variables_from_code_list(
            "description", input_data.MISSING_VALUES, input_data.CODELIST
        )
    )

    assert (
        transformed_codelist_with_missing
        == input_data.TRANSFORMED_CODELIST_WITH_MISSING_VALUES
    )

    with pytest.raises(ValueError) as e:
        dataset_transformer._represented_variables_from_code_list(
            "description", [], []
        )
    assert "Code list can not be empty" in str(e)


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
