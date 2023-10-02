import json
from pathlib import Path

import pytest

from job_executor.worker.steps import dataset_transformer
from tests.resources.worker.steps import transformer as test_data


def test_transform_identifier():
    # test that pseudonymized variable has dataType Long
    assert test_data.PERSON_IDENTIFIER["dataType"] == "STRING"
    transformed_identifier = dataset_transformer._transform_variable(
        test_data.PERSON_IDENTIFIER, "Identifier", "2020-01-01", "2020-12-31"
    )
    assert transformed_identifier["dataType"] == "Long"

    # test that not pseudonymized variable keeps dataType
    assert (
        test_data.BK_HELSTASJONSKONSULTASJON_IDENTIFIER["dataType"] == "LONG"
    )
    transformed_identifier = dataset_transformer._transform_variable(
        test_data.BK_HELSTASJONSKONSULTASJON_IDENTIFIER,
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
            "description", [], test_data.CODELIST
        )
    )
    assert transformed_codelist == test_data.TRANSFORMED_CODELIST

    """
    SentinelAndMissingValues are included in each represented variables
    code list. And marked as a missing value in the missingValues list
    """
    transformed_codelist_with_missing = (
        dataset_transformer._represented_variables_from_code_list(
            "description", test_data.MISSING_VALUES, test_data.CODELIST
        )
    )

    assert (
        transformed_codelist_with_missing
        == test_data.TRANSFORMED_CODELIST_WITH_MISSING_VALUES
    )

    with pytest.raises(ValueError) as e:
        dataset_transformer._represented_variables_from_code_list(
            "description", [], []
        )
    assert "Code list can not be empty" in str(e)


def test_dataset_with_enumerated_valuedomain():
    actual_metadata = dataset_transformer.run(test_data.KREFTREG_DS_ENUMERATED)
    assert actual_metadata == test_data.ENUMERATED_EXPECTED


def test_dataset_with_described_valuedomain():
    actual_metadata = dataset_transformer.run(test_data.KREFTREG_DS_DESCRIBED)
    assert actual_metadata == test_data.DESCRIBED_EXPECTED


def test_dataset_with_status_type():
    actual_metadata = dataset_transformer.run(test_data.UTDANNING)
    assert actual_metadata == test_data.STATUS_EXPECTED


def test_patch_dataset_with_status_type():
    actual_metadata = dataset_transformer.run(test_data.UTDANNING_PATCH)
    assert actual_metadata == test_data.STATUS_PATCH_EXPECTED
