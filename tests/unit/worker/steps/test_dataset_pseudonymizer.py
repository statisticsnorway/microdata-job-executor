import json
import os
import shutil
from pathlib import Path

import pyarrow
import pytest
from pyarrow import dataset, parquet

from job_executor.adapter import pseudonym_service
from job_executor.exception import BuilderStepError
from job_executor.model import Metadata
from job_executor.worker.steps import dataset_pseudonymizer

TABLE_SIZE = 1000
UNIT_ID_INPUT = [f"i{count}" for count in range(TABLE_SIZE)]
UNIT_ID_PSEUDONYMIZED = [count for count in range(TABLE_SIZE)]

INPUT_TABLE = pyarrow.Table.from_pydict(
    {
        "unit_id": UNIT_ID_INPUT,
        "value": UNIT_ID_INPUT,
        "start_epoch_days": pyarrow.array(
            [18200] * TABLE_SIZE, type=pyarrow.int16()
        ),
        "stop_epoch_days": pyarrow.array(
            [18201] * TABLE_SIZE, type=pyarrow.int16()
        ),
    }
)

INPUT_TABLE_START_YEAR = pyarrow.Table.from_pydict(
    {
        "unit_id": UNIT_ID_INPUT,
        "value": UNIT_ID_INPUT,
        "start_epoch_days": pyarrow.array(
            [18200] * TABLE_SIZE, type=pyarrow.int16()
        ),
        "stop_epoch_days": pyarrow.array(
            [18201] * TABLE_SIZE, type=pyarrow.int16()
        ),
        "start_year": pyarrow.array(
            [str(year) for year in [2020] * TABLE_SIZE]
        ),
    }
)

EXPECTED_TABLE = pyarrow.Table.from_pydict(
    {
        "unit_id": UNIT_ID_PSEUDONYMIZED,
        "value": UNIT_ID_INPUT,
        "start_epoch_days": pyarrow.array(
            [18200] * TABLE_SIZE, type=pyarrow.int16()
        ),
        "stop_epoch_days": pyarrow.array(
            [18201] * TABLE_SIZE, type=pyarrow.int16()
        ),
    }
)

EXPECTED_TABLE_START_YEAR = pyarrow.Table.from_pydict(
    {
        "unit_id": UNIT_ID_PSEUDONYMIZED,
        "value": UNIT_ID_INPUT,
        "start_epoch_days": pyarrow.array(
            [18200] * TABLE_SIZE, type=pyarrow.int16()
        ),
        "stop_epoch_days": pyarrow.array(
            [18201] * TABLE_SIZE, type=pyarrow.int16()
        ),
        "start_year": pyarrow.array(
            [str(year) for year in [2020] * TABLE_SIZE]
        ),
    }
)

EXPECTED_TABLE_WITH_BOTH_PSEUDONYMIZED = pyarrow.Table.from_pydict(
    {
        "unit_id": UNIT_ID_PSEUDONYMIZED,
        "value": UNIT_ID_PSEUDONYMIZED,
        "start_epoch_days": pyarrow.array(
            [18200] * TABLE_SIZE, type=pyarrow.int16()
        ),
        "stop_epoch_days": pyarrow.array(
            [18201] * TABLE_SIZE, type=pyarrow.int16()
        ),
    }
)

EXPECTED_TABLE_WITH_ONLY_VALUE_PSEUDONYMIZED = pyarrow.Table.from_pydict(
    {
        "unit_id": UNIT_ID_INPUT,
        "value": UNIT_ID_PSEUDONYMIZED,
        "start_epoch_days": pyarrow.array(
            [18200] * TABLE_SIZE, type=pyarrow.int16()
        ),
        "stop_epoch_days": pyarrow.array(
            [18201] * TABLE_SIZE, type=pyarrow.int16()
        ),
    }
)


WORKING_DIR = Path("tests/resources/worker/steps/pseudonymizer")
INPUT_PARQUET_PATH = WORKING_DIR / "input.parquet"
OUTPUT_PARQUET_PATH = WORKING_DIR / "input_pseudonymized.parquet"

INPUT_PARQUET_PATH_START_YEAR = WORKING_DIR / "input_start_year.parquet"
OUTPUT_PARQUET_PATH_START_YEAR = (
    WORKING_DIR / "input_start_year_pseudonymized.parquet"
)

JOB_ID = "123-123-123-123"
PSEUDONYM_DICT = {f"i{count}": count for count in range(TABLE_SIZE)}
with open(f"{WORKING_DIR}/metadata.json", encoding="utf-8") as file:
    METADATA = Metadata(**json.load(file))
with open(
    f"{WORKING_DIR}/metadata_invalid_unit_type.json", encoding="utf-8"
) as file:
    INVALID_METADATA = Metadata(**json.load(file))
with open(
    f"{WORKING_DIR}/metadata_psudonymize_unit_id_and_value.json",
    encoding="utf-8",
) as file:
    PSEUDONYMIZE_UNIT_ID_AND_VALUE_METADATA = Metadata(**json.load(file))
with open(
    f"{WORKING_DIR}/metadata_psudonymize_value.json",
    encoding="utf-8",
) as file:
    PSEUDONYMIZE_ONLY_VALUE_METADATA = Metadata(**json.load(file))


@pytest.fixture(autouse=True)
def set_working_dir(monkeypatch):
    monkeypatch.setenv("WORKING_DIR", str(WORKING_DIR))


def setup_function():
    if os.path.isdir(f"{WORKING_DIR}_backup"):
        shutil.rmtree(f"{WORKING_DIR}_backup")

    shutil.copytree(WORKING_DIR, f"{WORKING_DIR}_backup")

    parquet.write_table(INPUT_TABLE, INPUT_PARQUET_PATH)
    parquet.write_table(INPUT_TABLE_START_YEAR, INPUT_PARQUET_PATH_START_YEAR)


def teardown_function():
    shutil.rmtree(WORKING_DIR)
    shutil.move(f"{WORKING_DIR}_backup", WORKING_DIR)


def test_pseudonymizer(mocker):
    mocker.patch.object(
        pseudonym_service, "pseudonymize", return_value=PSEUDONYM_DICT
    )
    assert str(
        dataset_pseudonymizer.run(INPUT_PARQUET_PATH, METADATA, JOB_ID)
    ) == str(OUTPUT_PARQUET_PATH)

    actual_table = dataset.dataset(OUTPUT_PARQUET_PATH).to_table()
    _validate_content(actual_table, EXPECTED_TABLE)

    expected_types = {
        "unit_id": "int64",
        "value": "string",
        "start_epoch_days": "int16",
        "stop_epoch_days": "int16",
    }

    # Checking the parquet schema is what we expect
    _verify_parquet_schema(OUTPUT_PARQUET_PATH, expected_types)


def test_pseudonymizer_unit_id_and_value(mocker):
    mocker.patch.object(
        pseudonym_service, "pseudonymize", return_value=PSEUDONYM_DICT
    )

    # Pseudonymize
    pseudonymized_output_path = dataset_pseudonymizer.run(
        INPUT_PARQUET_PATH,
        PSEUDONYMIZE_UNIT_ID_AND_VALUE_METADATA,
        JOB_ID,
    )
    actual_table = dataset.dataset(pseudonymized_output_path).to_table()
    _validate_content(actual_table, EXPECTED_TABLE_WITH_BOTH_PSEUDONYMIZED)

    expected_types = {
        "unit_id": "int64",
        "value": "int64",
        "start_epoch_days": "int16",
        "stop_epoch_days": "int16",
    }

    # Checking the parquet schema is what we expect
    _verify_parquet_schema(OUTPUT_PARQUET_PATH, expected_types)


def test_pseudonymizer_only_value(mocker):
    mocker.patch.object(
        pseudonym_service, "pseudonymize", return_value=PSEUDONYM_DICT
    )

    # Pseudonymize
    pseudonymized_output_path = dataset_pseudonymizer.run(
        INPUT_PARQUET_PATH,
        PSEUDONYMIZE_ONLY_VALUE_METADATA,
        JOB_ID,
    )
    actual_table = dataset.dataset(pseudonymized_output_path).to_table()
    _validate_content(
        actual_table, EXPECTED_TABLE_WITH_ONLY_VALUE_PSEUDONYMIZED
    )

    expected_types = {
        "unit_id": "string",
        "value": "int64",
        "start_epoch_days": "int16",
        "stop_epoch_days": "int16",
    }

    # Checking the parquet schema is what we expect
    _verify_parquet_schema(OUTPUT_PARQUET_PATH, expected_types)


def test_pseudonymizer_start_year(mocker):
    mocker.patch.object(
        pseudonym_service, "pseudonymize", return_value=PSEUDONYM_DICT
    )
    assert str(
        dataset_pseudonymizer.run(
            INPUT_PARQUET_PATH_START_YEAR, METADATA, JOB_ID
        )
    ) == str(OUTPUT_PARQUET_PATH_START_YEAR)

    actual_table = dataset.dataset(OUTPUT_PARQUET_PATH_START_YEAR).to_table()
    _validate_content(actual_table, EXPECTED_TABLE_START_YEAR)

    expected_types = {
        "unit_id": "int64",
        "value": "string",
        "start_year": "string",
        "start_epoch_days": "int16",
        "stop_epoch_days": "int16",
    }

    # Checking the parquet schema is what we expect
    _verify_parquet_schema(OUTPUT_PARQUET_PATH_START_YEAR, expected_types)


def test_pseudonymizer_adapter_failure():
    with pytest.raises(BuilderStepError) as e:
        dataset_pseudonymizer.run(INPUT_PARQUET_PATH, METADATA, JOB_ID)
    assert "Failed to pseudonymize dataset" == str(e.value)


def test_pseudonymizer_invalid_unit_id_type():
    with pytest.raises(BuilderStepError) as e:
        dataset_pseudonymizer.run(INPUT_PARQUET_PATH, INVALID_METADATA, JOB_ID)
    assert f"Failed to pseudonymize, UnregisteredUnitType: {str(e)}"


# In Parquet, the physical type refers to how the data is stored.
# INT32 and INT16 are both physically stored as INT32 in Parquet files.
# The logical type can provide additional context about the data.
# For example, it can tell you that a certain INT32 physical column is to be
# interpreted as a INT16 logical type.
def _verify_parquet_schema(parquet_file_path, expected_types):
    """
    Checks the logical type of each column in the parquet file to make sure
    they are what we expect.
    """
    table = parquet.read_table(parquet_file_path)
    schema = table.schema

    for column_name, expected_type in expected_types.items():
        actual_type = schema.field(column_name).type
        assert str(actual_type) == expected_type


def _validate_content(actual_table, expected_table):
    """
    Validate the content of the actual table against the expected table.
    """

    for column_name in expected_table.schema.names:
        assert (
            actual_table[column_name].to_pylist()
            == expected_table[column_name].to_pylist()
        )
