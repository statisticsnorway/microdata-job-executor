import os
import json
import shutil
from pathlib import Path

import pytest
import pyarrow
from pyarrow import parquet, dataset

from job_executor.exception import BuilderStepError
from job_executor.model import Metadata
from job_executor.worker.steps import dataset_pseudonymizer
from job_executor.adapter import pseudonym_service


TABLE_SIZE = 1000
INPUT_TABLE = pyarrow.Table.from_pydict(
    {
        "unit_id": [f"i{count}" for count in range(TABLE_SIZE)],
        "value": ["a"] * TABLE_SIZE,
        "start_year": [2020] * TABLE_SIZE,
        "start_epoch_days": [18200] * TABLE_SIZE,
        "stop_epoch_days": [18201] * TABLE_SIZE,
    }
)

EXPECTED_TABLE = pyarrow.Table.from_pydict(
    {
        "unit_id": [f"{count}" for count in range(TABLE_SIZE)],
        "value": ["a"] * TABLE_SIZE,
        "start_year": [2020] * TABLE_SIZE,
        "start_epoch_days": [18200] * TABLE_SIZE,
        "stop_epoch_days": [18201] * TABLE_SIZE,
    }
)

WORKING_DIR = Path("tests/resources/worker/steps/pseudonymizer")
INPUT_PARQUET_PATH = WORKING_DIR / "input.parquet"
OUTPUT_PARQUET_PATH = WORKING_DIR / "input_pseudonymized.parquet"
parquet.write_table(INPUT_TABLE, INPUT_PARQUET_PATH)

JOB_ID = "123-123-123-123"
PSEUDONYM_DICT = {f"i{count}": f"{count}" for count in range(TABLE_SIZE)}
with open(f"{WORKING_DIR}/metadata.json", encoding="utf-8") as file:
    METADATA = Metadata(**json.load(file))
with open(
    f"{WORKING_DIR}/metadata_invalid_unit_type.json", encoding="utf-8"
) as file:
    INVALID_METADATA = Metadata(**json.load(file))


@pytest.fixture(autouse=True)
def set_working_dir(monkeypatch):
    monkeypatch.setenv("WORKING_DIR", str(WORKING_DIR))


def setup_function():
    if os.path.isdir(f"{WORKING_DIR}_backup"):
        shutil.rmtree(f"{WORKING_DIR}_backup")

    shutil.copytree(WORKING_DIR, f"{WORKING_DIR}_backup")


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
    column_names = [
        "unit_id",
        "value",
        "start_year",
        "start_epoch_days",
        "stop_epoch_days",
    ]
    for column_name in column_names:
        assert (
            actual_table[column_name].to_pylist()
            == EXPECTED_TABLE[column_name].to_pylist()
        )


def test_pseudonymizer_unit_id_and_value(mocker):
    # TODO: write this test
    # * metadata that contains a unit_id in both identifier and measure
    # * input dataset that makes sense with metadata
    # * expect identifiers to be switched with pseudonyms in both
    #   unit_id and value columns
    mocker.patch.object(
        pseudonym_service, "pseudonymize", return_value=PSEUDONYM_DICT
    )
    ...


def test_pseudonymizer_adapter_failure():
    with pytest.raises(BuilderStepError) as e:
        dataset_pseudonymizer.run(INPUT_PARQUET_PATH, METADATA, JOB_ID)
    assert "Failed to pseudonymize dataset" == str(e.value)


def test_pseudonymizer_invalid_unit_id_type():
    with pytest.raises(BuilderStepError) as e:
        dataset_pseudonymizer.run(INPUT_PARQUET_PATH, INVALID_METADATA, JOB_ID)
    assert "Failed to pseudonymize dataset" == str(e.value)
