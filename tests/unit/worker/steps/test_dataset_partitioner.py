import os
import shutil
from pathlib import Path

import pytest
import pyarrow
from pyarrow import parquet

from job_executor.worker.steps import dataset_partitioner


WORKING_DIR = Path("tests/resources/worker/steps/partitioner")
JOB_ID_PARTITIONER = "321-321-321-321"

TABLE_SIZE = 3000
UNIT_ID_INPUT = [f"i{count}" for count in range(TABLE_SIZE)]
YEARS = [2020] * 1000 + [2021] * 1000 + [2022] * 1000
INPUT_TABLE = pyarrow.Table.from_pydict(
    {
        "unit_id": UNIT_ID_INPUT,
        "value": UNIT_ID_INPUT,
        "start_year": YEARS,
        "start_epoch_days": [18200] * TABLE_SIZE,
        "stop_epoch_days": [18201] * TABLE_SIZE,
    }
)


# with open(
#     f"{WORKING_DIR}/metadata_partition.json", encoding="utf-8"
# ) as file:
#     METADATA_PARTITIONER = Metadata(**json.load(file))


@pytest.fixture(autouse=True)
def set_working_dir_partitioner(monkeypatch):
    monkeypatch.setenv("WORKING_DIR", str(WORKING_DIR))


def setup_function():
    if os.path.isdir(f"{WORKING_DIR}_backup"):
        shutil.rmtree(f"{WORKING_DIR}_backup")

    shutil.copytree(WORKING_DIR, f"{WORKING_DIR}_backup")

    parquet.write_table(
        INPUT_TABLE, WORKING_DIR / "input_pseudonymized.parquet"
    )


def teardown_function():
    shutil.rmtree(WORKING_DIR)
    shutil.move(f"{WORKING_DIR}_backup", WORKING_DIR)


def test_partitioner(mocker):
    # test files with temporality_type in ["STATUS", "ACCUMULATED"]:
    dataset_path = Path(f"{WORKING_DIR}/input_pseudonymized.parquet")
    dataset_partitioner.run(dataset_path, "input")
    output_dir = dataset_path.parent / "input__DRAFT"

    assert output_dir.exists()
    # Check each year's subdirectory
    for year in [2020, 2021, 2022]:
        partition_path = output_dir / f"start_year={year}"

        # 1. Verify the subdirectory exists
        assert partition_path.exists() and partition_path.is_dir()

        # 2. Verify each subdirectory contains exactly one file
        files = list(partition_path.iterdir())
        assert len(files) == 1

        # Load the parquet file and check its length
        table_from_partition = pyarrow.parquet.read_table(files[0])
        assert len(table_from_partition) == 1000  # Each year has 1000 records

        # column names are the same


def test_partitioner_no_partions(mocker):
    # test files without  if temporality_type in ["STATUS", "ACCUMULATED"]:
    #
    assert True
    ...


def test_partitioner_failure():
    assert True
    ...
