import os
import shutil
from pathlib import Path

import pytest
import pyarrow
from pyarrow import parquet

from job_executor.worker.steps import dataset_partitioner
from job_executor.exception import BuilderStepError


WORKING_DIR = Path("tests/resources/worker/steps/partitioner")
JOB_ID_PARTITIONER = "321-321-321-321"

TABLE_SIZE = 3000
UNIT_ID_INPUT = [f"i{count}" for count in range(TABLE_SIZE)]
YEARS = ["2020"] * 1000 + ["2021"] * 1000 + ["2022"] * 1000
START_EPOCH_DAYS = [18262] * 1000 + [18628] * 1000 + [18993] * 1000
INPUT_TABLE = pyarrow.Table.from_pydict(
    {
        "unit_id": UNIT_ID_INPUT,
        "value": UNIT_ID_INPUT,
        "start_year": YEARS,
        "start_epoch_days": START_EPOCH_DAYS,
        "stop_epoch_days": [day + 1 for day in START_EPOCH_DAYS],
    }
)


@pytest.fixture(autouse=True)
def set_working_dir_partitioner(monkeypatch):
    monkeypatch.setenv("WORKING_DIR", str(WORKING_DIR))


def setup_function():
    if os.path.isdir(f"{WORKING_DIR}_backup"):
        shutil.rmtree(f"{WORKING_DIR}_backup")

    if not os.path.isdir(WORKING_DIR):
        os.mkdir(WORKING_DIR)

    shutil.copytree(WORKING_DIR, f"{WORKING_DIR}_backup")

    parquet.write_table(
        INPUT_TABLE, WORKING_DIR / "input_pseudonymized.parquet"
    )


def teardown_function():
    shutil.rmtree(WORKING_DIR)
    shutil.move(f"{WORKING_DIR}_backup", WORKING_DIR)


def test_partitioner(mocker):
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

        # 3. Load the parquet file and check its length
        table_from_partition = pyarrow.parquet.read_table(files[0])
        assert len(table_from_partition) == 1000  # Each year has 1000 records

        # 4. column names are the same except for the partition column
        assert table_from_partition.column_names == [
            "unit_id",
            "value",
            "start_epoch_days",
            "stop_epoch_days",
        ]

        # 5. Check if start_epoch_days is within the correct start_year
        start_epochs = table_from_partition.column(
            "start_epoch_days"
        ).to_pylist()

        if year == 2020:
            assert all(18262 <= epoch < 18628 for epoch in start_epochs)
        elif year == 2021:
            assert all(18628 <= epoch < 18993 for epoch in start_epochs)
        elif year == 2022:
            assert all(18993 <= epoch for epoch in start_epochs)
        else:
            raise AssertionError(f"Unexpected year: {year}")


def test_partitioner_missing_start_year(mocker):
    # remove start_year column from input table
    input_table = INPUT_TABLE.remove_column(2)
    parquet.write_table(
        input_table, WORKING_DIR / "input_pseudonymized.parquet"
    )

    dataset_path = Path(f"{WORKING_DIR}/input_pseudonymized.parquet")
    with pytest.raises(BuilderStepError):
        dataset_partitioner.run(dataset_path, "input")
