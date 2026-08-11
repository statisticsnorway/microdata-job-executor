from pathlib import Path

import pyarrow as pa
import pytest
from pyarrow import dataset, parquet

from job_executor.common.exceptions import BuilderStepError
from job_executor.domain.worker.steps import dataset_encryptor
from tests.common.encrypted_parquet import decryption_file_format


def test_encryptor_writes_encrypted_parquet(tmp_path: Path):
    input_path = tmp_path / "input.parquet"
    output_path = tmp_path / "output.parquet"
    table = pa.table(
        {
            "unit_id": pa.array(["1", "2", "3"]),
            "value": pa.array(["10", "20", "30"]),
            "start_epoch_days": pa.array([100, 200, 300], type=pa.int16()),
            "stop_epoch_days": pa.array([101, 201, 301], type=pa.int16()),
        }
    )
    parquet.write_table(table, input_path)

    dataset_encryptor.run(input_path, output_path)

    assert output_path.exists()
    with pytest.raises(Exception):
        dataset.dataset(output_path).to_table()

    decrypted_table = dataset.dataset(
        output_path,
        format=decryption_file_format(),
    ).to_table()
    assert decrypted_table.num_rows == table.num_rows
    assert decrypted_table.column_names == table.column_names


def test_encryptor_raises_builder_error_on_missing_input(tmp_path: Path):
    with pytest.raises(BuilderStepError, match="Failed to encrypt dataset"):
        dataset_encryptor.run(
            tmp_path / "does_not_exist.parquet",
            tmp_path / "output.parquet",
        )
