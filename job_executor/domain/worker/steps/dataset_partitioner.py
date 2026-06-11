import logging
from pathlib import Path

import pyarrow
from pyarrow import dataset

from job_executor.common.exceptions import BuilderStepError

logger = logging.getLogger()


def run(
    data_path: Path,
    dataset_name: str,
    encryption_config: dataset.ParquetEncryptionConfig,
) -> None:
    """
    Partitions the given dataset by the 'start_year' column.

    This function reads the dataset from the specified path and writes
    a partitioned version of it based on the 'start_year' column to a
    new directory named '<dataset_name>__DRAFT' at the parent level of
    the given path.

    Raises:
    - ValueError: If the 'start_year' column is not found in the dataset.
    - BuilderStepError: If there's an error during the partitioning process.
    """
    try:
        ds = dataset.dataset(data_path)

        # Check if "start_year" column exists in the schema without loading
        # the entire dataset into memory
        if "start_year" not in ds.schema.names:
            raise BuilderStepError(
                "Column 'start_year' not found in the dataset"
            )

        table = ds.to_table()

        output_dir = data_path.parent / f"{dataset_name}__DRAFT"

        parquet_format = dataset.ParquetFileFormat()
        write_options = parquet_format.make_write_options(
            encryption_config=encryption_config
        )
        dataset.write_dataset(
            table,
            output_dir,
            format=parquet_format,
            file_options=write_options,
            partitioning=dataset.partitioning(
                pyarrow.schema([("start_year", pyarrow.string())]),
                flavor="hive",
            ),
            existing_data_behavior="delete_matching",
        )
    except Exception as e:
        logger.error(f"Error during partitioning: {str(e)}")
        raise BuilderStepError("Failed to partition dataset") from e
