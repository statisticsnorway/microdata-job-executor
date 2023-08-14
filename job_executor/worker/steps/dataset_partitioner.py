from pathlib import Path

from pyarrow import dataset
from pyarrow import parquet


def run(data_path: Path, dataset_name: str):
    table = dataset.dataset(data_path).to_table()

    output_dir = data_path.parent / f"{dataset_name}__DRAFT"

    # Write dataset partitioned by 'start_year'
    parquet.write_to_dataset(
        table, root_path=output_dir, partition_cols=["start_year"]
    )
