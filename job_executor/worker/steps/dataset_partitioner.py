from pathlib import Path

from pyarrow import dataset


def run(data_path: Path):
    # TODO:
    # Partitioning logic:
    # Look up how it is done in old dataset_converter.py step
    # We know that this is called only for datasets with
    # temporality_type in ["STATUS", "ACCUMULATED"]
    dataset.dataset(data_path).to_table()
    ...
