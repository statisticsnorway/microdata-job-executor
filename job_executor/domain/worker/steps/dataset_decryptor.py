from pathlib import Path

from microdata_tools import unpackage_dataset

from job_executor.adapter.local_storage import INPUT_DIR, WORKING_DIR
from job_executor.config import environment

RSA_KEYS_DIRECTORY = Path(environment.rsa_keys_directory)


def unpackage(dataset_name: str) -> None:
    file_path = Path(f"{INPUT_DIR}/archive/{dataset_name}.tar")
    unpackage_dataset(
        packaged_file_path=file_path,
        rsa_keys_dir=RSA_KEYS_DIRECTORY,
        output_dir=WORKING_DIR,
    )
