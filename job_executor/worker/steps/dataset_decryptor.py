from pathlib import Path
from job_executor.config import environment
from job_executor.adapter.local_storage import INPUT_DIR, WORKING_DIR

from microdata_tools import unpackage_dataset


RSA_KEYS_DIRECTORY = Path(environment.get("RSA_KEYS_DIRECTORY"))


def unpackage(dataset_name: str):
    file_path = Path(f"{INPUT_DIR}/archive/{dataset_name}.tar")
    unpackage_dataset(
        packaged_file_path=file_path,
        rsa_keys_dir=RSA_KEYS_DIRECTORY,
        output_dir=WORKING_DIR,
    )
