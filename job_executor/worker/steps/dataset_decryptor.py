from pathlib import Path
from job_executor.config import environment
from job_executor.adapter.local_storage import INPUT_DIR

from microdata_tools import unpackage_dataset


RSA_KEYS_DIRECTORY = Path(environment.get("RSA_KEYS_DIRECTORY"))


def unpackage(dataset_name: str):
    file_path = Path(f"{INPUT_DIR}/{dataset_name}.tar")
    output_dir = Path(f"{INPUT_DIR}/decrypted")
    unpackage_dataset(
        file_path,
        RSA_KEYS_DIRECTORY,
        output_dir,
        Path(f"{INPUT_DIR}/archive"),
    )
