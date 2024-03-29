import logging
from pathlib import Path
from typing import Tuple

from microdata_tools import validate_dataset, validate_metadata

from job_executor.exception import BuilderStepError
from job_executor.config import environment


logger = logging.getLogger()
WORKING_DIR = Path(environment.get("WORKING_DIR"))


def run_for_dataset(dataset_name: str) -> Tuple[Path, Path]:
    """
    Validates the data and metadata file in the working_directory
    using the microdata-tools.

    Returns path to validated data and metadata in working directory.
    """
    validation_errors = []
    try:
        validation_errors = validate_dataset(
            dataset_name,
            input_directory=str(WORKING_DIR),
            working_directory=str(WORKING_DIR),
            keep_temporary_files=True,
        )

    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        raise BuilderStepError(
            "Unexpected error when validating dataset"
        ) from e
    if validation_errors:
        logger.error("Dataset failed validation with microdata-tools")
        raise BuilderStepError(
            "Failed to validate dataset. "
            "Resolve errors with the microdata-tools validator before uploading. "
            "Remember to update to the latest version of microdata-tools. "
        )

    return (
        WORKING_DIR / f"{dataset_name}.parquet",
        WORKING_DIR / f"{dataset_name}.json",
    )


def run_for_metadata(dataset_name: str) -> Path:
    """
    Validates the metadata in the given file with the
    microdata-tools schema and moves file to working directory.

    Returns path to validated metadata in working directory.
    """
    validation_errors = []
    try:
        validation_errors = validate_metadata(
            dataset_name,
            input_directory=WORKING_DIR,
            working_directory=WORKING_DIR,
            keep_temporary_files=True,
        )

    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        raise BuilderStepError(
            "Unexpected error when validating metadata"
        ) from e
    if validation_errors:
        logger.error("Dataset failed validation with microdata-tools")
        raise BuilderStepError(
            "Failed to validate metadata. "
            "Resolve errors with the microdata-tools validator before uploading."
            "Remember to update to the latest version of microdata-tools. "
        )
    return WORKING_DIR / f"{dataset_name}.json"
