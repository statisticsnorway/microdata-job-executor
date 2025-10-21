import logging
from pathlib import Path

from microdata_tools import validate_dataset, validate_metadata

from job_executor.common.exceptions import BuilderStepError

logger = logging.getLogger()


def run_for_dataset(
    dataset_name: str, working_directory: Path
) -> tuple[str, str]:
    """
    Validates the data and metadata file in the working_directory
    using the microdata-tools.

    Returns file name of validated data and metadata in working directory.
    """
    validation_errors = []
    try:
        validation_errors = validate_dataset(
            dataset_name,
            input_directory=str(working_directory),
            working_directory=str(working_directory),
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
            "Resolve errors with the microdata-tools validator before "
            "uploading. Remember to update to the latest version of "
            "microdata-tools. "
        )

    return (
        f"{dataset_name}.parquet",
        f"{dataset_name}.json",
    )


def run_for_metadata(dataset_name: str, working_directory: Path) -> str:
    """
    Validates the metadata in the given file with the
    microdata-tools schema and moves file to working directory.

    Returns file name of the validated metadata in working directory.
    """
    validation_errors = []
    try:
        validation_errors = validate_metadata(
            dataset_name,
            input_directory=str(working_directory),
            working_directory=str(working_directory),
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
            "Resolve errors with the microdata-tools validator before "
            "uploading. Remember to update to the latest version of "
            "microdata-tools. "
        )
    return f"{dataset_name}.json"
