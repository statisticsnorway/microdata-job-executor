import shutil
import logging
from typing import Tuple
from microdata_validator import validate, validate_metadata

from job_executor.exception.exception import BuilderStepError
from job_executor.config import environment

logger = logging.getLogger()
INPUT_DIR = environment.get('INPUT_DIR')
WORKING_DIR = environment.get('WORKING_DIR')


def run_for_dataset(dataset_name: str) -> Tuple[str, str]:
    """
    Validates the data and metadata file in the input directory
    and moves the files to the working_directory using the microdata-validator.

    Returns path to validated data and metadata in working directory.
    """
    validation_errors = []
    try:
        validation_errors = validate(
            dataset_name,
            input_directory=INPUT_DIR,
            working_directory=WORKING_DIR,
            keep_temporary_files=True
        )
    except Exception as e:
        logger.error(f'Error during validation: {str(e)}')
        raise BuilderStepError(
            'Unexpected error when validating dataset'
        )
    if validation_errors:
        for error in validation_errors:
            logger.error(error)
        raise BuilderStepError(
            'Failed to validate dataset. '
            'Resolve errors with the microdata-validator before uploading.'
        )
    return (
        f'{WORKING_DIR}/{dataset_name}.csv',
        f'{WORKING_DIR}/{dataset_name}.json'
    )


def run_for_metadata(dataset_name: str):
    """
    Validates the metadata in the given file with the
    microdata-validator schema and moves file to working directory.

    Returns path to validated metadata in working directory.
    """
    metadata_input_directory_path = (
        f'{INPUT_DIR}/{dataset_name}/{dataset_name}.json'
    )
    metadata_working_directory_path = f'{WORKING_DIR}/{dataset_name}.json'
    validation_errors = []
    try:
        validation_errors = validate_metadata(
            metadata_input_directory_path
        )
    except Exception as e:
        logger.error(f'Error during validation: {str(e)}')
        raise BuilderStepError(
            'Unexpected error when validating metadata'
        )
    if validation_errors:
        for error in validation_errors:
            logger.error(error)
        raise BuilderStepError(
            'Failed to validate metadata. '
            'Resolve errors with the microdata-validator before uploading.'
        )
    shutil.copy(metadata_input_directory_path, metadata_working_directory_path)
    return metadata_working_directory_path