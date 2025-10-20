import logging
from multiprocessing import Queue
from pathlib import Path
from time import perf_counter

from job_executor.adapter import datastore_api
from job_executor.adapter.datastore_api.models import JobStatus
from job_executor.adapter.fs import LocalStorageAdapter
from job_executor.common.exceptions import BuilderStepError, HttpResponseError
from job_executor.config import environment
from job_executor.config.log import configure_worker_logger
from job_executor.domain.worker.steps import (
    dataset_decryptor,
    dataset_transformer,
    dataset_validator,
)

DATASTORE_DIR = Path(environment.datastore_dir)


def _clean_working_dir(dataset_name: str) -> None:
    local_storage = LocalStorageAdapter(DATASTORE_DIR)  # TODO
    local_storage.working_dir.delete_metadata(dataset_name)
    local_storage.working_dir.delete_sub_directory(dataset_name)
    local_storage.working_dir.delete_file(dataset_name)


def run_worker(job_id: str, dataset_name: str, logging_queue: Queue) -> None:
    start = perf_counter()
    logger = logging.getLogger()

    try:
        configure_worker_logger(logging_queue, job_id)
        logger.info(
            f"Starting metadata worker for dataset "
            f"{dataset_name} and job {job_id}"
        )
        local_storage = LocalStorageAdapter(
            DATASTORE_DIR
        )  # TODO: get this info from manager
        local_storage.input_dir.archive_importable(dataset_name)
        datastore_api.update_job_status(job_id, JobStatus.DECRYPTING)
        dataset_decryptor.unpackage(
            dataset_name,
            local_storage.input_dir.path,
            local_storage.working_dir.path,
            local_storage.datastore_dir.vault_dir,
        )
        datastore_api.update_job_status(job_id, JobStatus.VALIDATING)
        dataset_validator.run_for_metadata(
            dataset_name,
            local_storage.working_dir.path,
        )
        input_metadata = local_storage.working_dir.get_input_metadata(
            dataset_name
        )

        description = input_metadata["dataRevision"]["description"][0]["value"]
        datastore_api.update_description(job_id, description)
        local_storage.working_dir.delete_sub_directory(dataset_name)

        datastore_api.update_job_status(job_id, JobStatus.TRANSFORMING)
        transformed_metadata_json = dataset_transformer.run(input_metadata)
        local_storage.working_dir.write_metadata(
            dataset_name, transformed_metadata_json
        )

        local_storage.working_dir.delete_input_metadata(dataset_name)
        local_storage.input_dir.delete_archived_importable(dataset_name)
        datastore_api.update_job_status(job_id, JobStatus.BUILT)
    except BuilderStepError as e:
        error_message = "Failed during building metdata"
        logger.exception(error_message, exc_info=e)
        _clean_working_dir(dataset_name)
        datastore_api.update_job_status(job_id, JobStatus.FAILED, log=str(e))
    except HttpResponseError as e:
        logger.exception(e)
        _clean_working_dir(dataset_name)
        datastore_api.update_job_status(
            job_id,
            JobStatus.FAILED,
            log="Failed due to communication errors in platform",
        )
    except Exception as e:
        error_message = "Unknown error when building metadata"
        logger.exception(error_message, exc_info=e)
        _clean_working_dir(dataset_name)
        datastore_api.update_job_status(
            job_id,
            JobStatus.FAILED,
            log="Unexpected exception when building dataset",
        )
    finally:
        delta = perf_counter() - start
        logger.info(
            f"Metadata worker for dataset {dataset_name} and job {job_id}"
            f" done in {delta:.2f} seconds"
        )
