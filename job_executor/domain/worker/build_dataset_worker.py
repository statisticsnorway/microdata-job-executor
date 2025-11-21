import logging
import os
from multiprocessing import Queue
from time import perf_counter

from job_executor.adapter import datastore_api
from job_executor.adapter.datastore_api.models import JobStatus
from job_executor.adapter.fs import LocalStorageAdapter
from job_executor.common.exceptions import BuilderStepError, HttpResponseError
from job_executor.config.log import configure_worker_logger
from job_executor.domain.models import JobContext
from job_executor.domain.worker.steps import (
    dataset_decryptor,
    dataset_partitioner,
    dataset_pseudonymizer,
    dataset_transformer,
    dataset_validator,
)


def _clean_working_dir(
    local_storage: LocalStorageAdapter, dataset_name: str
) -> None:
    local_storage.working_dir.delete_metadata(dataset_name)
    local_storage.working_dir.delete_file(f"{dataset_name}.parquet")
    local_storage.working_dir.delete_file(
        f"{dataset_name}_pseudonymized.parquet"
    )
    local_storage.working_dir.delete_sub_directory(dataset_name)


def _dataset_requires_pseudonymization(input_metadata: dict) -> bool:
    return any(
        [
            input_metadata["identifierVariables"][0]
            .get("unitType", {})
            .get("requiresPseudonymization", False),
            input_metadata["measureVariables"][0]
            .get("unitType", {})
            .get("requiresPseudonymization", False),
        ]
    )


def run_worker(job_context: JobContext, logging_queue: Queue) -> None:
    start = perf_counter()
    logger = logging.getLogger()
    job_id = job_context.job.job_id
    local_storage = job_context.local_storage
    dataset_name = job_context.job.parameters.target
    try:
        configure_worker_logger(logging_queue, job_id)
        logger.info(
            f"Starting dataset worker for dataset "
            f"{dataset_name} and job {job_id}"
        )
        local_storage.input_dir.archive_importable(dataset_name)
        datastore_api.update_job_status(job_id, JobStatus.DECRYPTING)
        dataset_decryptor.unpackage(
            dataset_name,
            local_storage.input_dir.path,
            local_storage.working_dir.path,
            local_storage.datastore_dir.vault_dir,
        )
        datastore_api.update_job_status(job_id, JobStatus.VALIDATING)
        (data_file_name, _) = dataset_validator.run_for_dataset(
            dataset_name, local_storage.working_dir.path
        )
        input_metadata = local_storage.working_dir.get_input_metadata(
            dataset_name
        )
        description = input_metadata["dataRevision"]["description"][0]["value"]
        datastore_api.update_description(job_id, description)

        local_storage.working_dir.delete_sub_directory(dataset_name)
        datastore_api.update_job_status(job_id, JobStatus.TRANSFORMING)
        transformed_metadata = dataset_transformer.run(input_metadata)
        local_storage.working_dir.write_metadata(
            dataset_name, transformed_metadata
        )
        local_storage.working_dir.delete_input_metadata(dataset_name)

        temporality_type = transformed_metadata.temporality
        if _dataset_requires_pseudonymization(input_metadata):
            datastore_api.update_job_status(job_id, JobStatus.PSEUDONYMIZING)
            pre_pseudo_data_file_name = data_file_name
            data_file_name = dataset_pseudonymizer.run(
                local_storage.working_dir.path / data_file_name,
                transformed_metadata,
                job_id,
            )
            local_storage.working_dir.delete_file(pre_pseudo_data_file_name)

        datastore_api.update_job_status(job_id, JobStatus.PARTITIONING)
        if temporality_type in ["STATUS", "ACCUMULATED"]:
            dataset_partitioner.run(
                local_storage.working_dir.path / data_file_name, dataset_name
            )
            local_storage.working_dir.delete_file(data_file_name)
        else:
            target_path = (
                local_storage.working_dir.path
                / f"{dataset_name}__DRAFT.parquet"
            )
            os.rename(
                local_storage.working_dir.path / data_file_name,
                target_path,
            )
        local_storage.input_dir.delete_archived_importable(dataset_name)
        datastore_api.update_job_status(job_id, JobStatus.BUILT)
        logger.info("Dataset built successfully")
    except BuilderStepError as e:
        logger.error(str(e))
        _clean_working_dir(local_storage, dataset_name)
        datastore_api.update_job_status(job_id, JobStatus.FAILED, log=str(e))
    except HttpResponseError as e:
        logger.error(str(e))
        _clean_working_dir(local_storage, dataset_name)
        datastore_api.update_job_status(
            job_id,
            JobStatus.FAILED,
            log="Failed due to communication errors in platform",
        )
    except Exception as e:
        logger.exception(e)
        _clean_working_dir(local_storage, dataset_name)
        datastore_api.update_job_status(
            job_id,
            JobStatus.FAILED,
            log="Unexpected error when building dataset",
        )
    finally:
        delta = perf_counter() - start
        logger.info(
            f"Dataset worker for dataset "
            f"{dataset_name} and job {job_id} "
            f"done in {delta:.2f} seconds"
        )
