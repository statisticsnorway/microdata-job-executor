import logging
from multiprocessing import Queue
import os
from pathlib import Path
from time import perf_counter

from job_executor.adapter import job_service, local_storage
from job_executor.config import environment
from job_executor.config.log import configure_worker_logger
from job_executor.exception import BuilderStepError, HttpResponseError
from job_executor.model.metadata import Metadata
from job_executor.worker.steps import (
    dataset_decryptor,
    dataset_validator,
    dataset_transformer,
    dataset_partitioner,
    dataset_pseudonymizer,
)

WORKING_DIR = Path(environment.get("WORKING_DIR"))


def _clean_working_dir(dataset_name: str):
    generated_files = [
        WORKING_DIR / f"{dataset_name}.json",
        WORKING_DIR / f"{dataset_name}.parquet",
        WORKING_DIR / f"{dataset_name}_pseudonymized.parquet",
        WORKING_DIR / dataset_name,
    ]
    for file_path in generated_files:
        if file_path.is_dir():
            local_storage.delete_working_dir_dir(file_path)
        else:
            local_storage.delete_working_dir_file(file_path)


def run_worker(job_id: str, dataset_name: str, logging_queue: Queue):
    start = perf_counter()
    logger = logging.getLogger()

    try:
        configure_worker_logger(logging_queue, job_id)
        logger.info(
            f"Starting dataset worker for dataset "
            f"{dataset_name} and job {job_id}"
        )

        local_storage.archive_input_files(dataset_name)

        job_service.update_job_status(job_id, "decrypting")
        dataset_decryptor.unpackage(dataset_name)

        job_service.update_job_status(job_id, "validating")
        (
            validated_data_file_path,
            metadata_file_path,
        ) = dataset_validator.run_for_dataset(dataset_name)
        input_metadata = local_storage.get_working_dir_input_metadata(
            dataset_name
        )
        description = input_metadata["dataRevision"]["description"][0]["value"]
        job_service.update_description(job_id, description)

        local_storage.delete_working_dir_dir(WORKING_DIR / f"{dataset_name}")
        job_service.update_job_status(job_id, "transforming")
        transformed_metadata_json = dataset_transformer.run(input_metadata)
        local_storage.write_working_dir_metadata(
            dataset_name, transformed_metadata_json
        )
        local_storage.delete_working_dir_file(metadata_file_path)
        transformed_metadata = Metadata(**transformed_metadata_json)

        temporality_type = transformed_metadata.temporality

        job_service.update_job_status(job_id, "pseudonymizing")
        pseudonymized_data_path = dataset_pseudonymizer.run(
            validated_data_file_path, transformed_metadata, job_id
        )
        local_storage.delete_working_dir_file(validated_data_file_path)

        job_service.update_job_status(job_id, "partitioning")
        if temporality_type in ["STATUS", "ACCUMULATED"]:
            dataset_partitioner.run(pseudonymized_data_path, dataset_name)
            local_storage.delete_working_dir_file(pseudonymized_data_path)
        else:
            target_path = os.path.join(
                os.path.dirname(pseudonymized_data_path),
                f"{dataset_name}__DRAFT.parquet",
            )
            os.rename(pseudonymized_data_path, target_path)

        local_storage.delete_archived_input(dataset_name)
        job_service.update_job_status(job_id, "built")
        logger.info("Dataset built successfully")
    except BuilderStepError as e:
        logger.error(str(e))
        _clean_working_dir(dataset_name)
        job_service.update_job_status(job_id, "failed", log=str(e))
    except HttpResponseError as e:
        logger.error(str(e))
        _clean_working_dir(dataset_name)
        job_service.update_job_status(
            job_id,
            "failed",
            log="Failed due to communication errors in platform",
        )
    except Exception as e:
        logger.exception(e)
        _clean_working_dir(dataset_name)
        job_service.update_job_status(
            job_id, "failed", log="Unexpected error when building dataset"
        )
    finally:
        delta = perf_counter() - start
        logger.info(
            f"Dataset worker for dataset "
            f"{dataset_name} and job {job_id} "
            f"done in {delta:.2f} seconds"
        )
