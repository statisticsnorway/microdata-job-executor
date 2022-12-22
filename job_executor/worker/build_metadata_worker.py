import logging
from multiprocessing import Queue
from time import perf_counter

from job_executor.config.log import configure_worker_logger
from job_executor.exception import (
    BuilderStepError,
    HttpResponseError
)
from job_executor.config import environment
from job_executor.adapter import job_service, local_storage
from job_executor.worker.steps import (
    dataset_validator,
    dataset_transformer
)

WORKING_DIR = environment.get('WORKING_DIR')


def run_worker(job_id: str, dataset_name: str, logging_queue: Queue):
    start = perf_counter()
    logger = logging.getLogger()

    try:
        configure_worker_logger(logging_queue, job_id)
        logger.info(
            f'Starting metadata worker for dataset '
            f'{dataset_name} and job {job_id}'
        )

        job_service.update_job_status(job_id, 'validating')
        metadata_file_path = dataset_validator.run_for_metadata(dataset_name)
        input_metadata = local_storage.get_working_dir_input_metadata(
            dataset_name
        )
        local_storage.archive_input_files(dataset_name)
        description = input_metadata['dataRevision']['description'][0]['value']
        job_service.update_description(job_id, description)

        job_service.update_job_status(job_id, 'transforming')
        dataset_transformer.run(metadata_file_path)
        local_storage.delete_files([metadata_file_path])
        job_service.update_job_status(job_id, 'built')
    except BuilderStepError as e:
        logger.error(str(e))
        job_service.update_job_status(job_id, 'failed', log=str(e))
    except HttpResponseError as e:
        logger.error(str(e))
        job_service.update_job_status(
            job_id, 'failed',
            log='Failed due to communication errors in platform'
        )
    except Exception as e:
        logger.error(str(e))
        job_service.update_job_status(
            job_id, 'failed', log='Unexpected exception when building dataset'
        )
    finally:
        delta = perf_counter() - start
        logger.info(
            f'Metadata worker for dataset {dataset_name} and job {job_id}'
            f' done in {delta:.2f} seconds'
        )
