import logging
from multiprocessing import Queue
from time import perf_counter

from job_executor.adapter import job_service, local_storage
from job_executor.config import environment
from job_executor.config.log import configure_worker_logger
from job_executor.exception import (
    BuilderStepError,
    HttpResponseError
)
from job_executor.worker.steps import (
    dataset_validator,
    dataset_converter,
    dataset_transformer,
    dataset_enricher,
    dataset_pseudonymizer
)

WORKING_DIR = environment.get('WORKING_DIR')


def run_worker(job_id: str, dataset_name: str, logging_queue: Queue):
    start = perf_counter()
    logger = logging.getLogger()
    consumed_files: list[str] = []
    
    try:
        configure_worker_logger(logging_queue, job_id)
        logger.info(
            f'Starting dataset worker for dataset '
            f'{dataset_name} and job {job_id}'
        )

        job_service.update_job_status(job_id, 'validating')
        data_file_path, metadata_file_path = dataset_validator.run_for_dataset(
            dataset_name
        )
        input_metadata = local_storage.get_working_dir_input_metadata(
            dataset_name
        )
        
        consumed_files.append(f'{WORKING_DIR}/{dataset_name}.db')
        
        description = input_metadata['dataRevision']['description'][0]['value']
        job_service.update_description(job_id, description)

        job_service.update_job_status(job_id, 'transforming')
        transformed_metadata = dataset_transformer.run(metadata_file_path)
        consumed_files.append(metadata_file_path)
        
        temporality_type = transformed_metadata.temporality
        temporal_coverage = transformed_metadata.temporal_coverage.dict()
        data_type = transformed_metadata.measure_variable.data_type

        job_service.update_job_status(job_id, 'pseudonymizing')
        pseudonymized_data_path = dataset_pseudonymizer.run(
            data_file_path, transformed_metadata, job_id
        )
        consumed_files.append(pseudonymized_data_path)
        consumed_files.append(data_file_path)
        job_service.update_job_status(job_id, 'enriching')
        enriched_data_path = dataset_enricher.run(
            pseudonymized_data_path, temporal_coverage, data_type
        )
        consumed_files.append(enriched_data_path)
        job_service.update_job_status(job_id, 'converting')
        dataset_converter.run(
            dataset_name, enriched_data_path, temporality_type, data_type
        )
        job_service.update_job_status(job_id, 'built')
        logger.info('Dataset built successfully')
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
        logger.exception(e)
        job_service.update_job_status(
            job_id, 'failed',
            log='Unexpected error when building dataset'
        )
    finally:
        local_storage.delete_files(consumed_files)
        delta = perf_counter() - start
        logger.info(f'Dataset worker for dataset '
                    f'{dataset_name} and job {job_id} '
                    f'done in {delta:.2f} seconds'
                    )
