import logging

from job_executor.config import environment
from job_executor.adapter import job_service, local_storage
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


logger = logging.getLogger()
WORKING_DIR = environment.get('WORKING_DIR')


def run_worker(job_id: str, dataset_name: str):
    try:
        job_service.update_job_status(job_id, 'validating')
        data_file_path, metadata_file_path = dataset_validator.run_for_dataset(
            dataset_name
        )
        input_metadata = local_storage.get_working_dir_input_metadata(
            dataset_name
        )
        description = input_metadata['dataRevision']['description'][0]['value']
        job_service.update_description(job_id, description)

        job_service.update_job_status(job_id, 'transforming')
        transformed_metadata = dataset_transformer.run(metadata_file_path)
        temporality_type = transformed_metadata.temporality
        temporal_coverage = transformed_metadata.temporal_coverage.dict()
        data_type = transformed_metadata.measure_variable.data_type

        job_service.update_job_status(job_id, 'pseudonymizing')
        pseudonymized_data_path = dataset_pseudonymizer.run(
            data_file_path, transformed_metadata, job_id
        )
        job_service.update_job_status(job_id, 'enriching')
        enriched_data_path = dataset_enricher.run(
            pseudonymized_data_path, temporal_coverage, data_type
        )

        job_service.update_job_status(job_id, 'converting')
        dataset_converter.run(
            dataset_name, enriched_data_path, temporality_type, data_type
        )
        job_service.update_job_status(job_id, 'built')
        logger.info('Dataset built sucessfully')

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
