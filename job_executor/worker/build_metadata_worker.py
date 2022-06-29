import logging

from job_executor.exception.exception import (
    BuilderStepError,
    HttpResponseError
)
from job_executor.adapter import job_service_adapter
from job_executor.worker.steps import (
    dataset_validator,
    dataset_transformer
)

logger = logging.getLogger()

def run_worker(job_id: str, dataset_name: str):
    try:
        job_service_adapter.update_job_status(job_id, 'validating')
        metadata_file_path = dataset_validator.run_for_metadata(dataset_name)
        job_service_adapter.update_job_status(job_id, 'transforming')
        dataset_transformer.run(metadata_file_path)
        job_service_adapter.update_job_status(job_id, 'built')
    except BuilderStepError as e:
        pass
        job_service_adapter.update_job_status(job_id, 'failed', log=str(e))
    except HttpResponseError as e:
        logger.error(str(e))
        job_service_adapter.update_job_status(
            job_id, 'failed',
            log='Failed due to communication errors in platform'
        )
    except Exception:
        pass
        job_service_adapter.update_job_status(
            job_id, 'failed', log='Unexpected exception when building dataset'
        )
