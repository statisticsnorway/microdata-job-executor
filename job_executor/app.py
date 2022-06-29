import time
import logging
from multiprocessing import Process
from typing import List
from job_executor.exception.exception import UnknownOperationException
from job_executor.worker import (
    build_dataset_worker,
    build_metadata_worker
)
from job_executor.domain import datastore
from job_executor.model.job import Job
from job_executor.adapter import job_service_adapter
from job_executor.config import environment


NUMBER_OF_WORKERS = environment.get('NUMBER_OF_WORKERS')
workers: List[Process] = []

logger = logging.getLogger()


def main():
    while True:
        time.sleep(5)
        workers = [worker for worker in workers if worker.is_alive()]
        queued_worker_jobs = job_service_adapter.get_jobs(
            status='queued',
            operations=['PATCH_METADATA', 'ADD', 'CHANGE_DATA']
        )
        for job in queued_worker_jobs:
            if len(workers) < NUMBER_OF_WORKERS:
                _handle_worker_job(job)

        built_jobs = job_service_adapter.get_jobs(
            status='built'
        )
        queued_manager_jobs = job_service_adapter.get_jobs(
            status='queued',
            operations=['SET_STATUS', 'BUMP', 'DELETE_DRAFT', 'REMOVE']
        )
        for job in built_jobs + queued_manager_jobs:
            try:
                _handle_manager_job(job)
            except Exception as e:
                job_service_adapter.update_status(
                    job.jobId, 'failed',
                    log=str(e)
                )


def _handle_worker_job(job: Job):
    dataset_name = job.datasetName
    job_id = job.id
    operation = job.operation
    if operation in ['ADD', 'CHANGE_DATA']:
        worker = Process(
            target=build_dataset_worker.run_worker,
            args=(dataset_name, job_id,)
        )
        workers.append(worker)
        worker.start()
    elif operation == 'PATCH_METADATA':
        worker = Process(
            target=build_metadata_worker.run_worker,
            args=(dataset_name, job_id,)
        )
        workers.append(worker)
        worker.start()
    else:
        logger.error(f'Unknown operation "{operation}"')
        job_service_adapter.update_job_status(
            job_id, 'failed',
            log=f'Unknown operation type {operation}'
        )
    

def _handle_manager_job(job: Job):
    operation = job.operation
    if operation == 'BUMP':
        datastore.bump_version(
            job.parameters.bumpManifesto,
            job.parameters.description
        )
    elif operation == 'PATCH_METADATA':
        datastore.patch_metadata(job.parameters.datasetName)
    elif operation == 'SET_STATUS':
        datastore.set_draft_release_status(
            job.parameters.datasetName, job.parameters.releaseStatus
        )
    elif operation == 'ADD':
        datastore.change_data(job.parameters.datasetName)
    elif operation == 'CHANGE_DATA':
        datastore.add(job.parameters.datasetName)
    elif operation == 'REMOVE':
        datastore.remove(job.parameters.datasetName)
    elif operation == 'DELETE_DRAFT':
        datastore.delete_draft(job.parameters.datasetName)
    else:
        raise UnknownOperationException(f'Unknown operation {operation}')


if __name__ == '__main__':
    logger.info('Initiating dataset-builder')
    logger.info('Started polling for jobs')
    main()
