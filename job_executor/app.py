import time
import logging
from multiprocessing import Process
from typing import List
from job_executor.exception import UnknownOperationException
from job_executor.worker import (
    build_dataset_worker,
    build_metadata_worker
)
from job_executor.model import Job, Datastore
from job_executor.adapter import job_service
from job_executor.config import environment


NUMBER_OF_WORKERS = environment.get('NUMBER_OF_WORKERS')
datastore = Datastore()
logger = logging.getLogger()


def main():
    workers: List[Process] = []

    while True:
        time.sleep(5)
        workers = [worker for worker in workers if worker.is_alive()]
        queued_worker_jobs = job_service.get_jobs(
            job_status='queued',
            operations=['PATCH_METADATA', 'ADD', 'CHANGE_DATA']
        )
        for job in queued_worker_jobs:
            if len(workers) < NUMBER_OF_WORKERS:
                _handle_worker_job(job, workers)

        built_jobs = job_service.get_jobs(
            job_status='built'
        )
        queued_manager_jobs = job_service.get_jobs(
            job_status='queued',
            operations=['SET_STATUS', 'BUMP', 'DELETE_DRAFT', 'REMOVE']
        )
        for job in built_jobs + queued_manager_jobs:
            try:
                _handle_manager_job(job)
                job_service.update_job_status(
                    job.job_id, 'completed'
                )
            except Exception as e:
                job_service.update_job_status(
                    job.job_id, 'failed',
                    log=str(e)
                )


def _handle_worker_job(job: Job, workers: List[Process]):
    dataset_name = job.parameters.target
    job_id = job.job_id
    operation = job.parameters.operation
    if operation in ['ADD', 'CHANGE_DATA']:
        worker = Process(
            target=build_dataset_worker.run_worker,
            args=(job_id, dataset_name,)
        )
        workers.append(worker)
        worker.start()
    elif operation == 'PATCH_METADATA':
        worker = Process(
            target=build_metadata_worker.run_worker,
            args=(job_id, dataset_name,)
        )
        workers.append(worker)
        worker.start()
    else:
        logger.error(f'Unknown operation "{operation}"')
        job_service.update_job_status(
            job_id, 'failed',
            log=f'Unknown operation type {operation}'
        )


def _handle_manager_job(job: Job):
    operation = job.parameters.operation
    if operation == 'BUMP':
        datastore.bump_version(
            job.parameters.bump_manifesto,
            job.parameters.description
        )
    elif operation == 'PATCH_METADATA':
        datastore.patch_metadata(
            job.parameters.target,
            job.parameters.description
        )
    elif operation == 'SET_STATUS':
        datastore.set_draft_release_status(
            job.parameters.target,
            job.parameters.release_status
        )
    elif operation == 'ADD':
        datastore.add(
            job.parameters.target,
            job.parameters.description
        )
    elif operation == 'CHANGE_DATA':
        datastore.change_data(
            job.parameters.target,
            job.parameters.description
        )
    elif operation == 'REMOVE':
        datastore.remove(
            job.parameters.target,
            job.parameters.description
        )
    elif operation == 'DELETE_DRAFT':
        datastore.delete_draft(job.parameters.target)
    else:
        raise UnknownOperationException(f'Unknown operation {operation}')


if __name__ == '__main__':
    logger.info('Initiating dataset-builder')
    logger.info('Started polling for jobs')
    main()
