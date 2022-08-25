import sys
import threading
import time
import logging
from multiprocessing import Process, Queue
from typing import List

import json_logging

from job_executor.config.log import CustomJSONLog
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
json_logging.init_non_web(custom_formatter=CustomJSONLog, enable_json=True)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))


def logger_thread(logging_queue: Queue):
    """
    This method will run as a thread in the main process and will receive
    logs from workers via Queue.
    """
    while True:
        record = logging_queue.get()
        if record is None:
            break
        logger.handle(record)


def main():
    workers: List[Process] = []
    logging_queue = Queue()

    log_thread = threading.Thread(target=logger_thread, args=(logging_queue,))
    log_thread.start()

    try:
        while True:
            time.sleep(5)
            workers = [worker for worker in workers if worker.is_alive()]
            queued_worker_jobs = job_service.get_jobs(
                job_status='queued',
                operations=['PATCH_METADATA', 'ADD', 'CHANGE_DATA']
            )
            logger.info(f'Found {len(queued_worker_jobs)} worker jobs')
            for job in queued_worker_jobs:
                if len(workers) < NUMBER_OF_WORKERS:
                    _handle_worker_job(job, workers, logging_queue)

            built_jobs = job_service.get_jobs(
                job_status='built'
            )
            logger.info(f'Found {len(built_jobs)} built jobs')
            queued_manager_jobs = job_service.get_jobs(
                job_status='queued',
                operations=['SET_STATUS', 'BUMP', 'DELETE_DRAFT', 'REMOVE']
            )
            logger.info(f'Found {len(queued_manager_jobs)} queued manager jobs')
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
    except Exception as e:
        logger.exception(e)
    finally:
        # Tell the logging thread to finish up
        logging_queue.put(None)
        log_thread.join()


def _handle_worker_job(job: Job, workers: List[Process], logging_queue: Queue):
    dataset_name = job.parameters.target
    job_id = job.job_id
    operation = job.parameters.operation
    if operation in ['ADD', 'CHANGE_DATA']:
        worker = Process(
            target=build_dataset_worker.run_worker,
            args=(job_id, dataset_name, logging_queue,)
        )
        workers.append(worker)
        worker.start()
    elif operation == 'PATCH_METADATA':
        worker = Process(
            target=build_metadata_worker.run_worker,
            args=(job_id, dataset_name, logging_queue,)
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
    logger.info('Polling for jobs...')
    main()
