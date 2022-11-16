import sys
import threading
import time
import logging
from typing import List
from multiprocessing import Process, Queue

import json_logging
from job_executor.exception import StartupException

from job_executor.model import Job, Datastore
from job_executor.domain import rollback
from job_executor.adapter import job_service
from job_executor.config import environment
from job_executor.config.log import CustomJSONLog
from job_executor.worker import (
    build_dataset_worker,
    build_metadata_worker
)


json_logging.init_non_web(custom_formatter=CustomJSONLog, enable_json=True)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))
NUMBER_OF_WORKERS = environment.get('NUMBER_OF_WORKERS')


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


def fix_interrupted_jobs():
    logger.info('Querying for interrupted jobs')
    in_progress_jobs = job_service.get_jobs(ignore_completed=True)
    queued_statuses = ['queued', 'built']
    interrupted_jobs = [
        job for job in in_progress_jobs
        if job.status not in queued_statuses
    ]
    logger.info(f'Found {len(interrupted_jobs)} interrupted jobs')

    for job in interrupted_jobs:
        job_operation = job.parameters.operation
        logger.info(
            f'{job.job_id}: Rolling back job with operation '
            f'"{job_operation}"'
        )
        if job_operation in ['ADD', 'CHANGE_DATA', 'PATCH_METADATA']:
            if job.status == 'importing':
                rollback.rollback_import_job(
                    job.job_id, job_operation, job.parameters.target
                )
                logger.info(
                    f'{job.job_id}: Rolled back importing of job with '
                    f'operation "{job_operation}". Retrying from status '
                    '"built"'
                )
                job_service.update_job_status(
                    job.job_id, 'built',
                    'Reset to built status will be due to '
                    'unexpected interruption'
                )
            else:
                rollback.rollback_worker_job(
                    job.job_id, job_operation, job.parameters.target
                )
                logger.info(
                    f'{job.job_id}: Setting status to "failed" for '
                    f'interrupted job'
                )
                job_service.update_job_status(
                    job.job_id, 'failed',
                    'Job was failed due to an unexpected interruption'
                )
        elif job_operation in ['SET_STATUS', 'DELETE_DRAFT', 'REMOVE']:
            logger.info(
                'Setting status to "queued" for '
                f'interrupted job with id {job.job_id}'
            )
            job_service.update_job_status(
                job.job_id, 'queued',
                'Retrying due to an unexpected interruption.'
            )
        elif job_operation == 'BUMP':
            try:
                rollback.rollback_bump(
                    job.job_id, job.parameters.bump_manifesto
                )
            except Exception as exc:
                error_message = f'Failed rollback for {job.job_id}'
                logger.exception(error_message, exc_info=exc)
                raise StartupException(error_message) from exc
            logger.info(
                'Setting status to "failed" for '
                f'interrupted job with id {job.job_id}'
            )
            job_service.update_job_status(
                job.job_id, 'failed',
                'Bump operation was interrupted and rolled back.'
            )
        else:
            log_message = (
                f'Unrecognized job operation {job_operation}'
                f'for job {job.job_id}'
            )
            logger.error(log_message)
            raise StartupException(log_message)


try:
    fix_interrupted_jobs()
    datastore = Datastore()
except Exception as e:
    logger.exception('Exception when initializing', exc_info=e)
    sys.exit("Exception when initializing")


def main():
    workers: List[Process] = []
    logging_queue = Queue()

    log_thread = threading.Thread(target=logger_thread, args=(logging_queue,))
    log_thread.start()

    try:
        while True:
            time.sleep(5)
            workers = [worker for worker in workers if worker.is_alive()]
            built_jobs = job_service.get_jobs(job_status='built')
            queued_manager_jobs = job_service.get_jobs(
                job_status='queued',
                operations=['SET_STATUS', 'BUMP', 'DELETE_DRAFT', 'REMOVE']
            )
            queued_worker_jobs = job_service.get_jobs(
                job_status='queued',
                operations=['PATCH_METADATA', 'ADD', 'CHANGE_DATA']
            )
            available_jobs = (
                len(queued_worker_jobs) +
                len(built_jobs) +
                len(queued_manager_jobs)
            )
            if available_jobs:
                logger.info(
                    f'Found {len(queued_worker_jobs)}/{len(built_jobs)}'
                    f'/{len(queued_manager_jobs)}'
                    f' (worker, built, queued manager jobs)'
                )
            for job in queued_worker_jobs:
                if len(workers) < NUMBER_OF_WORKERS:
                    _handle_worker_job(job, workers, logging_queue)

            for job in built_jobs + queued_manager_jobs:
                try:
                    _handle_manager_job(job)
                except Exception as exc:
                    logger.exception(
                        f'{job.job_id} failed and could not roll back',
                        exc_info=exc
                    )
                    raise exc
    except Exception as exc:
        logger.exception('Service stopped by exception', exc_info=exc)
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
    job_id = job.job_id
    operation = job.parameters.operation
    if operation == 'BUMP':
        datastore.bump_version(
            job_id,
            job.parameters.bump_manifesto,
            job.parameters.description
        )
    elif operation == 'PATCH_METADATA':
        datastore.patch_metadata(
            job_id,
            job.parameters.target,
            job.parameters.description
        )
    elif operation == 'SET_STATUS':
        datastore.set_draft_release_status(
            job_id,
            job.parameters.target,
            job.parameters.release_status
        )
    elif operation == 'ADD':
        datastore.add(
            job_id,
            job.parameters.target,
            job.parameters.description
        )
    elif operation == 'CHANGE_DATA':
        datastore.change_data(
            job_id,
            job.parameters.target,
            job.parameters.description
        )
    elif operation == 'REMOVE':
        datastore.remove(
            job_id,
            job.parameters.target,
            job.parameters.description
        )
    elif operation == 'DELETE_DRAFT':
        datastore.delete_draft(job_id, job.parameters.target)
    else:
        job_service.update_job_status(
            job.job_id, 'failed',
            log='Unknown operation for job'
        )


if __name__ == '__main__':
    logger.info('Polling for jobs...')
    main()
