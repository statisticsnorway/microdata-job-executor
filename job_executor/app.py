import logging
import os
import sys
import time
from multiprocessing import Process, Queue
from pathlib import Path
from typing import Dict, List

from job_executor.adapter import job_service
from job_executor.config import environment
from job_executor.config.log import setup_logging, initialize_logging_thread
from job_executor.domain import rollback
from job_executor.exception import RollbackException, StartupException
from job_executor.model import Job, Datastore
from job_executor.model.worker import Worker
from job_executor.worker import build_dataset_worker, build_metadata_worker

logger = logging.getLogger()
setup_logging()

NUMBER_OF_WORKERS = int(environment.get("NUMBER_OF_WORKERS"))
DATASTORE_DIR = environment.get("DATASTORE_DIR")

datastore = None


def is_system_paused() -> bool:
    """Return True if the system is paused, otherwise False."""
    maintenance_status = job_service.get_maintenance_status()
    return maintenance_status.paused


def fix_interrupted_jobs():
    logger.info("Querying for interrupted jobs")
    in_progress_jobs = job_service.get_jobs(ignore_completed=True)
    queued_statuses = ["queued", "built"]
    interrupted_jobs = [
        job for job in in_progress_jobs if job.status not in queued_statuses
    ]
    logger.info(f"Found {len(interrupted_jobs)} interrupted jobs")
    try:
        for job in interrupted_jobs:
            fix_interrupted_job(job)
    except RollbackException as e:
        raise StartupException(e) from e


def fix_interrupted_job(job):
    job_operation = job.parameters.operation
    logger.info(
        f"{job.job_id}: Rolling back job with operation " f'"{job_operation}"'
    )
    if job_operation in ["ADD", "CHANGE", "PATCH_METADATA"]:
        if job.status == "importing":
            rollback.rollback_manager_phase_import_job(
                job.job_id, job_operation, job.parameters.target
            )
            logger.info(
                f"{job.job_id}: Rolled back importing of job with "
                f'operation "{job_operation}". Retrying from status '
                '"built"'
            )
            job_service.update_job_status(
                job.job_id,
                "built",
                "Reset to built status will be due to "
                "unexpected interruption",
            )
        else:
            rollback.rollback_worker_phase_import_job(
                job.job_id, job_operation, job.parameters.target
            )
            logger.info(
                f'{job.job_id}: Setting status to "failed" for '
                f"interrupted job"
            )
            job_service.update_job_status(
                job.job_id,
                "failed",
                "Job was failed due to an unexpected interruption",
            )
    elif job_operation in ["SET_STATUS", "DELETE_DRAFT", "REMOVE", "ROLLBACK_REMOVE"]:
        logger.info(
            'Setting status to "queued" for '
            f"interrupted job with id {job.job_id}"
        )
        job_service.update_job_status(
            job.job_id,
            "queued",
            "Retrying due to an unexpected interruption.",
        )
    elif job_operation == "BUMP":
        try:
            rollback.rollback_bump(job.job_id, job.parameters.bump_manifesto)
        except Exception as exc:
            error_message = f"Failed rollback for {job.job_id}"
            logger.exception(error_message, exc_info=exc)
            raise RollbackException(error_message) from exc
        logger.info(
            'Setting status to "failed" for '
            f"interrupted job with id {job.job_id}"
        )
        job_service.update_job_status(
            job.job_id,
            "failed",
            "Bump operation was interrupted and rolled back.",
        )
    else:
        log_message = (
            f"Unrecognized job operation {job_operation}"
            f"for job {job.job_id}"
        )
        logger.error(log_message)
        raise RollbackException(log_message)


def check_tmp_directory():
    tmp_dir = Path(DATASTORE_DIR) / "tmp"
    if os.path.isdir(tmp_dir):
        raise StartupException("tmp directory exists")


def query_for_jobs() -> Dict[str, List[Job]]:
    """
    Retrieves different types of jobs based on the system's state (paused or active).

    When the system is paused, only jobs with a 'built' status are fetched.
    In the active state, jobs are fetched based on their operations.

    Returns:
        Dict[str, List[Job]]: A dictionary structured as:
        - "built_jobs": Jobs that have already been built.
        - "queued_manager_jobs": Jobs in the queue with managerial operations.
        - "queued_worker_jobs": Jobs in the queue with worker operations.
    """

    job_dict = {
        "built_jobs": [],
        "queued_manager_jobs": [],
        "queued_worker_jobs": [],
    }

    job_mapping = {
        "built_jobs": {"status": "built", "operations": None},
        "queued_manager_jobs": {
            "status": "queued",
            "operations": [
                "SET_STATUS",
                "BUMP",
                "DELETE_DRAFT",
                "REMOVE",
                "ROLLBACK_REMOVE",
                "DELETE_ARCHIVE",
            ],
        },
        "queued_worker_jobs": {
            "status": "queued",
            "operations": ["PATCH_METADATA", "ADD", "CHANGE"],
        },
    }

    try:
        # If System is paused we only want to fetch built jobs
        if is_system_paused():
            logger.info("System is paused. Only fetching built jobs.")
            job_dict["built_jobs"] = job_service.get_jobs(
                job_status="built", operations=None
            )
        else:
            for job_type, criteria in job_mapping.items():
                job_dict[job_type] = job_service.get_jobs(
                    job_status=criteria["status"],
                    operations=criteria["operations"],
                )

        return job_dict

    except Exception as e:
        logger.exception("Exception when querying for jobs", exc_info=e)
        return job_dict


def initialize_app():
    global datastore
    try:
        fix_interrupted_jobs()
        check_tmp_directory()
        datastore = Datastore()
    except Exception as e:
        logger.exception("Exception when initializing", exc_info=e)
        sys.exit("Exception when initializing")


def main():
    initialize_app()
    logging_queue, log_thread = initialize_logging_thread()
    workers: List[Worker] = []

    try:
        while True:
            time.sleep(5)

            job_dict = query_for_jobs()
            queued_worker_jobs = job_dict["queued_worker_jobs"]
            built_jobs = job_dict["built_jobs"]
            queued_manager_jobs = job_dict["queued_manager_jobs"]

            dead_workers = [
                worker for worker in workers if not worker.is_alive()
            ]
            clean_up_after_dead_workers(dead_workers)

            workers = [worker for worker in workers if worker.is_alive()]

            available_jobs = (
                len(queued_worker_jobs)
                + len(built_jobs)
                + len(queued_manager_jobs)
            )
            if available_jobs:
                logger.info(
                    f"Found {len(queued_worker_jobs)}/{len(built_jobs)}"
                    f"/{len(queued_manager_jobs)}"
                    f" (worker, built, queued manager jobs)"
                )
            for job in queued_worker_jobs:
                if len(workers) < NUMBER_OF_WORKERS:
                    _handle_worker_job(job, workers, logging_queue)

            for job in built_jobs + queued_manager_jobs:
                try:
                    _handle_manager_job(job)
                except Exception as exc:
                    # All exceptions that occur during the handling of a job
                    # are resolved by rolling back. The exceptions that
                    # reach here are exceptions raised by the rollback.
                    logger.exception(
                        f"{job.job_id} failed and could not roll back",
                        exc_info=exc,
                    )
                    raise exc

    except Exception as exc:
        logger.exception("Service stopped by exception", exc_info=exc)
    finally:
        # Tell the logging thread to finish up
        logging_queue.put(None)
        log_thread.join()


def clean_up_after_dead_workers(dead_workers: List[Worker]) -> None:
    if len(dead_workers) > 0:
        in_progress_jobs = job_service.get_jobs(ignore_completed=True)
        for dead_worker in dead_workers:
            job = next(
                (
                    job
                    for job in in_progress_jobs
                    if dead_worker.job_id == job.job_id
                ),
                None,  # not found in in_progress => completed or failed
            )
            if job and job.status not in ["queued", "built"]:
                logger.info(f"Worker died and did not finish job {job.job_id}")
                fix_interrupted_job(job)


def _handle_worker_job(job: Job, workers: List[Worker], logging_queue: Queue):
    dataset_name = job.parameters.target
    job_id = job.job_id
    operation = job.parameters.operation
    if operation in ["ADD", "CHANGE"]:
        worker = Worker(
            process=Process(
                target=build_dataset_worker.run_worker,
                args=(
                    job_id,
                    dataset_name,
                    logging_queue,
                ),
            ),
            job_id=job_id,
        )
        workers.append(worker)
        job_service.update_job_status(job_id, "initiated")
        worker.start()
    elif operation == "PATCH_METADATA":
        worker = Worker(
            process=Process(
                target=build_metadata_worker.run_worker,
                args=(
                    job_id,
                    dataset_name,
                    logging_queue,
                ),
            ),
            job_id=job_id,
        )
        workers.append(worker)
        job_service.update_job_status(job_id, "initiated")
        worker.start()
    else:
        logger.error(f'Unknown operation "{operation}"')
        job_service.update_job_status(
            job_id, "failed", log=f"Unknown operation type {operation}"
        )


def _handle_manager_job(job: Job):
    job_id = job.job_id
    operation = job.parameters.operation
    if operation == "BUMP":
        datastore.bump_version(
            job_id, job.parameters.bump_manifesto, job.parameters.description
        )
    elif operation == "PATCH_METADATA":
        datastore.patch_metadata(
            job_id, job.parameters.target, job.parameters.description
        )
    elif operation == "SET_STATUS":
        datastore.set_draft_release_status(
            job_id, job.parameters.target, job.parameters.release_status
        )
    elif operation == "ADD":
        datastore.add(
            job_id, job.parameters.target, job.parameters.description
        )
    elif operation == "CHANGE":
        datastore.change(
            job_id, job.parameters.target, job.parameters.description
        )
    elif operation == "REMOVE":
        datastore.remove(
            job_id, job.parameters.target, job.parameters.description
        )
    elif operation == "ROLLBACK_REMOVE":
        datastore.delete_draft(job_id, job.parameters.target, rollback_remove=True)
    elif operation == "DELETE_DRAFT":
        datastore.delete_draft(job_id, job.parameters.target, rollback_remove=False)
    elif operation == "DELETE_ARCHIVE":
        datastore.delete_archived_input(job_id, job.parameters.target)
    else:
        job_service.update_job_status(
            job.job_id, "failed", log="Unknown operation for job"
        )


if __name__ == "__main__":
    logger.info("Polling for jobs...")
    main()
