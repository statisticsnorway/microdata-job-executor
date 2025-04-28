import sys
import time
import logging
from multiprocessing import Process, Queue

from job_executor.adapter import job_service, local_storage
from job_executor.config import environment
from job_executor.config.log import initialize_logging_thread, setup_logging
from job_executor.domain import rollback
from job_executor.exception import StartupException

from job_executor.model import Datastore, Job
from job_executor.model.worker import Worker
from job_executor.worker import (
    build_dataset_worker,
    build_metadata_worker,
)
from job_executor.manager import Manager


logger = logging.getLogger()
setup_logging()


def initialize_app():
    try:
        rollback.fix_interrupted_jobs()
        if local_storage.temporary_backup_exists:
            raise StartupException("tmp directory exists")
    except Exception as e:
        logger.exception("Exception when initializing", exc_info=e)
        sys.exit("Exception when initializing")


def main():
    initialize_app()
    logging_queue, log_thread = initialize_logging_thread()
    manager = Manager(
        max_workers=int(environment.get("NUMBER_OF_WORKERS")),
        max_bytes_all_workers=(
            int(environment.get("MAX_GB_ALL_WORKERS"))
            * 1024**3  # Covert from GB to bytes
        ),
        datastore=Datastore(),
    )

    try:
        while True:
            time.sleep(5)

            job_dict = job_service.query_for_jobs()
            queued_worker_jobs = job_dict["queued_worker_jobs"]
            built_jobs = job_dict["built_jobs"]
            queued_manager_jobs = job_dict["queued_manager_jobs"]

            manager.clean_up_after_dead_workers()

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
                job_size = local_storage.get_input_tar_size_in_bytes(
                    job.parameters.target
                )
                if job_size == 0:
                    logger.error(
                        f"{job.job_id} Failed to get the size of the dataset."
                    )
                    job_service.update_job_status(
                        job.job_id,
                        "failed",
                        log="No such dataset available for import",
                    )
                    continue  # skip futher processing of this job
                if job_size > manager.max_bytes_all_workers:
                    logger.warning(
                        f"{job.job_id} Exceeded the maximum size for all workers."
                    )
                    job_service.update_job_status(
                        job.job_id,
                        "failed",
                        log="Dataset too large for import",
                    )
                    continue  # skip futher processing of this job
                if manager.can_spawn_new_worker(job_size):
                    _handle_worker_job(job, manager, job_size, logging_queue)

            for job in built_jobs + queued_manager_jobs:
                try:
                    manager.unregister_job(job.job_id)
                    _handle_manager_job(job, manager)
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


def _handle_worker_job(
    job: Job,
    manager: Manager,
    job_size: int,
    logging_queue: Queue,
):
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
            job_size=job_size,
        )
        manager.register_job(worker)
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
            job_size=job_size,
        )
        manager.register_job(worker)
        job_service.update_job_status(job_id, "initiated")
        worker.start()
    else:
        logger.error(f'Unknown operation "{operation}"')
        job_service.update_job_status(
            job_id, "failed", log=f"Unknown operation type {operation}"
        )


def _handle_manager_job(job: Job, manager: Manager):
    job_id = job.job_id
    operation = job.parameters.operation
    if operation == "BUMP":
        manager.datastore.bump_version(
            job_id, job.parameters.bump_manifesto, job.parameters.description
        )
    elif operation == "PATCH_METADATA":
        manager.datastore.patch_metadata(
            job_id, job.parameters.target, job.parameters.description
        )
    elif operation == "SET_STATUS":
        manager.datastore.set_draft_release_status(
            job_id, job.parameters.target, job.parameters.release_status
        )
    elif operation == "ADD":
        manager.datastore.add(
            job_id, job.parameters.target, job.parameters.description
        )
    elif operation == "CHANGE":
        manager.datastore.change(
            job_id, job.parameters.target, job.parameters.description
        )
    elif operation == "REMOVE":
        manager.datastore.remove(
            job_id, job.parameters.target, job.parameters.description
        )
    elif operation == "ROLLBACK_REMOVE":
        manager.datastore.delete_draft(
            job_id, job.parameters.target, rollback_remove=True
        )
    elif operation == "DELETE_DRAFT":
        manager.datastore.delete_draft(
            job_id, job.parameters.target, rollback_remove=False
        )
    elif operation == "DELETE_ARCHIVE":
        manager.datastore.delete_archived_input(job_id, job.parameters.target)
    else:
        job_service.update_job_status(
            job.job_id, "failed", log="Unknown operation for job"
        )


if __name__ == "__main__":
    logger.info("Polling for jobs...")
    main()
