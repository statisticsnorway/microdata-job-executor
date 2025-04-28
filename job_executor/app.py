import sys
import time
import logging
from multiprocessing import Process, Queue

from job_executor.adapter import job_service, local_storage
from job_executor.config import environment
from job_executor.config.log import initialize_logging_thread, setup_logging
from job_executor.domain import rollback
from job_executor.exception import StartupException

from job_executor.model import Datastore
from job_executor.manager import Manager


logger = logging.getLogger()
setup_logging()


def initialize_app():
    try:
        rollback.fix_interrupted_jobs()
        if local_storage.temporary_backup_exists():
            raise StartupException("tmp directory exists")
    except Exception as e:
        logger.exception("Exception when initializing", exc_info=e)
        sys.exit("Exception when initializing")


def handle_jobs(manager: Manager, logging_queue: Queue):
    job_query_result = job_service.query_for_jobs()
    manager.clean_up_after_dead_workers()
    if job_query_result.available_jobs_count:
        logger.info(
            f"Found {len(job_query_result.queued_worker_jobs)}"
            f"/{len(job_query_result.built_jobs)}"
            f"/{len(job_query_result.queued_manager_jobs)}"
            f" (worker, built, queued manager jobs)"
        )

    for job in job_query_result.queued_worker_jobs:
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
            manager.handle_worker_job(job, job_size, logging_queue)

    for job in job_query_result.queued_manager_and_built_jobs():
        try:
            manager.handle_manager_job(job)
        except Exception as exc:
            # All exceptions that occur during the handling of a job
            # are resolved by rolling back. The exceptions that
            # reach here are exceptions raised by the rollback.
            logger.exception(
                f"{job.job_id} failed and could not roll back",
                exc_info=exc,
            )
            raise exc


def main():
    logging_queue = None
    log_thread = None
    try:
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

        while True:
            time.sleep(5)
            handle_jobs(manager, logging_queue)
    except Exception as exc:
        logger.exception("Service stopped by exception", exc_info=exc)
    finally:
        # Tell the logging thread to finish up
        if logging_queue is not None:
            logging_queue.put(None)
        if log_thread is not None:
            log_thread.join()


if __name__ == "__main__":
    logger.info("Polling for jobs...")
    main()
