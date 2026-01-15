import logging
from multiprocessing import Process, Queue
from threading import Thread

from job_executor.adapter import datastore_api
from job_executor.adapter.datastore_api.models import (
    JobQueryResult,
    JobStatus,
    Operation,
)
from job_executor.config.log import initialize_logging_thread
from job_executor.domain import datastores, rollback
from job_executor.domain.models import JobContext, build_job_context
from job_executor.domain.worker import (
    build_dataset_worker,
    build_metadata_worker,
)
from job_executor.domain.worker.models import Worker

logger = logging.getLogger()


class Manager:
    """
    Manager works in the main thread and executes all incoming jobs. Either
    by handing off work to an available worker in a sub-process for work that
    can be done in parallel, or by making changes to the datastore directly.

    It ensures that the common workload of the application does not exceed
    memory limits, and makes sure that the sub-process workers lifetimes
    are handled appropriately and that their logs are piped to the main
    process.
    """

    max_workers: int
    max_bytes_all_workers: int
    logging_queue: Queue
    logging_thread: Thread

    def __init__(
        self,
        max_workers: int,
        max_bytes_all_workers: int,
    ) -> None:
        """
        :param default_max_workers: The maximum number of workers
        :param max_gb_all_workers: Threshold in GB (50) for when the number
        of workers are reduced
        """
        self.max_workers = max_workers
        self.max_bytes_all_workers = max_bytes_all_workers
        self.workers: list[Worker] = []
        self.logging_queue, self.log_thread = initialize_logging_thread()

    @property
    def current_total_size(self) -> int:
        return sum(
            worker.job_size
            for worker in self.workers
            if worker and worker.is_alive()
        )

    def can_spawn_new_worker(self, new_job_size: int) -> bool:
        """
        Called to check if a new worker can be spawned.
        """
        alive_workers = [worker for worker in self.workers if worker.is_alive()]
        if len(alive_workers) >= self.max_workers:
            return False
        if self.current_total_size + new_job_size >= self.max_bytes_all_workers:
            return False
        return True

    def unregister_worker(self, job_id: str) -> None:
        """
        Called when a worker finishes or fails.
        """
        self.workers = [
            worker for worker in self.workers if worker.job_id != job_id
        ]

    def clean_up_after_dead_workers(self) -> None:
        dead_workers = [
            worker for worker in self.workers if not worker.is_alive()
        ]
        if len(dead_workers) > 0:
            in_progress_jobs = datastore_api.get_jobs(ignore_completed=True)
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
                    logger.warning(
                        f"Worker died and did not finish job {job.job_id}"
                    )
                    rollback.fix_interrupted_job(job)
                self.unregister_worker(dead_worker.job_id)

    def _handle_worker_job(self, job_context: JobContext) -> None:
        job_id = job_context.job.job_id
        operation = job_context.job.parameters.operation
        assert job_context.job_size is not None
        if operation in ["ADD", "CHANGE"]:
            worker = Worker(
                process=Process(
                    target=build_dataset_worker.run_worker,
                    args=(
                        job_context,
                        self.logging_queue,
                    ),
                ),
                job_id=job_id,
                job_size=job_context.job_size,
            )
            self.workers.append(worker)
            datastore_api.update_job_status(job_id, JobStatus.INITIATED)
            worker.start()
        elif operation == "PATCH_METADATA":
            worker = Worker(
                process=Process(
                    target=build_metadata_worker.run_worker,
                    args=(
                        job_context,
                        self.logging_queue,
                    ),
                ),
                job_id=job_id,
                job_size=job_context.job_size,
            )
            self.workers.append(worker)
            datastore_api.update_job_status(job_id, JobStatus.INITIATED)
            worker.start()
        else:
            logger.error(f'Unknown operation "{operation}"')
            datastore_api.update_job_status(
                job_id,
                JobStatus.FAILED,
                log=f"Unknown operation type {operation}",
            )

    def _handle_manager_job(self, job_context: JobContext) -> None:
        job_id = job_context.job.job_id
        operation = job_context.job.parameters.operation
        self.unregister_worker(
            job_id
        )  # Filter out job from worker jobs if built
        if operation == Operation.BUMP:
            datastores.bump_version(job_context)
        elif operation == Operation.PATCH_METADATA:
            datastores.patch_metadata(job_context)
        elif operation == Operation.SET_STATUS:
            datastores.set_draft_release_status(job_context)
        elif operation == Operation.ADD:
            datastores.add(job_context)
        elif operation == Operation.CHANGE:
            datastores.change(job_context)
        elif operation == Operation.REMOVE:
            datastores.remove(job_context)
        elif operation == Operation.ROLLBACK_REMOVE:
            datastores.delete_draft(job_context, rollback_remove=True)
        elif operation == Operation.DELETE_DRAFT:
            datastores.delete_draft(job_context)
        elif operation == Operation.DELETE_ARCHIVE:
            datastores.delete_archived_input(job_context)
        elif operation == Operation.GENERATE_RSA_KEYS:
            datastores.generate_rsa_keys(job_context)
        else:
            datastore_api.update_job_status(
                job_context.job.job_id,
                JobStatus.FAILED,
                log="Unknown operation for job",
            )

    def handle_jobs(self, job_query_result: JobQueryResult) -> None:
        self.clean_up_after_dead_workers()
        if job_query_result.available_jobs_count:
            logger.info(
                f"Found {len(job_query_result.queued_worker_jobs)}"
                f"/{len(job_query_result.built_jobs)}"
                f"/{len(job_query_result.queued_manager_jobs)}"
                f" (worker, built, queued manager jobs)"
            )

        for job in job_query_result.queued_worker_jobs:
            job_id = job.job_id
            job_context = build_job_context(job, "worker")
            if job_context.job_size == 0 or job_context.job_size is None:
                logger.error(f"{job_id} Failed to get the size of the dataset.")
                datastore_api.update_job_status(
                    job_context.job.job_id,
                    JobStatus.FAILED,
                    log="No such dataset available for import",
                )
                continue  # skip futher processing of this job
            if job_context.job_size > self.max_bytes_all_workers:
                logger.warning(
                    f"{job_id} Exceeded the maximum size for all workers."
                )
                datastore_api.update_job_status(
                    job_id,
                    JobStatus.FAILED,
                    log="Dataset too large for import",
                )
                continue  # skip futher processing of this job
            if self.can_spawn_new_worker(job_context.job_size):
                self._handle_worker_job(job_context)

        for job in job_query_result.queued_manager_and_built_jobs():
            job_context = build_job_context(job, "manager")
            try:
                self._handle_manager_job(job_context)
            except Exception as exc:
                # All exceptions that occur during the handling of a job
                # are resolved by rolling back. The exceptions that
                # reach here are exceptions raised by the rollback.
                logger.exception(
                    f"{job.job_id} failed and could not roll back",
                    exc_info=exc,
                )
                raise exc

    def close_logging_thread(self) -> None:
        if self.logging_queue is not None:
            self.logging_queue.put(None)
        if self.log_thread is not None:
            self.log_thread.join()
