import logging
from multiprocessing import Process, Queue

from job_executor.adapter import datastore_api
from job_executor.adapter.datastore_api.models import Job, JobStatus, Operation
from job_executor.adapter.fs import LocalStorageAdapter
from job_executor.domain import datastores, rollback
from job_executor.domain.datastores import Datastore
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
    are handled appropriately.
    """

    def __init__(
        self,
        max_workers: int,
        max_bytes_all_workers: int,
    ) -> None:
        """
        :param default_max_workers: The maximum number of workers
        :param max_gb_all_workers: Threshold in GB (50) for when the number
        of workers are reduced
        :param datastore: Datastore singleton for updating the state of the
        datastore
        """
        self.max_workers = max_workers
        self.max_bytes_all_workers = max_bytes_all_workers
        self.workers: list[Worker] = []

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

    def handle_worker_job(
        self,
        job: Job,
        job_size: int,
        logging_queue: Queue,
    ) -> None:
        dataset_name = job.parameters.target
        operation = job.parameters.operation
        if operation in ["ADD", "CHANGE"]:
            worker = Worker(
                process=Process(
                    target=build_dataset_worker.run_worker,
                    args=(
                        job,
                        dataset_name,
                        logging_queue,
                    ),
                ),
                job_id=job.job_id,
                job_size=job_size,
            )
            self.workers.append(worker)
            datastore_api.update_job_status(job.job_id, JobStatus.INITIATED)
            worker.start()
        elif operation == "PATCH_METADATA":
            worker = Worker(
                process=Process(
                    target=build_metadata_worker.run_worker,
                    args=(
                        job,
                        dataset_name,
                        logging_queue,
                    ),
                ),
                job_id=job.job_id,
                job_size=job_size,
            )
            self.workers.append(worker)
            datastore_api.update_job_status(job.job_id, JobStatus.INITIATED)
            worker.start()
        else:
            logger.error(f'Unknown operation "{operation}"')
            datastore_api.update_job_status(
                job.job_id,
                JobStatus.FAILED,
                log=f"Unknown operation type {operation}",
            )

    def handle_manager_job(self, job: Job) -> None:
        job_id = job.job_id
        operation = job.parameters.operation
        local_storage = LocalStorageAdapter(
            datastore_api.get_datastore_directory(job.datastore_rdn)
        )
        datastore = Datastore(local_storage)
        self.unregister_worker(
            job_id
        )  # Filter out job from worker jobs if built
        # Ignoring a lot of types here as we already have done the validation
        # in the pydantic model.
        if operation == Operation.BUMP:
            datastores.bump_version(
                datastore,
                local_storage,
                job,
                job.parameters.bump_manifesto,  # type: ignore
                job.parameters.description,  # type: ignore
            )
        elif operation == Operation.PATCH_METADATA:
            datastores.patch_metadata(
                datastore,
                local_storage,
                job,
                job.parameters.target,
                job.parameters.description,  # type: ignore
            )
        elif operation == Operation.SET_STATUS:
            datastores.set_draft_release_status(
                datastore,
                local_storage,
                job,
                job.parameters.target,
                job.parameters.release_status,  # type: ignore
            )
        elif operation == Operation.ADD:
            datastores.add(
                datastore,
                local_storage,
                job,
                job.parameters.target,
                job.parameters.description,  # type: ignore
            )
        elif operation == Operation.CHANGE:
            datastores.change(
                datastore,
                local_storage,
                job,
                job.parameters.target,
                job.parameters.description,  # type: ignore
            )
        elif operation == Operation.REMOVE:
            datastores.remove(
                datastore,
                local_storage,
                job,
                job.parameters.target,
                job.parameters.description,  # type: ignore
            )
        elif operation == Operation.ROLLBACK_REMOVE:
            datastores.delete_draft(
                datastore,
                local_storage,
                job,
                job.parameters.target,
                rollback_remove=True,
            )
        elif operation == Operation.DELETE_DRAFT:
            datastores.delete_draft(
                datastore,
                local_storage,
                job,
                job.parameters.target,
                rollback_remove=False,
            )
        elif operation == Operation.DELETE_ARCHIVE:
            datastores.delete_archived_input(job, job.parameters.target)
        else:
            datastore_api.update_job_status(
                job.job_id, JobStatus.FAILED, log="Unknown operation for job"
            )
