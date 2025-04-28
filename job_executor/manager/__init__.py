import logging
from typing import List
from multiprocessing import Process, Queue

from job_executor.adapter import job_service
from job_executor.domain import rollback
from job_executor.model.job import Job
from job_executor.model.worker import Worker
from job_executor.worker import build_dataset_worker, build_metadata_worker


logger = logging.getLogger()


class Manager:
    def __init__(self, max_workers, max_bytes_all_workers, datastore):
        """
        :param default_max_workers: The maximum number of workers
        :param max_gb_all_workers: Threshold in GB (50) for when the number
        of workers are reduced
        :param datastore: Datastore singleton for updating the state of the
        datastore
        """
        self.max_workers = max_workers
        self.max_bytes_all_workers = max_bytes_all_workers
        self.datastore = datastore
        self.workers: List[Worker] = []

    @property
    def current_total_size(self) -> int:
        return sum(
            worker.job_size for worker in self.workers if worker.is_alive()
        )

    def can_spawn_new_worker(self, new_job_size: int) -> bool:
        """
        Called to check if a new worker can be spawned.
        """
        alive_workers = [
            worker for worker in self.workers if worker.is_alive()
        ]
        if len(alive_workers) >= self.max_workers:
            return False
        if (
            self.current_total_size + new_job_size
            >= self.max_bytes_all_workers
        ):
            return False
        return True

    def unregister_worker(self, job_id):
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
            self.workers.append(worker)
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
            self.workers.append(worker)
            job_service.update_job_status(job_id, "initiated")
            worker.start()
        else:
            logger.error(f'Unknown operation "{operation}"')
            job_service.update_job_status(
                job_id, "failed", log=f"Unknown operation type {operation}"
            )

    def handle_manager_job(self, job: Job):
        job_id = job.job_id
        operation = job.parameters.operation
        self.unregister_worker(
            job_id
        )  # Filter out job from worker jobs if built
        if operation == "BUMP":
            self.datastore.bump_version(
                job_id,
                job.parameters.bump_manifesto,
                job.parameters.description,
            )
        elif operation == "PATCH_METADATA":
            self.datastore.patch_metadata(
                job_id, job.parameters.target, job.parameters.description
            )
        elif operation == "SET_STATUS":
            self.datastore.set_draft_release_status(
                job_id, job.parameters.target, job.parameters.release_status
            )
        elif operation == "ADD":
            self.datastore.add(
                job_id, job.parameters.target, job.parameters.description
            )
        elif operation == "CHANGE":
            self.datastore.change(
                job_id, job.parameters.target, job.parameters.description
            )
        elif operation == "REMOVE":
            self.datastore.remove(
                job_id, job.parameters.target, job.parameters.description
            )
        elif operation == "ROLLBACK_REMOVE":
            self.datastore.delete_draft(
                job_id, job.parameters.target, rollback_remove=True
            )
        elif operation == "DELETE_DRAFT":
            self.datastore.delete_draft(
                job_id, job.parameters.target, rollback_remove=False
            )
        elif operation == "DELETE_ARCHIVE":
            self.datastore.delete_archived_input(job_id, job.parameters.target)
        else:
            job_service.update_job_status(
                job.job_id, "failed", log="Unknown operation for job"
            )
