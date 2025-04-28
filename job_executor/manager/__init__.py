from job_executor.adapter import job_service
from job_executor.model.worker import Worker

from typing import List


class Manager:
    def __init__(self, max_workers, max_bytes_all_workers):
        """
        :param default_max_workers: The maximum number of workers
        :param max_gb_all_workers: Threshold in GB (50) for when the number
        of workers are reduced
        """
        self.max_workers = max_workers
        self.max_bytes_all_workers = max_bytes_all_workers

        self.workers: List[Worker] = []

    @property
    def dead_workers(self) -> List[Worker]:
        """
        Return a list of dead workers
        """
        return [worker for worker in self.workers if not worker.is_alive()]

    @property
    def alive_workers(self) -> List[Worker]:
        """
        Return a list of alive workers
        """
        return [worker for worker in self.workers if worker.is_alive()]

    @property
    def current_total_size(self) -> int:
        return sum(
            worker.job_size for worker in self.workers if worker.is_alive()
        )

    def can_spawn_new_worker(self, new_job_size: int) -> bool:
        """
        Called to check if a new worker can be spawned.
        """
        if len(self.alive_workers) >= self.max_workers:
            return False
        if (
            self.current_total_size + new_job_size
            >= self.max_bytes_all_workers
        ):
            return False
        return True

    def register_job(self, worker: Worker):
        """
        Called when a worker picks up a job.
        """
        self.workers.append(worker)

    def unregister_job(self, job_id):
        """
        Called when a job finishes or fails.
        """
        self.workers = [
            worker for worker in self.workers if worker.job_id != job_id
        ]

    def clean_up_after_dead_workers(self) -> None:
        dead_workers = self.dead_workers
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
                    fix_interrupted_job(job)
                self.unregister_job(dead_worker.job_id)
