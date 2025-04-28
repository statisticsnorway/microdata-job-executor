from job_executor.model.worker import Worker

from typing import List


class ManagerState:
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
