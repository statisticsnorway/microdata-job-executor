import logging
from job_executor.model.worker import Worker

from typing import List

logger = logging.getLogger()

REDUCED_WORKER_NUMBER = 2


class ManagerState:
    def __init__(self, default_max_workers=4, max_gb_all_workers=50):
        """
        :param default_max_workers: The maximum number of workers
        :param max_gb_all_workers: Threshold in GB (50) for when the number
        of workers are reduced
        """
        self.default_max_workers = default_max_workers
        self.current_max_workers = default_max_workers
        self.max_bytes_all_workers = (
            max_gb_all_workers * 1024**3  # Threshold in bytes
        )

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
        can_spawn = True

        active_workers = len(self.alive_workers)
        if active_workers >= self.current_max_workers:
            can_spawn = False

        logger.info(
            f"Checking can_spawn_new_worker({new_job_size}): "
            f"active={active_workers}, dynamic_limit={self.current_max_workers}, "
            f"current_total_size={self.current_total_size}, can_spawn={can_spawn}"
        )

        return can_spawn

    def update_worker_limit(self, new_job_size: int):
        """
        Check the current size beeing procces in the pipeline.
        And changes the number of workers as needed.
        """
        new_total = self.current_total_size + new_job_size
        if new_total >= self.max_bytes_all_workers:
            self.current_max_workers = REDUCED_WORKER_NUMBER
        else:
            self.current_max_workers = self.default_max_workers

    def register_job(self, worker: Worker):
        """
        Called when a worker picks up a job.
        When a job is register the current_max_workers are updated.
        """
        self.workers.append(worker)
        self.update_worker_limit(worker.job_size)

    def unregister_job(self, job_id):
        """
        Called when a job finishes or fails.
        """
        self.workers = [
            worker for worker in self.workers if worker.job_id != job_id
        ]
