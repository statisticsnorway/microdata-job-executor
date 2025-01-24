import logging

logger = logging.getLogger()


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

        # Maps job_id -> dataset size = {jobid1: size_in_bytes, jobid2: size_in_bytes, ...}
        self.datasets = {}

    @property
    def current_total_size(self):
        return sum(self.datasets.values())

    def can_spawn_new_worker(self, new_job_size):
        """
        Called to check if a new worker can be spawned.
        """
        can_spawn = True

        active_workers = len(self.datasets)
        if active_workers >= self.current_max_workers:
            can_spawn = False

        logger.info(
            f"Checking can_spawn_new_worker({new_job_size}): "
            f"active={active_workers}, dynamic_limit={self.current_max_workers}, "
            f"current_total_size={self.current_total_size}, can_spawn={can_spawn}"
        )

        return can_spawn

    def update_worker_limit(self, new_job_size):
        """
        Check the current size beeing procces in the pipeline.
        And changes the number of workers as needed.
        """
        if (
            self.current_total_size + new_job_size
        ) >= self.max_bytes_all_workers:
            self.current_max_workers = 2
        else:
            self.current_max_workers = self.default_max_workers

    def register_job(self, job_id, job_size):
        """
        Called when a worker picks up a job.
        When a job is register the current_max_workers are updated.
        """
        self.datasets[job_id] = job_size
        self.update_worker_limit(job_size)

    def unregister_job(self, job_id):
        """
        Called when a job finishes or fails.
        """
        if job_id in self.datasets:
            del self.datasets[job_id]

    def reset(self):
        self.current_max_workers = self.default_max_workers
        self.datasets.clear()
