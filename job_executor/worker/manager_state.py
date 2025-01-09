class ManagerState:
    def __init__(
        self,
        default_max_workers=4,
        dynamic_worker_threshold=50 * 1024**3,
    ):
        """
        :param default_max_workers: The maximum number of workers
        :param dynamic_worker_threshold: Threshold when the number of workers are reduced (50GB - 50 * 1024**3)
        """
        self.default_max_workers = default_max_workers
        self.current_max_workers = default_max_workers
        self.dynamic_worker_threshold = dynamic_worker_threshold

        # Maps job_id -> dataset size = {jobid1: size_in_bytes, jobid2: size_in_bytes, ...}
        self.datasets = {}

    @property
    def current_total_size(self):
        return sum(self.datasets.values())

    def can_spawn_new_worker(self, new_job_size):
        """
        Called to check if a new worker can be spawned. If the current total size of data being processed
        is larger than the threshold the number of workers are reduced to 2.

        When a job is finished and unregister the number of workers will reset to default_max_workers
        """

        # Could tweak this to be more gradual if we want
        if self.current_total_size >= self.dynamic_worker_threshold:
            self.current_max_workers = 2
        else:
            self.current_max_workers = self.default_max_workers

        active_workers = len(self.datasets)
        if active_workers >= self.current_max_workers:
            return False

        return True

    def register_job(self, job_id, job_size):
        """
        Called when a worker picks up a job.
        """
        self.datasets[job_id] = job_size

    def unregister_job(self, job_id):
        """
        Called when a job finishes or fails.
        """
        if job_id in self.datasets:
            del self.datasets[job_id]

    def reset(self):
        self.current_max_workers = self.default_max_workers
        self.datasets.clear()
