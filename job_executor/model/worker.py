from multiprocessing import Process


class Worker:
    job_id: str
    job_size: int
    process: Process

    def __init__(self, process: Process, job_id: str, job_size: int):
        self.process = process
        self.job_id = job_id
        self.job_size = job_size

    def is_alive(self):
        return self.process.is_alive()

    def start(self):
        self.process.start()
