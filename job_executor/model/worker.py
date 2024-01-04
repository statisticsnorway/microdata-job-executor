from multiprocessing import Process


class Worker:
    def __init__(self, process: Process, job_id: str):
        self.process = process
        self.job_id = job_id

    def is_alive(self):
        return self.process.is_alive()

    def start(self):
        self.process.start()
