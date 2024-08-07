import sys
import json
import logging
import logging.handlers
import datetime
import tomlkit
import threading

from typing import Tuple
from multiprocessing import Queue
from job_executor.config import environment


def _get_project_meta():
    with open("pyproject.toml", encoding="utf-8") as pyproject:
        file_contents = pyproject.read()
    return tomlkit.parse(file_contents)["tool"]["poetry"]


class MicrodataJSONFormatter(logging.Formatter):
    def __init__(self):
        self.pkg_meta = _get_project_meta()
        self.host = environment.get("DOCKER_HOST_NAME")
        self.command = json.dumps(sys.argv)

    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "@timestamp": datetime.datetime.fromtimestamp(
                    record.created,
                    tz=datetime.timezone.utc,
                ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
                + "Z",
                "command": self.command,
                "error.stack": record.__dict__.get("exc_info"),
                "host": self.host,
                "message": record.getMessage(),
                "level": record.levelno,
                "levelName": record.levelname,
                "loggerName": record.name,
                "schemaVersion": "v3",
                "serviceName": "job-executor",
                "serviceVersion": str(self.pkg_meta["version"]),
                "thread": record.threadName,
            }
        )


class WorkerFormatter(logging.Formatter):
    job_id = ""

    def __init__(self, job_id: str):
        self.job_id = job_id

    def format(self, record: logging.LogRecord) -> str:
        return f"{self.job_id}: {record.msg}"


def logger_thread(logging_queue: Queue):
    """
    This method will run as a thread in the main process and will receive
    logs from workers via Queue.
    """
    logger = logging.getLogger()
    while True:
        record = logging_queue.get()
        if record is None:
            break
        logger.handle(record)


def initialize_logging_thread() -> Tuple[Queue, threading.Thread]:
    logging_queue = Queue()

    log_thread = threading.Thread(target=logger_thread, args=(logging_queue,))
    log_thread.start()
    return logging_queue, log_thread


def setup_logging(log_level: int = logging.INFO) -> None:
    logger = logging.getLogger()
    logger.setLevel(log_level)

    formatter = MicrodataJSONFormatter()

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


def configure_worker_logger(queue: Queue, job_id: str):
    queue_handler = logging.handlers.QueueHandler(queue)
    queue_handler.setLevel(logging.INFO)
    queue_handler.formatter = WorkerFormatter(job_id)

    logger = logging.getLogger()
    logger.handlers.clear()
    logger.addHandler(queue_handler)
