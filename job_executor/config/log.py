import datetime
import json
import logging
import logging.handlers
import sys
import threading
from multiprocessing import Queue
from typing import Tuple

from job_executor.config import environment


class MicrodataJSONFormatter(logging.Formatter):
    def __init__(self) -> None:
        self.host = environment.get("DOCKER_HOST_NAME")
        self.command = json.dumps(sys.argv)
        self.commit_id = environment.get("COMMIT_ID")

    def format(self, record: logging.LogRecord) -> str:
        stack_trace = ""
        if record.exc_info is not None:
            stack_trace = self.formatException(record.exc_info)
        return json.dumps(
            {
                "@timestamp": datetime.datetime.fromtimestamp(
                    record.created,
                    tz=datetime.timezone.utc,
                ).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
                + "Z",
                "command": self.command,
                "error.stack": stack_trace,
                "host": self.host,
                "message": record.getMessage(),
                "level": record.levelno,
                "levelName": record.levelname,
                "loggerName": record.name,
                "schemaVersion": "v3",
                "serviceName": "job-executor",
                "serviceVersion": self.commit_id,
                "thread": record.threadName,
            }
        )


class WorkerFilter(logging.Filter):
    job_id = ""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = f"{self.job_id}: {record.msg}"
        return True


def logger_thread(logging_queue: Queue) -> None:
    """
    This method will run as a thread in the main process and will receive
    logs from workers via Queue.
    """
    logger = logging.getLogger()
    while True:
        record: logging.LogRecord = logging_queue.get()
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


def configure_worker_logger(queue: Queue, job_id: str) -> None:
    queue_handler = logging.handlers.QueueHandler(queue)
    queue_handler.setLevel(logging.INFO)

    logger = logging.getLogger()
    logger.addFilter(WorkerFilter(job_id))
    logger.handlers.clear()
    logger.addHandler(queue_handler)
