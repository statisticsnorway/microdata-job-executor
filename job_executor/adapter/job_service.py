import logging
from typing import List, Dict
from urllib.error import HTTPError

import requests
from requests import RequestException, Response
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from job_executor.config import environment
from job_executor.exception import HttpResponseError, HttpRequestError
from job_executor.model.job import Job, JobStatus, Operation
from job_executor.model.maintenance_status import MaintenanceStatus

JOB_SERVICE_URL = environment.get("JOB_SERVICE_URL")
DEFAULT_REQUESTS_TIMEOUT = (10, 60)  # (read timeout, connect timeout)

logger = logging.getLogger()


class JobQueryResult:
    def __init__(
        self,
        queued_worker_jobs: List[Job],
        built_jobs: List[Job],
        queued_manager_jobs: List[Job],
    ):
        self.queued_worker_jobs = queued_worker_jobs
        self.built_jobs = built_jobs
        self.queued_manager_jobs = queued_manager_jobs

    @property
    def available_jobs_count(self):
        return (
            len(self.queued_worker_jobs)
            + len(self.built_jobs)
            + len(self.queued_manager_jobs)
        )

    def queued_manager_and_built_jobs(self):
        return self.queued_manager_jobs + self.built_jobs


def get_jobs(
    job_status: JobStatus = None,
    operations: List[Operation] = None,
    ignore_completed: bool = None,
) -> List[Job]:
    query_fields = []
    if job_status is not None:
        query_fields.append(f"status={job_status}")
    if operations is not None:
        query_fields.append(f"operation={','.join(operations)}")
    if ignore_completed is not None:
        query_fields.append(f"ignoreCompleted={str(ignore_completed).lower()}")

    request_url = f"{JOB_SERVICE_URL}/jobs"
    if query_fields:
        request_url += f"?{'&'.join(query_fields)}"

    response = execute_request("GET", request_url, True)
    return [Job(**job) for job in response.json()]


def update_job_status(job_id: str, new_status: JobStatus, log: str = None):
    payload = {"status": new_status}
    if log is not None:
        payload.update({"log": log})
    execute_request("PUT", f"{JOB_SERVICE_URL}/jobs/{job_id}", json=payload)


def update_description(job_id: str, new_description: str):
    execute_request(
        "PUT",
        f"{JOB_SERVICE_URL}/jobs/{job_id}",
        json={"description": new_description},
    )


def get_maintenance_status() -> MaintenanceStatus:
    request_url = f"{JOB_SERVICE_URL}/maintenance-status"
    response = execute_request("GET", request_url, True)
    return MaintenanceStatus(**response.json())


def is_system_paused() -> bool:
    """Return True if the system is paused, otherwise False."""
    maintenance_status = get_maintenance_status()
    return maintenance_status.paused


def execute_request(
    method: str, url: str, retry: bool = False, **kwargs
) -> Response:
    try:
        if retry:
            with requests.Session() as s:
                retries = Retry(
                    total=6,
                    backoff_factor=0.5,  # [0.0s, 1.0s, 2.0s, 4.0s, 8.0s, 16.0s] between retries
                    allowed_methods={"GET"},
                )
                s.mount("http://", HTTPAdapter(max_retries=retries))
                response = s.request(method=method, url=url, **kwargs)
        else:
            response = requests.request(
                method=method,
                url=url,
                timeout=DEFAULT_REQUESTS_TIMEOUT,
                **kwargs,
            )
        if response.status_code != 200:
            raise HttpResponseError(f"{response.status_code}: {response.text}")
        return response
    except (RequestException, HTTPError) as e:
        raise HttpRequestError(e) from e


def query_for_jobs() -> JobQueryResult:
    """
    Retrieves different types of jobs based on the system's state (paused or active).

    When the system is paused, only jobs with a 'built' status are fetched.
    In the active state, jobs are fetched based on their operations.

    Returns:
        Dict[str, List[Job]]: A dictionary structured as:
        - "built_jobs": Jobs that have already been built.
        - "queued_manager_jobs": Jobs in the queue with managerial operations.
        - "queued_worker_jobs": Jobs in the queue with worker operations.
    """
    try:
        if is_system_paused():
            logger.info("System is paused. Only fetching built jobs.")
            return JobQueryResult(
                built_jobs=get_jobs(job_status="built", operations=None),
                queued_manager_jobs=[],
                queued_worker_jobs=[],
            )
        else:
            return JobQueryResult(
                built_jobs=get_jobs(job_status="built", operations=None),
                queued_manager_jobs=get_jobs(
                    job_status="queued",
                    operations=[
                        "SET_STATUS",
                        "BUMP",
                        "DELETE_DRAFT",
                        "REMOVE",
                        "ROLLBACK_REMOVE",
                        "DELETE_ARCHIVE",
                    ],
                ),
                queued_worker_jobs=get_jobs(
                    job_status="queued",
                    operations=[
                        "PATCH_METADATA",
                        "ADD",
                        "CHANGE",
                    ],
                ),
            )
    except Exception as e:
        logger.exception("Exception when querying for jobs", exc_info=e)
        return JobQueryResult(
            built_jobs=[],
            queued_manager_jobs=[],
            queued_worker_jobs=[],
        )
