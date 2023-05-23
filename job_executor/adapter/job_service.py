import logging
from typing import List

import requests
from requests import RequestException, Response

from job_executor.config import environment
from job_executor.model.job import Job, JobStatus, Operation
from job_executor.exception import HttpResponseError, HttpRequestError

JOB_SERVICE_URL = environment.get("JOB_SERVICE_URL")
DEFAULT_REQUESTS_TIMEOUT = (10, 60)  # (read timeout, connect timeout)

logger = logging.getLogger()


def get_jobs(
    job_status: JobStatus = None,
    operations: List[Operation] = None,
    ignore_completed: bool = None,
) -> List[Job]:
    query_fields = []
    if job_status is not None:
        query_fields.append(f"status={job_status}")
    if operations is not None:
        query_fields.append(f'operation={",".join(operations)}')
    if ignore_completed is not None:
        query_fields.append(f"ignoreCompleted={str(ignore_completed).lower()}")

    request_url = f"{JOB_SERVICE_URL}/jobs"
    if query_fields:
        request_url += f'?{"&".join(query_fields)}'

    response = execute_request("GET", request_url)
    if response.status_code != 200:
        raise HttpResponseError(f"{response.text}")
    return [Job(**job) for job in response.json()]


def update_job_status(job_id: str, new_status: JobStatus, log: str = None):
    payload = {"status": new_status}
    if log is not None:
        payload.update({"log": log})
    response = execute_request(
        "PUT", f"{JOB_SERVICE_URL}/jobs/{job_id}", json=payload
    )
    if response.status_code != 200:
        raise HttpResponseError(f"{response.text}")


def update_description(job_id: str, new_description: str):
    response = execute_request(
        "PUT",
        f"{JOB_SERVICE_URL}/jobs/{job_id}",
        json={"description": new_description},
    )
    if response.status_code != 200:
        raise HttpResponseError(f"{response.status_code}: {response.text}")


def execute_request(method: str, url: str, **kwargs) -> Response:
    try:
        return requests.request(
            method=method, url=url, timeout=DEFAULT_REQUESTS_TIMEOUT, **kwargs
        )
    except RequestException as e:
        logger.exception(e)
        raise HttpRequestError(e) from e
