import logging
from typing import List
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
