import logging
from pathlib import Path
from urllib.error import HTTPError

import requests
from requests import RequestException, Response
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from job_executor.adapter.datastore_api.models import (
    DatastoreResponse,
    Job,
    JobQueryResult,
    JobStatus,
    MaintenanceStatus,
    Operation,
)
from job_executor.common.exceptions import HttpRequestError, HttpResponseError
from job_executor.config import environment, secrets

DATASTORE_API_URL = environment.datastore_api_url
DEFAULT_REQUESTS_TIMEOUT = (10, 60)  # (read timeout, connect timeout)
DATASTORE_API_SERVICE_KEY = secrets.datastore_api_service_key

logger = logging.getLogger()


def execute_request(
    method: str,
    url: str,
    retry: bool = False,
    **kwargs,  # noqa
) -> Response:
    try:
        if retry:
            with requests.Session() as s:
                retries = Retry(
                    total=6,
                    backoff_factor=0.5,
                    # [0.0s, 1.0s, 2.0s, 4.0s, 8.0s, 16.0s] between retries
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


def get_jobs(
    job_status: JobStatus | None = None,
    operations: list[Operation] | None = None,
    ignore_completed: bool | None = None,
) -> list[Job]:
    query_fields = []
    if job_status is not None:
        query_fields.append(f"status={job_status}")
    if operations is not None:
        query_fields.append(f"operation={','.join(operations)}")
    if ignore_completed is not None:
        query_fields.append(f"ignoreCompleted={str(ignore_completed).lower()}")

    request_url = f"{DATASTORE_API_URL}/jobs"
    if query_fields:
        request_url += f"?{'&'.join(query_fields)}"

    response = execute_request("GET", request_url, True, headers={
             "X-API-Key": DATASTORE_API_SERVICE_KEY,
        },)
    return [Job.model_validate(job) for job in response.json()]


def update_job_status(
    job_id: str, new_status: JobStatus, log: str | None = None
) -> None:
    payload: dict[str, JobStatus | str] = {"status": str(new_status)}
    if log is not None:
        payload.update({"log": log})
    execute_request("PUT", f"{DATASTORE_API_URL}/jobs/{job_id}", json=payload, headers={
            "Content-Type": "application/json",
            "X-API-Key": DATASTORE_API_SERVICE_KEY,
        },)


def update_description(job_id: str, new_description: str) -> None:
    execute_request(
        "PUT",
        f"{DATASTORE_API_URL}/jobs/{job_id}",
        json={"description": new_description},
        headers={
            "Content-Type": "application/json",
            "X-API-Key": DATASTORE_API_SERVICE_KEY,
        }
    )


def get_maintenance_status() -> MaintenanceStatus:
    request_url = f"{DATASTORE_API_URL}/maintenance-statuses/latest"
    response = execute_request("GET", request_url, True)
    return MaintenanceStatus(**response.json())


def is_system_paused() -> bool:
    """Return True if the system is paused, otherwise False."""
    maintenance_status = get_maintenance_status()
    return maintenance_status.paused


def get_datastores() -> list[str]:
    """Get a list of all datastore rdns on this tenant"""
    request_url = f"{DATASTORE_API_URL}/datastores"
    return execute_request("GET", request_url, True).json()


def get_datastore_directory(rdn: str) -> Path:
    request_url = f"{DATASTORE_API_URL}/datastores/{rdn}"
    response = execute_request("GET", request_url, True)
    return Path(DatastoreResponse.model_validate(response.json()).directory)


def post_public_key(datastore_rdn: str, public_key_pem: bytes) -> None:
    """
    Post the public RSA key to the datastore-api.

    :param datastore_rdn: The RDN of the datastore
    :param public_key_pem: The public key in PEM format as bytes
    """
    request_url = f"{DATASTORE_API_URL}/datastores/{datastore_rdn}/public-key"
    execute_request(
        "POST",
        request_url,
        data=public_key_pem,
        headers={
            "Content-Type": "application/x-pem-file",
            "X-API-Key": DATASTORE_API_SERVICE_KEY,
        },
    )


def query_for_jobs() -> JobQueryResult:
    """
    Retrieves different types of jobs based on the system's state
    (paused or active).

    When the system is paused, only jobs with a 'built' status are fetched.
    In the active state, jobs are fetched based on their operations.
    """
    try:
        if is_system_paused():
            logger.info("System is paused. Only fetching built jobs.")
            return JobQueryResult(
                built_jobs=get_jobs(
                    job_status=JobStatus.BUILT, operations=None
                ),
            )
        else:
            return JobQueryResult(
                built_jobs=get_jobs(
                    job_status=JobStatus.BUILT, operations=None
                ),
                queued_manager_jobs=get_jobs(
                    job_status=JobStatus.QUEUED,
                    operations=[
                        Operation.SET_STATUS,
                        Operation.BUMP,
                        Operation.DELETE_DRAFT,
                        Operation.REMOVE,
                        Operation.ROLLBACK_REMOVE,
                        Operation.DELETE_ARCHIVE,
                        Operation.GENERATE_RSA_KEYS,
                    ],
                ),
                queued_worker_jobs=get_jobs(
                    job_status=JobStatus.QUEUED,
                    operations=[
                        Operation.PATCH_METADATA,
                        Operation.ADD,
                        Operation.CHANGE,
                    ],
                ),
            )
    except Exception as e:
        logger.exception("Exception when querying for jobs", exc_info=e)
        return JobQueryResult()
