import requests
from typing import List

from job_executor.config import environment
from job_executor.model.job import Job, JobStatus, Operation
from job_executor.exception.exception import HttpResponseError


JOB_SERVICE_URL = environment.get('JOB_SERVICE_URL')


def get_jobs(
    job_status: JobStatus = None, operations: List[Operation] = None
) -> List[Job]:
    query_fields = []
    if job_status is not None:
        query_fields.append(f'status={job_status}')
    if operations is not None:
        query_fields.append(f'operations={",".join(operations)}')

    request_url = f'{JOB_SERVICE_URL}/jobs'
    if query_fields:
        request_url += f'?{"&".join(query_fields)}'

    response = requests.get(request_url)
    if response.status_code != 200:
        raise HttpResponseError(f'{response.text}')
    return [Job(**job) for job in response.json()]


def update_job_status(job_id: str, new_status: JobStatus, log: str = None):
    payload = {'status': new_status}
    if log is not None:
        payload.update({'log': log})
    response = requests.put(
        f'{JOB_SERVICE_URL}/jobs/{job_id}',
        json=payload
    )
    if response.status_code != 200:
        raise HttpResponseError(f'{response.text}')


def update_description(job_id: str, new_description: str):
    response = requests.put(
        f'{JOB_SERVICE_URL}/jobs/{job_id}',
        json={'description': new_description}
    )
    if response.status_code != 200:
        raise HttpResponseError(f'{response.text}')