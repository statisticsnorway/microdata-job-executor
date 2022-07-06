import os
import pytest

from job_executor.model.job import Job, JobParameters
from job_executor.adapter import job_service
from job_executor.exception import HttpResponseError


JOB_SERVICE_URL = os.environ['JOB_SERVICE_URL']
JOB_ID = '123'
JOB_LIST = [
    Job(
        jobId=JOB_ID,
        operation='CHANGE_DATA',
        status='queued',
        parameters=JobParameters(datasetName='INNTEKT')
    ),
    Job(
        jobId=JOB_ID,
        operation='SET_STATUS',
        status='queued',
        parameters=JobParameters(
            datasetName='KJOENN',
            releaseStatus='PENDING_RELEASE'
        )
    )
]
LOG_MESSAGE = 'log message'
DESCRIPTION = 'new description'
ERROR_RESPONSE = 'Internal Server Error'


def test_get_jobs(requests_mock):
    requests_mock.get(
        f'{JOB_SERVICE_URL}/jobs', json=[job.dict() for job in JOB_LIST]
    )
    jobs = job_service.get_jobs()
    assert jobs == JOB_LIST
    assert len(requests_mock.request_history) == 1


def test_update_job_status(requests_mock):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    job_service.update_job_status(JOB_ID, 'queued')
    job_service.update_job_status(JOB_ID, 'queued', LOG_MESSAGE)
    request_history = requests_mock.request_history
    assert len(request_history) == 2
    assert request_history[0].json() == {
        'status': 'queued'
    }
    assert request_history[1].json() == {
        'status': 'queued',
        'log': LOG_MESSAGE
    }


def test_update_description(requests_mock):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    job_service.update_description(JOB_ID, DESCRIPTION)
    request_history = requests_mock.request_history
    assert len(request_history) == 1
    assert request_history[0].json() == {
        'description': DESCRIPTION
    }


def test_no_connection(requests_mock):
    requests_mock.get(
        f'{JOB_SERVICE_URL}/jobs',
        status_code=500,
        text=ERROR_RESPONSE
    )
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}',
        status_code=500,
        text=ERROR_RESPONSE
    )
    with pytest.raises(HttpResponseError) as e:
        job_service.get_jobs()
    assert ERROR_RESPONSE in str(e)
    with pytest.raises(HttpResponseError) as e:
        job_service.update_job_status(JOB_ID, 'queued')
    assert ERROR_RESPONSE in str(e)
    with pytest.raises(HttpResponseError) as e:
        job_service.update_description(JOB_ID, DESCRIPTION)
    assert ERROR_RESPONSE in str(e)
