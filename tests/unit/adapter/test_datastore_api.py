import os

import pytest
from requests_mock import Mocker as RequestsMocker

from job_executor.adapter import datastore_api
from job_executor.adapter.datastore_api.models import (
    Job,
    JobParameters,
    JobStatus,
    Operation,
    ReleaseStatus,
    UserInfo,
)
from job_executor.common.exceptions import HttpResponseError

DATASTORE_API_URL = os.environ["DATASTORE_API_URL"]
JOB_ID = "123"
JOB_LIST = [
    Job(
        job_id=JOB_ID,
        status=JobStatus.QUEUED,
        parameters=JobParameters(target="INNTEKT", operation=Operation.CHANGE),
        log=[],
        created_at="2022-05-18T11:40:22.519222",
        created_by=UserInfo(
            user_id="123-123-123", first_name="Data", last_name="Admin"
        ),
    ),
    Job(
        job_id=JOB_ID,
        status=JobStatus.QUEUED,
        parameters=JobParameters(
            operation=Operation.SET_STATUS,
            target="KJOENN",
            release_status=ReleaseStatus.PENDING_RELEASE,
        ),
        log=[],
        created_at="2022-05-18T11:40:22.519222",
        created_by=UserInfo(
            user_id="123-123-123", first_name="Data", last_name="Admin"
        ),
    ),
]
LOG_MESSAGE = "log message"
DESCRIPTION = "new description"
ERROR_RESPONSE = "Internal Server Error"


def test_get_jobs(requests_mock: RequestsMocker):
    requests_mock.get(
        f"{DATASTORE_API_URL}/jobs",
        json=[
            job.model_dump(by_alias=True, exclude_none=True) for job in JOB_LIST
        ],
    )
    jobs = datastore_api.get_jobs()
    assert jobs == JOB_LIST
    assert len(requests_mock.request_history) == 1


def test_update_job_status(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{DATASTORE_API_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    datastore_api.update_job_status(JOB_ID, JobStatus.QUEUED)
    datastore_api.update_job_status(JOB_ID, JobStatus.QUEUED, LOG_MESSAGE)
    request_history = requests_mock.request_history
    assert len(request_history) == 2
    assert request_history[0].json() == {"status": "queued"}
    assert request_history[1].json() == {
        "status": "queued",
        "log": LOG_MESSAGE,
    }


def test_update_description(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{DATASTORE_API_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    datastore_api.update_description(JOB_ID, DESCRIPTION)
    request_history = requests_mock.request_history
    assert len(request_history) == 1
    assert request_history[0].json() == {"description": DESCRIPTION}


def test_no_connection(requests_mock: RequestsMocker):
    requests_mock.get(
        f"{DATASTORE_API_URL}/jobs", status_code=500, text=ERROR_RESPONSE
    )
    requests_mock.put(
        f"{DATASTORE_API_URL}/jobs/{JOB_ID}",
        status_code=500,
        text=ERROR_RESPONSE,
    )
    with pytest.raises(HttpResponseError) as e:
        datastore_api.get_jobs()
    assert ERROR_RESPONSE in str(e)
    with pytest.raises(HttpResponseError) as e:
        datastore_api.update_job_status(JOB_ID, JobStatus.QUEUED)
    assert ERROR_RESPONSE in str(e)
    with pytest.raises(HttpResponseError) as e:
        datastore_api.update_description(JOB_ID, DESCRIPTION)
    assert ERROR_RESPONSE in str(e)


def test_get_maintenance_status(requests_mock: RequestsMocker):
    requests_mock.get(
        f"{DATASTORE_API_URL}/maintenance-statuses/latest",
        json={
            "paused": False,
            "msg": "OK",
            "timestamp": "2023-05-08T06:31:00.519222",
        },
    )
    maintenance_status = datastore_api.get_maintenance_status()
    assert maintenance_status.paused is False


def test_get_maintenance_status_error(requests_mock: RequestsMocker):
    requests_mock.get(
        f"{DATASTORE_API_URL}/maintenance-statuses/latest",
        status_code=500,
        text=ERROR_RESPONSE,
    )
    with pytest.raises(HttpResponseError) as e:
        datastore_api.get_maintenance_status()
    assert ERROR_RESPONSE in str(e)


@pytest.mark.parametrize(
    "is_paused,expected_result",
    [
        (
            True,
            datastore_api.JobQueryResult(
                built_jobs=JOB_LIST,
                queued_manager_jobs=[],
                queued_worker_jobs=[],
            ),
        ),
        (
            False,
            datastore_api.JobQueryResult(
                built_jobs=JOB_LIST,
                queued_manager_jobs=JOB_LIST,
                queued_worker_jobs=JOB_LIST,
            ),
        ),
    ],
)
def test_query_for_jobs(is_paused, expected_result, requests_mock, monkeypatch):
    monkeypatch.setattr(
        "job_executor.adapter.datastore_api.is_system_paused", lambda: is_paused
    )

    # Always return built jobs even if system is paused
    # If system is paused, return empty list for queued and queued_manager jobs
    def mock_get_jobs(job_status=None, operations=None):
        if job_status == "built":
            return JOB_LIST
        elif job_status == "queued":
            return JOB_LIST if not is_paused else []
        elif job_status == "queued_manager":
            return JOB_LIST if not is_paused else []

    monkeypatch.setattr(
        "job_executor.adapter.datastore_api.get_jobs", mock_get_jobs
    )

    result = datastore_api.query_for_jobs()
    assert result.built_jobs == JOB_LIST
    if is_paused:
        assert result.queued_manager_jobs == []
        assert result.queued_worker_jobs == []
    else:
        assert result.queued_manager_jobs == JOB_LIST
        assert result.queued_worker_jobs == JOB_LIST
