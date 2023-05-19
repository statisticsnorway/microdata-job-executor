import pytest
from requests_mock import Mocker as RequestsMocker
from job_executor.config import environment, secrets
from job_executor.adapter import pseudonym_service
from job_executor.exception import HttpResponseError


JOB_ID = "123-123-123"
PSEUDONYM_SERVICE_URL = environment.get("PSEUDONYM_SERVICE_URL")
API_KEY = secrets.get("PSEUDONYM_SERVICE_API_KEY")

URL = f"{PSEUDONYM_SERVICE_URL}/?unit_id_type=FNR&job_id={JOB_ID}"
UNIT_ID_TYPE = "FNR"
IDENTIFIERS = ["test1", "test2"]
PSEUDONYM_DICT = {"test1": "value", "test2": "value"}


def test_pseudonymize(requests_mock: RequestsMocker):
    requests_mock.post(URL, status_code=200, json=PSEUDONYM_DICT)
    assert (
        pseudonym_service.pseudonymize(IDENTIFIERS, UNIT_ID_TYPE, JOB_ID)
        == PSEUDONYM_DICT
    )
    request_history = requests_mock.request_history
    request = request_history[0]

    assert len(request_history) == 1
    assert request.url == URL
    assert request.method == "POST"
    assert request.json() == IDENTIFIERS
    assert request.headers["X-API-Key"] == API_KEY


def test_pseudonymize_bad_status(requests_mock: RequestsMocker):
    requests_mock.post(URL, status_code=500, text="error")
    with pytest.raises(HttpResponseError) as e:
        pseudonym_service.pseudonymize(IDENTIFIERS, UNIT_ID_TYPE, JOB_ID)
    request_history = requests_mock.request_history
    request = request_history[0]

    assert len(request_history) == 1
    assert request.url == URL
    assert request.method == "POST"
    assert request.json() == IDENTIFIERS
    assert request.headers["X-API-Key"] == API_KEY
    assert "500: error" == str(e.value)
