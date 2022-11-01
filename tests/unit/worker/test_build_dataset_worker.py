import os
import shutil
from multiprocessing import Queue
from requests_mock import Mocker as RequestsMocker

from job_executor.worker.build_dataset_worker import run_worker, local_storage


PARTITIONED_DATASET_NAME = 'INNTEKT'
DATASET_NAME = 'BOSTED'
JOB_ID = '1234-1234-1234-1234'
WORKING_DIR = os.environ['WORKING_DIR']
JOB_SERVICE_URL = os.environ['JOB_SERVICE_URL']
PSEUDONYM_SERVICE_URL = os.environ['PSEUDONYM_SERVICE_URL']
IDENTIFIERS = [
    '00000000000001', '00000000000002', '00000000000003', '00000000000004',
    '00000000000005', '00000000000006', '00000000000007', '00000000000008',
    '123123', '123124', '123125', '123126', '123127', '123128', '1231299',
    '1231210', '1231211', '1231212', '1231213', '1231214', '5123123',
    '5123124', '5123125', '5123126', '5123127', '5123128', '51231299',
    '51231210', '51231211', '51231212', '51231213', '51231214', '6123123',
    '6123124', '6123125', '6123126', '6123127', '6123128', '61231299',
    '61231210', '61231211', '61231212', '61231213', '61231214', '7123123',
    '7123124', '7123125', '7123126', '7123127', '7123128', '71231299',
    '71231210', '71231211', '71231212', '71231213', '71231214'
]
PSEUDONYM_DICT = {
    identifier: str(index) for index, identifier in enumerate(IDENTIFIERS)
}

EXPECTED_REQUESTS_PARTITIONED = [
    {
        'json': {'status': 'validating'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    },
    {
        'json': {'description': 'Oppdaterte data'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    },
    {
        'json': {'status': 'transforming'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    },
    {
        'json': {'status': 'pseudonymizing'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    },
    {
        'json': [
            '51231213',
            '71231213',
            '7123127',
            '71231214',
            '6123127',
            '123124',
            '5123125',
            '5123126',
            '51231211',
            '51231299',
            '7123125',
            '123125',
            '6123124',
            '71231299',
            '61231212',
            '61231214',
            '7123124',
            '7123126',
            '5123124',
            '61231210',
            '5123128',
            '6123125',
            '51231212',
            '1231212',
            '123126',
            '123128',
            '1231213',
            '6123126',
            '71231211',
            '1231210',
            '1231214',
            '61231211',
            '123123',
            '6123128',
            '61231299',
            '5123127',
            '71231210',
            '7123128',
            '51231210',
            '1231299',
            '5123123',
            '51231214',
            '1231211',
            '123127',
            '6123123',
            '7123123',
            '71231212',
            '61231213'
        ],
        'method': 'POST',
        'url': (
            'http://mock.pseudonym.service/'
            '?unit_id_type=FNR&job_id=1234-1234-1234-1234'
        )
    },
    {
        'json': {'status': 'enriching'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    },
    {
        'json': {'status': 'converting'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    },
    {
        'json': {'status': 'built'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    }
]

EXPECTED_REQUESTS_IMPORT = [
    {
        'json': {'status': 'validating'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    },
    {
        'json': {'description': 'Første publisering.'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    },
    {
        'json': {'status': 'transforming'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    },
    {
        'json': {'status': 'pseudonymizing'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    },
    {
        'json': [
            '00000000000001',
            '00000000000002',
            '00000000000003',
            '00000000000004',
            '00000000000005',
            '00000000000006',
            '00000000000007',
            '00000000000008'
        ],
        'method': 'POST',
        'url': (
            'http://mock.pseudonym.service/'
            '?unit_id_type=FNR&job_id=1234-1234-1234-1234'
        )
    },
    {
        'json': {'status': 'enriching'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    },
    {
        'json': {'status': 'converting'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    },
    {
        'json': {'status': 'built'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    }
]


def setup_function():

    if os.path.isdir('tests/resources_backup'):
        shutil.rmtree('tests/resources_backup')

    shutil.copytree(
        'tests/resources',
        'tests/resources_backup'
    )


def teardown_function():
    shutil.rmtree('tests/resources')
    shutil.move(
        'tests/resources_backup',
        'tests/resources'
    )


def test_build_partitioned_dataset(requests_mock: RequestsMocker):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    requests_mock.post(
        f'{PSEUDONYM_SERVICE_URL}?unit_id_type=FNR&job_id={JOB_ID}',
        json=PSEUDONYM_DICT
    )
    run_worker(JOB_ID, PARTITIONED_DATASET_NAME, Queue())

    assert os.path.isdir(
        f'{WORKING_DIR}/{PARTITIONED_DATASET_NAME}__DRAFT'
    )
    assert os.path.isfile(
        f'{WORKING_DIR}/{PARTITIONED_DATASET_NAME}__DRAFT.json'
    )
    requests_made = [
        {
            'method': req.method,
            'json': req.json(),
            'url': req.url
        }
        for req in requests_mock.request_history
    ]
    assert len(requests_made) == len(EXPECTED_REQUESTS_PARTITIONED)
    for index, _ in enumerate(requests_made):
        assert request_matches(
            requests_made[index], EXPECTED_REQUESTS_PARTITIONED[index]
        )


def test_import(requests_mock: RequestsMocker):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    requests_mock.post(
        f'{PSEUDONYM_SERVICE_URL}?unit_id_type=FNR&job_id={JOB_ID}',
        json=PSEUDONYM_DICT
    )
    run_worker(JOB_ID, DATASET_NAME, Queue())

    assert os.path.isfile(
        f'{WORKING_DIR}/{DATASET_NAME}__DRAFT.parquet'
    )
    assert os.path.isfile(
        f'{WORKING_DIR}/{DATASET_NAME}__DRAFT.json'
    )
    requests_made = [
        {
            'method': req.method,
            'json': req.json(),
            'url': req.url
        }
        for req in requests_mock.request_history
    ]
    assert len(requests_made) == len(EXPECTED_REQUESTS_IMPORT)
    for index, _ in enumerate(requests_made):
        assert request_matches(
            requests_made[index], EXPECTED_REQUESTS_IMPORT[index]
        )


def request_matches(request: dict, other: dict):
    if request.get('url') != other.get('url'):
        return False
    if request.get('method') != other.get('method'):
        return False
    request_payload = request.get('json')
    other_payload = request.get('json')
    if (
        isinstance(request_payload, list)
        and isinstance(other_payload, list)
    ):
        request_payload.sort()
        other_payload.sort()
        if request_payload != other_payload:
            return False
    return True


def test_delete_files_is_called(requests_mock: RequestsMocker, mocker):
    spy = mocker.patch.object(
        local_storage, 'delete_files')

    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )
    requests_mock.post(
        f'{PSEUDONYM_SERVICE_URL}?unit_id_type=FNR&job_id={JOB_ID}',
        json=PSEUDONYM_DICT
    )
    run_worker(JOB_ID, DATASET_NAME, Queue())
    spy.assert_called()
