import os
import shutil

from job_executor.worker.build_dataset_worker import run_worker

PARTITIONED_DATASET_NAME = 'INNTEKT'
DATASET_NAME = 'BOSTED'
JOB_ID = '1234-1234-1234-1234'
WORKING_DIR = os.environ['WORKING_DIR']
JOB_SERVICE_URL = os.environ['JOB_SERVICE_URL']
EXPECTED_REQUESTS = [
    {
        'json': {'status': 'validating'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'
    },
    {
        'json': {'status': 'transforming'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'},
    {
        'json': {'status': 'enriching'},
        'method': 'PUT',
        'url': f'{JOB_SERVICE_URL}/jobs/{JOB_ID}'},
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


def test_build_partitioned_dataset(requests_mock):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )

    run_worker(JOB_ID, PARTITIONED_DATASET_NAME)

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
    assert requests_made == EXPECTED_REQUESTS


def test_import(requests_mock):
    requests_mock.put(
        f'{JOB_SERVICE_URL}/jobs/{JOB_ID}', json={"message": "OK"}
    )

    run_worker(JOB_ID, DATASET_NAME)

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
    assert requests_made == EXPECTED_REQUESTS
