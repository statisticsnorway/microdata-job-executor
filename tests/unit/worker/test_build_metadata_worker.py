import json
import os
import shutil
from multiprocessing import Queue
from pathlib import Path
from types import SimpleNamespace

from microdata_tools import package_dataset
from requests_mock import Mocker as RequestsMocker

from job_executor.config import environment
from job_executor.domain.worker.build_metadata_worker import run_worker
from tests.unit.worker.test_build_dataset_worker import _create_rsa_public_key

RSA_KEYS_DIRECTORY = Path(environment.datastore_dir) / "vault"

DATASET_NAME = "KJOENN"
JOB_ID = "1234-1234-1234-1234"
DATASTORE_DIR = os.environ["DATASTORE_DIR"]
WORKING_DIR = DATASTORE_DIR + "_working"
INPUT_DIR = DATASTORE_DIR + "_input"
INPUT_DIR_ARCHIVE = f"{INPUT_DIR}/archive"
EXPECTED_DIR = "tests/resources/worker/build_metadata/expected"
DATASTORE_API_URL = os.environ["DATASTORE_API_URL"]
EXPECTED_REQUESTS = [
    {
        "json": {"status": "decrypting"},
        "method": "PUT",
        "url": f"{DATASTORE_API_URL}/jobs/{JOB_ID}",
    },
    {
        "json": {"status": "validating"},
        "method": "PUT",
        "url": f"{DATASTORE_API_URL}/jobs/{JOB_ID}",
    },
    {
        "json": {"description": "Første publisering."},
        "method": "PUT",
        "url": f"{DATASTORE_API_URL}/jobs/{JOB_ID}",
    },
    {
        "json": {"status": "transforming"},
        "method": "PUT",
        "url": f"{DATASTORE_API_URL}/jobs/{JOB_ID}",
    },
    {
        "json": {"status": "built"},
        "method": "PUT",
        "url": f"{DATASTORE_API_URL}/jobs/{JOB_ID}",
    },
]
DATASTORE_RDN = os.environ["DATASTORE_RDN"]
JOB = SimpleNamespace(job_id=JOB_ID, datastore_rdn=DATASTORE_RDN)


def setup_function():
    if os.path.isdir("tests/resources_backup"):
        shutil.rmtree("tests/resources_backup")
    shutil.copytree("tests/resources", "tests/resources_backup")

    _create_rsa_public_key(RSA_KEYS_DIRECTORY)
    for dataset in os.listdir(INPUT_DIR):
        shutil.move(f"{INPUT_DIR}/{dataset}", f"{INPUT_DIR}/raw/{dataset}")
        package_dataset(
            rsa_keys_dir=RSA_KEYS_DIRECTORY,
            dataset_dir=Path(f"{INPUT_DIR}/raw/{dataset}"),
            output_dir=Path(f"{INPUT_DIR}"),
        )


def teardown_function():
    shutil.rmtree("tests/resources")
    shutil.move("tests/resources_backup", "tests/resources")


def test_import(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{DATASTORE_API_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    run_worker(JOB, DATASET_NAME, Queue())  # type: ignore
    with open(
        f"{WORKING_DIR}/{DATASET_NAME}__DRAFT.json", "r", encoding="utf-8"
    ) as f:
        actual_metadata = json.load(f)
    with open(
        f"{EXPECTED_DIR}/{DATASET_NAME}.json", "r", encoding="utf-8"
    ) as f:
        expected_metadata = json.load(f)

    assert actual_metadata == expected_metadata
    requests_made = [
        {"method": req.method, "json": req.json(), "url": req.url}
        for req in requests_mock.request_history
    ]
    assert requests_made == EXPECTED_REQUESTS

    assert not os.path.exists(f"{INPUT_DIR}/{DATASET_NAME}")
    assert not os.path.exists(f"{INPUT_DIR}/{DATASET_NAME}.json")
    assert not (
        Path(INPUT_DIR_ARCHIVE) / f"unpackaged/{DATASET_NAME}.tar"
    ).exists()
