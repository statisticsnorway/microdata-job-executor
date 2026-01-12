import os
from dataclasses import dataclass
from datetime import UTC, datetime
from multiprocessing import Queue
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from job_executor.adapter.datastore_api.models import (
    Job,
    JobParameters,
    JobStatus,
    Operation,
    UserInfo,
)
from job_executor.adapter.fs import LocalStorageAdapter
from job_executor.domain.models import JobContext
from job_executor.domain.worker import (
    build_dataset_worker,
    build_metadata_worker,
)
from tests.integration.common import (
    backup_resources,
    prepare_datastore,
    recover_resources_from_backup,
)

RESOURCES_DIR = Path("tests/integration/resources/datastores")
DATASTORE_DIR = RESOURCES_DIR / "TEST_DATASTORE"
WORKING_DIR = RESOURCES_DIR / "TEST_DATASTORE_working"
INPUT_DIR = RESOURCES_DIR / "TEST_DATASTORE_input"


@dataclass
class MockedDatastoreApi:
    update_job_status: MagicMock
    update_description: MagicMock


@dataclass
class MockedPseudonymService:
    pseudonymize: MagicMock


@pytest.fixture
def mocked_datastore_api(mocker) -> MockedDatastoreApi:
    return MockedDatastoreApi(
        update_job_status=mocker.patch(
            "job_executor.adapter.datastore_api.update_job_status",
            return_value=None,
        ),
        update_description=mocker.patch(
            "job_executor.adapter.datastore_api.update_description",
            return_value=None,
        ),
    )


@pytest.fixture(autouse=True)
def mocked_pseudonym_service(mocker) -> MockedPseudonymService:
    return MockedPseudonymService(
        pseudonymize=mocker.patch(
            "job_executor.adapter.pseudonym_service.pseudonymize",
            return_value={
                "00000000001": 1,
                "00000000002": 2,
                "00000000003": 3,
                "00000000004": 4,
                "00000000005": 5,
                "00000000006": 6,
                "00000000007": 7,
            },
        )
    )


@pytest.fixture(autouse=True)
def set_up_resources():
    backup_resources()
    prepare_datastore(str(DATASTORE_DIR), package_to_input=True)
    yield
    recover_resources_from_backup()


def generate_job_context(operation: Operation, target: str) -> JobContext:
    return JobContext(
        handler="worker",
        local_storage=LocalStorageAdapter(DATASTORE_DIR),
        job_size=100,
        job=Job(
            job_id="1",
            datastore_rdn="TEST_DATASTORE",
            status=JobStatus.QUEUED,
            parameters=JobParameters(
                operation=operation,
                target=target,
            ),
            created_at=datetime.now(tz=UTC).isoformat(),
            created_by=UserInfo(
                user_id="1", first_name="Test", last_name="Testerson"
            ),
        ),
    )


def test_import_patch(mocked_datastore_api: MockedDatastoreApi):
    DATASET_NAME = "IMPORTABLE_PATCH_METADATA"
    patch_metadata_context = generate_job_context(
        operation=Operation.PATCH_METADATA,
        target=DATASET_NAME,
    )
    build_metadata_worker.run_worker(patch_metadata_context, Queue())
    assert mocked_datastore_api.update_job_status.call_count == 4
    assert mocked_datastore_api.update_description.call_count == 1
    assert os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT.json")


def test_import_add(mocked_datastore_api: MockedDatastoreApi):
    DATASET_NAME = "IMPORTABLE_ADD"
    add_context = generate_job_context(
        operation=Operation.ADD,
        target=DATASET_NAME,
    )
    build_dataset_worker.run_worker(add_context, Queue())
    assert mocked_datastore_api.update_job_status.call_count == 6
    assert mocked_datastore_api.update_description.call_count == 1
    assert os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT.json")
    assert os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT.parquet")


def test_import_change(mocked_datastore_api: MockedDatastoreApi):
    DATASET_NAME = "IMPORTABLE_CHANGE"
    change_context = generate_job_context(
        operation=Operation.CHANGE,
        target=DATASET_NAME,
    )
    build_dataset_worker.run_worker(change_context, Queue())
    assert mocked_datastore_api.update_job_status.call_count == 6
    assert mocked_datastore_api.update_description.call_count == 1
    assert os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT.json")
    assert os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT.parquet")


def test_import_add_no_pseudo(
    mocked_datastore_api: MockedDatastoreApi,
    mocked_pseudonym_service: MockedPseudonymService,
):
    DATASET_NAME = "IMPORTABLE_ADD_NO_PSEUDO"
    add_no_pseudo_context = generate_job_context(
        operation=Operation.ADD,
        target=DATASET_NAME,
    )
    build_dataset_worker.run_worker(add_no_pseudo_context, Queue())
    assert mocked_datastore_api.update_job_status.call_count == 5
    assert mocked_datastore_api.update_description.call_count == 1
    assert mocked_pseudonym_service.pseudonymize.call_count == 0
    assert os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT.json")
    assert os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT.parquet")


def test_import_add_partitioned(mocked_datastore_api: MockedDatastoreApi):
    DATASET_NAME = "IMPORTABLE_ADD_PARTITIONED"
    add_partitioned_context = generate_job_context(
        operation=Operation.ADD,
        target=DATASET_NAME,
    )
    build_dataset_worker.run_worker(add_partitioned_context, Queue())
    assert mocked_datastore_api.update_job_status.call_count == 6
    assert mocked_datastore_api.update_description.call_count == 1
    assert os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT.json")
    assert os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT")


def test_import_add_invalid(mocked_datastore_api: MockedDatastoreApi):
    DATASET_NAME = "IMPORTABLE_ADD_INVALID"
    add_invalid_context = generate_job_context(
        operation=Operation.ADD,
        target=DATASET_NAME,
    )
    build_dataset_worker.run_worker(add_invalid_context, Queue())
    assert mocked_datastore_api.update_job_status.call_count == 3
    assert mocked_datastore_api.update_description.call_count == 0
    assert os.path.exists(INPUT_DIR / f"archive/{DATASET_NAME}.tar")
    assert not os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT.json")
    assert not os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT")


@pytest.fixture
def mocked_offline_datastore_api(mocker) -> MockedDatastoreApi:
    return MockedDatastoreApi(
        update_job_status=mocker.patch(
            "job_executor.adapter.datastore_api.update_job_status",
            side_effect=Exception("offline"),
        ),
        update_description=mocker.patch(
            "job_executor.adapter.datastore_api.update_description",
            side_effect=Exception("offline"),
        ),
    )


def test_import_add_datastore_api_down(
    mocked_offline_datastore_api: MockedDatastoreApi,
):
    DATASET_NAME = "IMPORTABLE_ADD"
    add_datastore_api_down_context = generate_job_context(
        operation=Operation.ADD, target=DATASET_NAME
    )
    with pytest.raises(Exception, match="offline"):
        build_dataset_worker.run_worker(add_datastore_api_down_context, Queue())
