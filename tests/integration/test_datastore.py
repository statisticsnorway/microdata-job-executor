import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from job_executor.adapter.datastore_api.models import (
    Job,
    JobParameters,
    JobStatus,
    Operation,
    ReleaseStatus,
    UserInfo,
)
from job_executor.adapter.fs import LocalStorageAdapter
from job_executor.adapter.fs.models.datastore_versions import DatastoreVersion
from job_executor.adapter.fs.models.metadata import Metadata
from job_executor.domain import datastores
from job_executor.domain.models import JobContext
from tests.integration.common import (
    backup_resources,
    prepare_datastore,
    recover_resources_from_backup,
)

RESOURCES_DIR = Path("tests/integration/resources")
DATASTORE_DIR = RESOURCES_DIR / "datastores/TEST_DATASTORE"
WORKING_DIR = RESOURCES_DIR / "TEST_DATASTORE_working"
INPUT_DIR = RESOURCES_DIR / "TEST_DATASTORE_input"


@dataclass
class MockedDatastoreApi:
    update_job_status: MagicMock


@pytest.fixture(autouse=True)
def mocked_datastore_api(mocker) -> MockedDatastoreApi:
    return MockedDatastoreApi(
        update_job_status=mocker.patch(
            "job_executor.adapter.datastore_api.update_job_status",
            return_value=None,
        )
    )


@pytest.fixture(autouse=True)
def set_up_resources():
    backup_resources()
    prepare_datastore(str(DATASTORE_DIR))
    yield
    recover_resources_from_backup()


def generate_job_context(
    operation: Operation,
    target: str,
    *,
    bump_manifesto: DatastoreVersion | None = None,
    release_status: ReleaseStatus | None = None,
) -> JobContext:
    def generate_job_parameters(
        operation: Operation,
        target: str,
        bump_manifesto: DatastoreVersion | None,
        release_status: ReleaseStatus | None,
    ) -> JobParameters:
        match operation:
            case Operation.BUMP:
                return JobParameters(
                    bump_manifesto=bump_manifesto,
                    description="some description",
                    bump_from_version="1.0.0",
                    bump_to_version="2.0.0",  # todo
                    target=target,
                    operation=operation,
                )
            case Operation.ADD | Operation.PATCH_METADATA | Operation.CHANGE:
                return JobParameters(
                    operation=operation, target=target, description="importing"
                )
            case Operation.SET_STATUS:
                return JobParameters(
                    operation=operation,
                    target=target,
                    release_status=release_status,
                )
            case Operation.DELETE_DRAFT:
                return JobParameters(
                    operation=operation,
                    target=target,
                )
        raise AssertionError("Could not generate job parameteres")

    return JobContext(
        handler="worker",
        local_storage=LocalStorageAdapter(DATASTORE_DIR),
        job_size=100,
        job=Job(
            job_id="1",
            datastore_rdn="TEST_DATASTORE",
            status=JobStatus.QUEUED,
            parameters=generate_job_parameters(
                operation, target, bump_manifesto, release_status
            ),
            created_at=datetime.now(tz=UTC).isoformat(),
            created_by=UserInfo(
                user_id="1", first_name="Test", last_name="Testerson"
            ),
        ),
    )


def _get_metadata_from_draft(
    job_context: JobContext, dataset_name: str
) -> Metadata:
    datastore_dir = job_context.local_storage.datastore_dir
    return next(
        ds
        for ds in datastore_dir.get_metadata_all_draft().data_structures
        if ds.name == dataset_name
    )


def test_import_built_patch(mocked_datastore_api: MockedDatastoreApi):
    DATASET_NAME = "BUILT_PATCH_METADATA"
    job_context = generate_job_context(
        operation=Operation.PATCH_METADATA,
        target=DATASET_NAME,
    )
    released_metadata = _get_metadata_from_draft(job_context, DATASET_NAME)
    datastores.patch_metadata(job_context)
    patched_metadata = _get_metadata_from_draft(job_context, DATASET_NAME)
    assert released_metadata != patched_metadata
    assert mocked_datastore_api.update_job_status.call_count == 2
    metadata_all_draft = (
        job_context.local_storage.datastore_dir.get_metadata_all_draft()
    )
    assert DATASET_NAME in [
        ds.name for ds in metadata_all_draft.data_structures
    ]
    assert not os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT.json")


def test_import_built_add(mocked_datastore_api: MockedDatastoreApi):
    DATASET_NAME = "BUILT_ADD"
    job_context = generate_job_context(
        operation=Operation.ADD,
        target=DATASET_NAME,
    )
    datastores.add(job_context)
    assert mocked_datastore_api.update_job_status.call_count == 2
    assert _get_metadata_from_draft(job_context, DATASET_NAME)
    assert not os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT.json")
    assert not os.path.exists(
        DATASTORE_DIR / f"data/{DATASET_NAME}__DRAFT.parquet"
    )
    assert os.path.exists(DATASTORE_DIR / f"data/{DATASET_NAME}")


def test_import_built_change(mocked_datastore_api: MockedDatastoreApi):
    DATASET_NAME = "BUILT_CHANGE"
    job_context = generate_job_context(
        operation=Operation.CHANGE,
        target=DATASET_NAME,
    )
    released_metadata = _get_metadata_from_draft(job_context, DATASET_NAME)
    datastores.change(job_context)
    changed_metadata = _get_metadata_from_draft(job_context, DATASET_NAME)
    assert changed_metadata != released_metadata
    assert mocked_datastore_api.update_job_status.call_count == 2
    metadata_all_draft = (
        job_context.local_storage.datastore_dir.get_metadata_all_draft()
    )
    assert DATASET_NAME in [
        ds.name for ds in metadata_all_draft.data_structures
    ]
    assert not os.path.exists(WORKING_DIR / f"{DATASET_NAME}__DRAFT.json")
    assert not os.path.exists(
        DATASTORE_DIR / f"data/{DATASET_NAME}__DRAFT.parquet"
    )


def test_bump_patch(mocked_datastore_api: MockedDatastoreApi):
    DATASET_NAME = "DRAFT_PATCH_METADATA"
    set_status_job_context = generate_job_context(
        operation=Operation.SET_STATUS,
        target=DATASET_NAME,
        release_status=ReleaseStatus.PENDING_RELEASE,
    )
    datastores.set_draft_release_status(set_status_job_context)
    draft_version = (
        set_status_job_context.local_storage.datastore_dir.get_draft_version()
    )
    assert (
        draft_version.get_dataset_release_status(DATASET_NAME)
        == "PENDING_RELEASE"
    )
    assert mocked_datastore_api.update_job_status.call_count == 2
    bump_job_context = generate_job_context(
        operation=Operation.BUMP,
        target="DATASTORE",
        bump_manifesto=draft_version,
    )
    datastores.bump_version(bump_job_context)
    assert mocked_datastore_api.update_job_status.call_count == 4


def test_bump_minor(mocked_datastore_api: MockedDatastoreApi):
    DATASET_NAME = "DRAFT_ADD"
    set_status_job_context = generate_job_context(
        operation=Operation.SET_STATUS,
        target=DATASET_NAME,
        release_status=ReleaseStatus.PENDING_RELEASE,
    )
    datastores.set_draft_release_status(set_status_job_context)
    assert mocked_datastore_api.update_job_status.call_count == 2
    draft_version = (
        set_status_job_context.local_storage.datastore_dir.get_draft_version()
    )
    assert (
        draft_version.get_dataset_release_status(DATASET_NAME)
        == "PENDING_RELEASE"
    )
    bump_job_context = generate_job_context(
        operation=Operation.BUMP,
        target="DATASTORE",
        bump_manifesto=draft_version,
    )
    datastores.bump_version(bump_job_context)
    assert mocked_datastore_api.update_job_status.call_count == 4


def test_bump_major(mocked_datastore_api: MockedDatastoreApi):
    DATASET_NAME = "DRAFT_CHANGE"
    set_status_job_context = generate_job_context(
        operation=Operation.SET_STATUS,
        target=DATASET_NAME,
        release_status=ReleaseStatus.PENDING_RELEASE,
    )
    datastores.set_draft_release_status(set_status_job_context)
    assert mocked_datastore_api.update_job_status.call_count == 2
    draft_version = (
        set_status_job_context.local_storage.datastore_dir.get_draft_version()
    )
    assert (
        draft_version.get_dataset_release_status(DATASET_NAME)
        == "PENDING_RELEASE"
    )
    bump_job_context = generate_job_context(
        operation=Operation.BUMP,
        target="DATASTORE",
        bump_manifesto=draft_version,
    )
    datastores.bump_version(bump_job_context)
    assert mocked_datastore_api.update_job_status.call_count == 4


def test_delete_draft(mocked_datastore_api: MockedDatastoreApi):
    DATASET_NAME = "DRAFT_CHANGE"
    delete_draft_job_context = generate_job_context(
        operation=Operation.DELETE_DRAFT,
        target=DATASET_NAME,
    )
    datastores.delete_draft(delete_draft_job_context)
    draft_version = (
        delete_draft_job_context.local_storage.datastore_dir.get_draft_version()
    )
    assert draft_version.get_dataset_release_status(DATASET_NAME) is None
    assert mocked_datastore_api.update_job_status.call_count == 2
