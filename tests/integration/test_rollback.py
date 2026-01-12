import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from microdata_tools.packaging import os

from job_executor.adapter.datastore_api.models import (
    Job,
    JobParameters,
    JobStatus,
    Operation,
    UserInfo,
)
from job_executor.adapter.fs.models.datastore_versions import DatastoreVersion
from job_executor.domain import rollback
from tests.integration.common import (
    backup_resources,
    prepare_datastore,
    recover_resources_from_backup,
)

RESOURCES_DIR = Path("tests/integration/resources")
DATASTORE_DIR = RESOURCES_DIR / "datastores/TEST_DATASTORE"
WORKING_DIR = RESOURCES_DIR / "TEST_DATASTORE_working"
INPUT_DIR = RESOURCES_DIR / "TEST_DATASTORE_input"

user_info = UserInfo(
    user_id="1",
    first_name="Test",
    last_name="Testersen",
)


@dataclass
class MockedDatastoreApi:
    update_job_status: MagicMock
    get_datastore_directory: MagicMock


@pytest.fixture(autouse=True)
def mocked_datastore_api(mocker) -> MockedDatastoreApi:
    return MockedDatastoreApi(
        update_job_status=mocker.patch(
            "job_executor.adapter.datastore_api.update_job_status",
            return_value=None,
        ),
        get_datastore_directory=mocker.patch(
            "job_executor.adapter.datastore_api.get_datastore_directory",
            return_value=DATASTORE_DIR,
        ),
    )


@pytest.fixture(autouse=True)
def selected_datastore(request):
    return request.param


@pytest.fixture(autouse=True)
def set_up_resources(selected_datastore):
    """
    As we want to set up a different datastore directory
    for each test. We parameterize this autouse fixture with
    a "selected_datastore" parameter.
    We always move the selected datastore to the DATASTORE_DIR
    when setting up tests so this global can be reused.
    """
    assert selected_datastore
    backup_resources()
    if selected_datastore != DATASTORE_DIR:
        shutil.rmtree(DATASTORE_DIR)
        shutil.move(selected_datastore, DATASTORE_DIR)
    prepare_datastore(str(DATASTORE_DIR))
    yield
    recover_resources_from_backup()


@pytest.mark.parametrize(
    "selected_datastore",
    [RESOURCES_DIR / "datastores/ROLLBACK_BUMP_DATASTORE"],
    indirect=True,
)
def test_rollback_bump(mocked_datastore_api: MockedDatastoreApi):
    with open(
        DATASTORE_DIR / "datastore" / "tmp" / "draft_version.json", "r"
    ) as f:
        bump_manifesto = DatastoreVersion.model_validate(json.load(f))
    job = Job(
        job_id="job_id",
        datastore_rdn="TEST_DATASTORE",
        status=JobStatus.INITIATED,
        created_at="2022-10-26T12:00:00Z",
        created_by=user_info,
        parameters=JobParameters(
            operation=Operation.BUMP,
            target="DATASTORE",
            bump_manifesto=bump_manifesto,
            description="some description",
            bump_from_version="1.0.0",
            bump_to_version="2.0.0",
        ),
    )
    rollback.fix_interrupted_job(job)
    assert mocked_datastore_api.update_job_status.call_count == 1
    mocked_datastore_api.update_job_status.assert_called_with(
        job.job_id,
        JobStatus.FAILED,
        "Bump operation was interrupted and rolled back.",
    )
    metadata_dir = DATASTORE_DIR / "datastore"
    assert not os.path.exists(metadata_dir / "tmp")
    assert not os.path.exists(metadata_dir / "metadata_all__2_0_0.json")
    assert not os.path.exists(metadata_dir / "data_versions__2_0.json")


@pytest.mark.parametrize(
    "selected_datastore",
    [RESOURCES_DIR / "datastores/FIRST_BUMP_ROLLBACK_DATASTORE"],
    indirect=True,
)
def test_rollback_first_bump(mocked_datastore_api: MockedDatastoreApi):
    with open(
        DATASTORE_DIR / "datastore" / "tmp" / "draft_version.json", "r"
    ) as f:
        bump_manifesto = DatastoreVersion.model_validate(json.load(f))
    job = Job(
        job_id="job_id",
        datastore_rdn="TEST_DATASTORE",
        status=JobStatus.INITIATED,
        created_at="2022-10-26T12:00:00Z",
        created_by=user_info,
        parameters=JobParameters(
            operation=Operation.BUMP,
            target="DATASTORE",
            bump_manifesto=bump_manifesto,
            description="some description",
            bump_from_version="0.0.0",
            bump_to_version="1.0.0",
        ),
    )
    rollback.fix_interrupted_job(job)
    assert mocked_datastore_api.update_job_status.call_count == 1
    mocked_datastore_api.update_job_status.assert_called_with(
        job.job_id,
        JobStatus.FAILED,
        "Bump operation was interrupted and rolled back.",
    )
    metadata_dir = DATASTORE_DIR / "datastore"
    assert not os.path.exists(metadata_dir / "tmp")
    assert all(
        file
        in [
            "metadata_all__DRAFT.json",
            "draft_version.json",
            "datastore_versions.json",
        ]
        for file in os.listdir(metadata_dir)
    )
    assert not any(
        file in ["metadata_all__1_0_0.json", "data_versions__1_0.json"]
        for file in os.listdir(metadata_dir)
    )


@pytest.mark.parametrize(
    "selected_datastore",
    [DATASTORE_DIR],
    indirect=True,
)
def test_rollback_import_worker_jobs(mocked_datastore_api: MockedDatastoreApi):
    working_dir = Path(f"{str(DATASTORE_DIR)}_working")
    for dataset_name in ["BUILT_ADD", "BUILT_CHANGE", "BUILT_PATCH_METADATA"]:
        interrupted_add = Job(
            job_id="job_id",
            datastore_rdn="TEST_DATASTORE",
            status=JobStatus.PSEUDONYMIZING,
            created_at="2022-10-26T12:00:00Z",
            created_by=user_info,
            parameters=JobParameters(
                operation=Operation.ADD, target=dataset_name
            ),
        )
        rollback.fix_interrupted_job(interrupted_add)
        mocked_datastore_api.update_job_status.assert_called_with(
            interrupted_add.job_id,
            JobStatus.FAILED,
            "Job was failed due to an unexpected interruption",
        )
        assert not os.path.exists(
            working_dir / f"{dataset_name}__DRAFT.parquet"
        )
        assert not os.path.exists(working_dir / f"{dataset_name}.json")
