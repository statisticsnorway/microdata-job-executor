import json
import os
import shutil
from pathlib import Path
from unittest.mock import MagicMock, Mock

from requests_mock import Mocker as RequestsMocker

from job_executor.domain import datastores
from job_executor.domain.datastores import Datastore
from job_executor.model import DatastoreVersion
from tests.unit.test_util import get_dir_list_from_dir, get_file_list_from_dir

test_datastore = Datastore()
JOB_SERVICE_URL = os.getenv("JOB_SERVICE_URL")
JOB_ID = "123-123-123-123"
DATASTORE_DIR = os.environ["DATASTORE_DIR"]
WORKING_DIR = os.environ["WORKING_DIR"]
DATASTORE_DATA_DIR = f"{DATASTORE_DIR}/data"
DATASTORE_METADATA_DIR = f"{DATASTORE_DIR}/metadata"
DATASTORE_INFO_DIR = f"{DATASTORE_DIR}/datastore"
DATA_VERSIONS = f"{DATASTORE_INFO_DIR}/datastore_versions.json"
DRAFT_VERSION = f"{DATASTORE_INFO_DIR}/draft_version.json"
METADATA_ALL_DRAFT = f"{DATASTORE_INFO_DIR}/metadata_all__DRAFT.json"
DATASTORE_ARCHIVE_DIR = f"{DATASTORE_DIR}/archive"


def working_dir_metadata_draft_path(name: str):
    return f"{WORKING_DIR}/{name}__DRAFT.json"


def draft_data_path(name: str):
    return f"{DATASTORE_DATA_DIR}/{name}/{name}__DRAFT.parquet"


def partitioned_draft_data_path(name: str):
    return f"{DATASTORE_DATA_DIR}/{name}/{name}__DRAFT"


def setup_module():
    shutil.copytree("tests/resources", "tests/resources_backup")


def teardown_module():
    shutil.rmtree("tests/resources")
    shutil.move("tests/resources_backup", "tests/resources")


def test_patch_metadata(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    DATASET_NAME = "SIVSTAND"
    DESCRIPTION = "oppdaterte metadata"
    datastores.patch_metadata(test_datastore, JOB_ID, DATASET_NAME, DESCRIPTION)
    assert len(requests_mock.request_history) == 2
    assert not os.path.exists(working_dir_metadata_draft_path(DATASET_NAME))
    with open(METADATA_ALL_DRAFT, encoding="utf-8") as f:
        metadata_all_draft = json.load(f)
    with open(DRAFT_VERSION, encoding="utf-8") as f:
        draft_version = json.load(f)
    assert {
        "name": DATASET_NAME,
        "description": DESCRIPTION,
        "operation": "PATCH_METADATA",
        "releaseStatus": "DRAFT",
    } in draft_version["dataStructureUpdates"]
    assert draft_version["releaseTime"] > 1_000_000
    assert any(
        draft["name"] == DATASET_NAME
        for draft in metadata_all_draft["dataStructures"]
    )
    assert test_datastore.metadata_all_latest is not None
    released_metadata = next(
        metadata.model_dump(by_alias=True, exclude_none=True)
        for metadata in test_datastore.metadata_all_latest
        if metadata.name == DATASET_NAME
    )
    draft_metadata = next(
        metadata.model_dump(by_alias=True, exclude_none=True)
        for metadata in test_datastore.metadata_all_draft
        if metadata.name == DATASET_NAME
    )
    assert released_metadata != draft_metadata


def test_add(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    DATASET_NAME = "FOEDESTED"
    DESCRIPTION = "første publisering"
    with open(
        working_dir_metadata_draft_path(DATASET_NAME), encoding="utf-8"
    ) as f:
        foedested_metadata = json.load(f)
    datastores.add(test_datastore, JOB_ID, DATASET_NAME, DESCRIPTION)
    assert len(requests_mock.request_history) == 2
    assert not os.path.exists(working_dir_metadata_draft_path(DATASET_NAME))
    assert os.path.exists(draft_data_path(DATASET_NAME))
    with open(METADATA_ALL_DRAFT, encoding="utf-8") as f:
        metadata_all_draft = json.load(f)
    with open(DRAFT_VERSION, encoding="utf-8") as f:
        draft_version = json.load(f)

    assert {
        "name": DATASET_NAME,
        "description": DESCRIPTION,
        "operation": "ADD",
        "releaseStatus": "DRAFT",
    } in draft_version["dataStructureUpdates"]
    assert draft_version["releaseTime"] > 1_000_000
    assert foedested_metadata in metadata_all_draft["dataStructures"]


def test_add_previously_deleted(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    DATASET_NAME = "INNTEKT"
    DESCRIPTION = "Ny variabel tidligere DELETED"
    datastores.add(test_datastore, JOB_ID, DATASET_NAME, DESCRIPTION)
    assert len(requests_mock.request_history) == 2
    assert not os.path.exists(working_dir_metadata_draft_path(DATASET_NAME))
    assert os.path.exists(partitioned_draft_data_path(DATASET_NAME))
    with open(METADATA_ALL_DRAFT, encoding="utf-8") as f:
        json.load(f)
    with open(DRAFT_VERSION, encoding="utf-8") as f:
        draft_version = json.load(f)

    assert {
        "name": DATASET_NAME,
        "description": DESCRIPTION,
        "operation": "ADD",
        "releaseStatus": "DRAFT",
    } in draft_version["dataStructureUpdates"]
    assert draft_version["releaseTime"] > 1_000_000
    assert any(
        [
            draft.name == DATASET_NAME
            for draft in test_datastore.metadata_all_draft
        ]
    )


def test_change(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    DATASET_NAME = "FOEDSELSVEKT"
    DESCRIPTION = "oppdaterte data"
    with open(
        working_dir_metadata_draft_path(DATASET_NAME), encoding="utf-8"
    ) as f:
        foedested_metadata = json.load(f)
    datastores.change(test_datastore, JOB_ID, DATASET_NAME, DESCRIPTION)
    assert len(requests_mock.request_history) == 2
    assert not os.path.exists(working_dir_metadata_draft_path(DATASET_NAME))
    assert os.path.exists(draft_data_path(DATASET_NAME))
    with open(METADATA_ALL_DRAFT, encoding="utf-8") as f:
        metadata_all_draft = json.load(f)
    with open(DRAFT_VERSION, encoding="utf-8") as f:
        draft_version = json.load(f)

    assert {
        "name": DATASET_NAME,
        "description": DESCRIPTION,
        "operation": "CHANGE",
        "releaseStatus": "DRAFT",
    } in draft_version["dataStructureUpdates"]
    assert draft_version["releaseTime"] > 1_000_000
    assert foedested_metadata in metadata_all_draft["dataStructures"]


def test_delete_draft(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    DATASET_NAME = "UTDANNING"
    datastores.delete_draft(
        test_datastore, JOB_ID, DATASET_NAME, rollback_remove=False
    )
    assert len(requests_mock.request_history) == 2

    with open(DRAFT_VERSION, encoding="utf-8") as f:
        draft_version = json.load(f)

    assert not os.path.exists(draft_data_path(DATASET_NAME))
    assert not os.path.exists(partitioned_draft_data_path(DATASET_NAME))
    assert not [
        update
        for update in draft_version["dataStructureUpdates"]
        if update["name"] == DATASET_NAME
    ]
    assert draft_version["releaseTime"] > 1_000_000


def test_set_draft_release_status(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    DATASET_NAME = "FOEDESTED"
    DESCRIPTION = "første publisering"
    NEW_STATUS = "PENDING_RELEASE"
    datastores.set_draft_release_status(
        test_datastore, JOB_ID, DATASET_NAME, NEW_STATUS
    )
    assert len(requests_mock.request_history) == 2
    with open(DRAFT_VERSION, encoding="utf-8") as f:
        draft_version = json.load(f)

    assert {
        "name": DATASET_NAME,
        "description": DESCRIPTION,
        "operation": "ADD",
        "releaseStatus": "PENDING_RELEASE",
    } in draft_version["dataStructureUpdates"]
    # Try again after a possible interrupt
    datastores.set_draft_release_status(
        test_datastore, JOB_ID, DATASET_NAME, NEW_STATUS
    )
    assert len(requests_mock.request_history) == 4
    assert requests_mock.request_history[3].json() == {
        "status": "completed",
        "log": "Status already set to PENDING_RELEASE",
    }
    with open(DRAFT_VERSION, encoding="utf-8") as f:
        draft_version = json.load(f)

    assert {
        "name": DATASET_NAME,
        "description": DESCRIPTION,
        "operation": "ADD",
        "releaseStatus": "PENDING_RELEASE",
    } in draft_version["dataStructureUpdates"]
    assert draft_version["releaseTime"] > 1_000_000


def test_bump_datastore_minor(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    with open(DRAFT_VERSION, encoding="utf-8") as f:
        bump_manifesto = DatastoreVersion(**json.load(f))

    datastores.bump_version(
        test_datastore, JOB_ID, bump_manifesto, "description"
    )
    assert len(requests_mock.request_history) == 2

    with open(DRAFT_VERSION, encoding="utf-8") as f:
        draft_after_bump = json.load(f)
    assert draft_after_bump["releaseTime"] > 1_000_000
    assert draft_after_bump["dataStructureUpdates"] == [
        {
            "description": "oppdaterte metadata",
            "name": "SIVSTAND",
            "operation": "PATCH_METADATA",
            "releaseStatus": "DRAFT",
        },
        {
            "description": "Ny variabel tidligere DELETED",
            "name": "INNTEKT",
            "operation": "ADD",
            "releaseStatus": "DRAFT",
        },
        {
            "description": "oppdaterte data",
            "name": "FOEDSELSVEKT",
            "operation": "CHANGE",
            "releaseStatus": "DRAFT",
        },
    ]
    with open(
        f"{DATASTORE_DIR}/datastore/metadata_all__2_0_0.json", encoding="utf-8"
    ) as f:
        previous_metadata_all = json.load(f)
    with open(
        f"{DATASTORE_DIR}/datastore/metadata_all__2_1_0.json", encoding="utf-8"
    ) as f:
        released_metadata_all = json.load(f)
    assert (
        len(released_metadata_all["dataStructures"])
        - len(previous_metadata_all["dataStructures"])
    ) == 2
    with open(
        f"{DATASTORE_DIR}/datastore/datastore_versions.json", encoding="utf-8"
    ) as f:
        datastore_versions_json = json.load(f)
    assert datastore_versions_json["versions"][0]["version"] == "2.1.0.0"
    with open(
        f"{DATASTORE_DIR}/datastore/data_versions__2_1.json", encoding="utf-8"
    ) as f:
        data_versions = json.load(f)
    assert data_versions == {
        "BRUTTO_INNTEKT": "BRUTTO_INNTEKT__2_1",
        "FOEDESTED": "FOEDESTED__2_1.parquet",
        "FOEDSELSVEKT": "FOEDSELSVEKT__1_0.parquet",
        "KJOENN": "KJOENN__1_0.parquet",
        "SIVSTAND": "SIVSTAND__1_0.parquet",
    }
    assert len(get_file_list_from_dir(Path(DATASTORE_ARCHIVE_DIR))) == 1
    assert len(get_dir_list_from_dir(Path(DATASTORE_ARCHIVE_DIR))) == 1


def test_remove(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    DATASET_NAME = "KJOENN"
    DESCRIPTION = "Fjernet variabel"
    datastores.remove(test_datastore, JOB_ID, DATASET_NAME, DESCRIPTION)
    assert len(requests_mock.request_history) == 2

    with open(DRAFT_VERSION, encoding="utf-8") as f:
        draft_version = json.load(f)

    assert {
        "name": DATASET_NAME,
        "description": DESCRIPTION,
        "operation": "REMOVE",
        "releaseStatus": "PENDING_DELETE",
    } in draft_version["dataStructureUpdates"]
    assert draft_version["releaseTime"] > 1_000_000


def test_bump_datastore_major(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    datastores.set_draft_release_status(
        test_datastore, JOB_ID, "FOEDSELSVEKT", "PENDING_RELEASE"
    )
    assert len(requests_mock.request_history) == 2
    with open(DRAFT_VERSION, encoding="utf-8") as f:
        bump_manifesto = DatastoreVersion(**json.load(f))
    datastores.bump_version(
        test_datastore, JOB_ID, bump_manifesto, "description"
    )
    assert len(requests_mock.request_history) == 4

    with open(DRAFT_VERSION, encoding="utf-8") as f:
        draft_after_bump = json.load(f)
    assert draft_after_bump["releaseTime"] > 1_000_000
    assert draft_after_bump["dataStructureUpdates"] == [
        {
            "description": "oppdaterte metadata",
            "name": "SIVSTAND",
            "operation": "PATCH_METADATA",
            "releaseStatus": "DRAFT",
        },
        {
            "description": "Ny variabel tidligere DELETED",
            "name": "INNTEKT",
            "operation": "ADD",
            "releaseStatus": "DRAFT",
        },
    ]
    with open(
        f"{DATASTORE_DIR}/datastore/metadata_all__2_1_0.json", encoding="utf-8"
    ) as f:
        previous_metadata_all = json.load(f)
    with open(
        f"{DATASTORE_DIR}/datastore/metadata_all__3_0_0.json", encoding="utf-8"
    ) as f:
        released_metadata_all = json.load(f)
    with open(
        f"{DATASTORE_DIR}/datastore/datastore_versions.json", encoding="utf-8"
    ) as f:
        datastore_versions_json = json.load(f)
    assert datastore_versions_json["versions"][0]["version"] == "3.0.0.0"
    assert (
        len(released_metadata_all["dataStructures"])
        == len(previous_metadata_all["dataStructures"]) - 1
    )  # Removed KJOENN in latest metadata_all
    with open(
        f"{DATASTORE_DIR}/datastore/data_versions__3_0.json", encoding="utf-8"
    ) as f:
        data_versions = json.load(f)
    assert data_versions == {
        "BRUTTO_INNTEKT": "BRUTTO_INNTEKT__2_1",
        "FOEDESTED": "FOEDESTED__2_1.parquet",
        "FOEDSELSVEKT": "FOEDSELSVEKT__3_0.parquet",
        "SIVSTAND": "SIVSTAND__1_0.parquet",
    }
    assert len(get_file_list_from_dir(Path(DATASTORE_ARCHIVE_DIR))) == 2
    assert len(get_dir_list_from_dir(Path(DATASTORE_ARCHIVE_DIR))) == 2


def test_delete_draft_after_interrupt(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    DATASET_NAME = "SIVSTAND"
    # Previous interrupted run deleted metadata
    test_datastore.metadata_all_draft.data_structures = [
        draft
        for draft in test_datastore.metadata_all_draft.data_structures
        if draft.name != DATASET_NAME
    ]
    datastores.delete_draft(
        test_datastore, JOB_ID, DATASET_NAME, rollback_remove=False
    )
    assert len(requests_mock.request_history) == 2
    assert requests_mock.request_history[1].json() == {"status": "completed"}
    with open(DRAFT_VERSION, encoding="utf-8") as f:
        draft_version = json.load(f)

    assert not os.path.exists(draft_data_path(DATASET_NAME))
    assert not [
        update
        for update in draft_version["dataStructureUpdates"]
        if update["name"] == DATASET_NAME
    ]


def test_invalid_bump_manifesto_archived_tmp_dir(
    requests_mock: RequestsMocker,
):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    datastores.set_draft_release_status(
        test_datastore, JOB_ID, "INNTEKT", "PENDING_RELEASE"
    )
    with open(DRAFT_VERSION, encoding="utf-8") as f:
        bump_manifesto = DatastoreVersion(**json.load(f))
    # introduce a change in bump manifesto
    bump_manifesto.data_structure_updates = [
        ds
        for ds in bump_manifesto.data_structure_updates
        if ds.release_status == "DRAFT"
    ]
    datastores.bump_version(
        test_datastore, JOB_ID, bump_manifesto, "description"
    )
    assert not os.path.exists(Path(DATASTORE_DIR) / "tmp")
    assert len(get_dir_list_from_dir(Path(DATASTORE_ARCHIVE_DIR))) == 3


def test_failed_bump(
    requests_mock: RequestsMocker,
):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    test_datastore.latest_version_number = Mock(side_effect=Exception())
    with open(DRAFT_VERSION, encoding="utf-8") as f:
        bump_manifesto = DatastoreVersion(**json.load(f))
    datastores.bump_version(
        test_datastore, JOB_ID, bump_manifesto, "description"
    )


def test_rollback_of_remove_operation(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    DATASET_NAME = "FOEDSELSVEKT"
    DESCRIPTION = "Setter til remove"

    datastores.remove(test_datastore, JOB_ID, DATASET_NAME, DESCRIPTION)
    datastores.delete_draft(
        test_datastore, JOB_ID, DATASET_NAME, rollback_remove=True
    )
    assert len(requests_mock.request_history) == 4
    with open(DRAFT_VERSION, encoding="utf-8") as f:
        draft_version = json.load(f)
    assert not [
        update
        for update in draft_version["dataStructureUpdates"]
        if update["name"] == DATASET_NAME
    ]
    assert draft_version["releaseTime"] > 1_000_000


def test_no_rollback(requests_mock: RequestsMocker):
    requests_mock.put(
        f"{JOB_SERVICE_URL}/jobs/{JOB_ID}", json={"message": "OK"}
    )
    DATASET_NAME = "FOEDSELSVEKT"
    DESCRIPTION = "Setter til remove"

    datastores.remove(test_datastore, JOB_ID, DATASET_NAME, DESCRIPTION)
    datastores.delete_draft(
        test_datastore, JOB_ID, DATASET_NAME, rollback_remove=False
    )
    assert len(requests_mock.request_history) == 4
    with open(DRAFT_VERSION, encoding="utf-8") as f:
        draft_version = json.load(f)
    assert {
        "name": DATASET_NAME,
        "description": DESCRIPTION,
        "operation": "REMOVE",
        "releaseStatus": "PENDING_DELETE",
    } in draft_version["dataStructureUpdates"]
    assert draft_version["releaseTime"] > 1_000_000
