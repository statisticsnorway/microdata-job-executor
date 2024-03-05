import os
import json
import shutil
import pytest
from job_executor.exception import (
    BumpException,
    ExistingDraftException,
    NoSuchDraftException,
)
from job_executor.model import (
    DatastoreVersion,
    DraftVersion,
    DataStructureUpdate,
)


def load_json(file_path):
    return json.load(open(file_path, encoding="utf"))


TEST_DIR = "tests/resources/model/datastore_version"
DRAFT_VERSION_IDENTICAL = load_json(f"{TEST_DIR}/draft_version_identical.json")
DRAFT_VERSION_ONLY_PENDING = load_json(
    f"{TEST_DIR}/draft_version_only_pending.json"
)
DRAFT_VERSION_ADDED_PENDING = load_json(
    f"{TEST_DIR}/draft_version_added_pending.json"
)

DATASTORE_DIR = f'{os.environ["DATASTORE_DIR"]}/datastore'
DRAFT_VERSION_PATH = f"{DATASTORE_DIR}/draft_version.json"
DATASTORE_VERSION = {
    "version": "0.1.0.1635299291",
    "description": "Første release",
    "releaseTime": 1635299291,
    "languageCode": "no",
    "dataStructureUpdates": [
        {
            "name": "INNTEKT",
            "description": "Første publisering",
            "operation": "ADD",
            "releaseStatus": "RELEASED",
        },
        {
            "name": "SIVSTAND",
            "description": "Første publisering",
            "operation": "PATCH_METADATA",
            "releaseStatus": "RELEASED",
        },
    ],
    "updateType": "MINOR",
}


def setup_function():
    if os.path.isdir("tests/resources_backup"):
        shutil.rmtree("tests/resources_backup")

    shutil.copytree("tests/resources", "tests/resources_backup")


def teardown_function():
    shutil.rmtree("tests/resources")
    shutil.move("tests/resources_backup", "tests/resources")


def test_datastore_version():
    datastore_version = DatastoreVersion(**DATASTORE_VERSION)
    assert datastore_version.model_dump(by_alias=True, exclude_none=True) == DATASTORE_VERSION


def test_get_dataset_release_status():
    datastore_version = DatastoreVersion(**DATASTORE_VERSION)

    release_status = datastore_version.get_dataset_release_status("INNTEKT")
    assert release_status == "RELEASED"

    release_status = datastore_version.get_dataset_release_status(
        "DOES_NOT_EXIST"
    )
    assert release_status is None


def test_draft_version():
    draft_version = DraftVersion()
    assert draft_version.model_dump(by_alias=True, exclude_none=True) == load_json(DRAFT_VERSION_PATH)


def test_draft_version_delete_draft():
    draft_version = DraftVersion()
    release_time = draft_version.release_time
    version = draft_version.version
    draft_version.delete_draft("BRUTTO_INNTEKT")
    draft_version_file = load_json(DRAFT_VERSION_PATH)
    update_names = [
        update["name"] for update in draft_version_file["dataStructureUpdates"]
    ]
    assert "BRUTTO_INNTEKT" not in update_names
    assert len(update_names) == 1
    assert release_time != draft_version.release_time
    assert version != draft_version.version

    with pytest.raises(NoSuchDraftException) as e:
        draft_version.delete_draft("NO_SUCH_DATASET")
    assert "Can't delete draft for NO_SUCH_DATASET" in str(e)


def test_add_draft_version_already_existing_dataset():
    draft_version = DraftVersion()
    with pytest.raises(ExistingDraftException) as e:
        draft_version.add(
            DataStructureUpdate(
                name="UTDANNING",
                description="",
                operation="ADD",
                releaseStatus="DRAFT",
            )
        )
    assert "Draft for UTDANNING already exists" in str(e)


def test_draft_version_validate_bump_manifesto():
    draft_version = DraftVersion()

    bump_manifesto = DatastoreVersion(**DRAFT_VERSION_IDENTICAL)
    assert draft_version.validate_bump_manifesto(bump_manifesto)

    bump_manifesto_only_pending = DatastoreVersion(
        **DRAFT_VERSION_ONLY_PENDING
    )
    assert draft_version.validate_bump_manifesto(bump_manifesto_only_pending)

    bump_manifesto_added_pending = DatastoreVersion(
        **DRAFT_VERSION_ADDED_PENDING
    )
    assert not draft_version.validate_bump_manifesto(
        bump_manifesto_added_pending
    )


def test_draft_version_release_pending():
    draft_version = DraftVersion()
    release_time = draft_version.release_time
    version = draft_version.version
    updates, update_type = draft_version.release_pending()
    assert update_type == "MINOR"
    assert [update.model_dump() for update in updates] == [
        {
            "description": "Første publisering",
            "name": "BRUTTO_INNTEKT",
            "operation": "ADD",
            "release_status": "PENDING_RELEASE",
        }
    ]
    assert release_time != draft_version.release_time
    assert version != draft_version.version
    with pytest.raises(BumpException):
        draft_version.release_pending()


def test_set_draft_release_status():
    draft_version = DraftVersion()
    release_time = draft_version.release_time
    version = draft_version.version
    draft_version.set_draft_release_status("UTDANNING", "PENDING_RELEASE")
    for update in draft_version:
        if update.name == "UTDANNING":
            assert update.release_status == "PENDING_RELEASE"
    assert release_time != draft_version.release_time
    assert version != draft_version.version

    draft_version.set_draft_release_status("UTDANNING", "DRAFT")
    for update in draft_version:
        if update.name == "UTDANNING":
            assert update.release_status == "DRAFT"
