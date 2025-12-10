import json
import os
import shutil
from pathlib import Path

from job_executor.adapter.fs import LocalStorageAdapter
from job_executor.adapter.fs.models.datastore_versions import (
    DatastoreVersions,
    DataStructureUpdate,
)


def load_json(file_path):
    return json.load(open(file_path, encoding="utf"))


DATASTORE_DIR = "tests/unit/resources/adapter/fs/TEST_DATASTORE"
METADATA_DIR = f"{DATASTORE_DIR}/datastore"
local_storage = LocalStorageAdapter(Path(DATASTORE_DIR))
DATASTORE_VERSIONS_PATH = f"{METADATA_DIR}/datastore_versions.json"


def setup_function():
    if os.path.isdir("tests/resources_backup"):
        shutil.rmtree("tests/resources_backup")

    shutil.copytree("tests/resources", "tests/resources_backup")


def teardown_function():
    shutil.rmtree("tests/resources")
    shutil.move("tests/resources_backup", "tests/resources")


def test_datastore_versions():
    datastore_versions = DatastoreVersions.model_validate(
        local_storage.datastore_dir.get_datastore_versions()
    )
    assert datastore_versions.model_dump(
        by_alias=True, exclude_none=True
    ) == load_json(DATASTORE_VERSIONS_PATH)


def test_add_new_release_version():
    datastore_versions = DatastoreVersions.model_validate(
        local_storage.datastore_dir.get_datastore_versions()
    )
    datastore_versions.add_new_release_version(
        [
            DataStructureUpdate(
                name="NEW_DATASET",
                description="Første publisering",
                operation="ADD",
                release_status="PENDING_RELEASE",
            )
        ],
        "new datastore version",
        "MAJOR",
    )
    assert len(datastore_versions.versions) == 3


def test_get_dataset_release_status():
    datastore_versions = DatastoreVersions.model_validate(
        local_storage.datastore_dir.get_datastore_versions()
    )
    assert (
        datastore_versions.get_dataset_release_status("SIVSTAND") == "RELEASED"
    )
    assert datastore_versions.get_dataset_release_status("INNTEKT") == "DELETED"
    assert (
        datastore_versions.get_dataset_release_status("DOES_NOT_EXIST") is None
    )
