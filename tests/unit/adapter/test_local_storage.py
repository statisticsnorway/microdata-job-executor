import json
import os
import shutil
from pathlib import Path

import pytest

from job_executor.adapter.fs import LocalStorageAdapter
from job_executor.adapter.fs.models.datastore_versions import (
    DatastoreVersions,
    DraftVersion,
)
from job_executor.adapter.fs.models.metadata import (
    MetadataAll,
)
from job_executor.common.exceptions import LocalStorageError

DATASTORE_DIR = os.environ["DATASTORE_DIR"]
WORKING_DIR = DATASTORE_DIR + "_working"
DATASTORE_DATA_DIR = f"{DATASTORE_DIR}/data"

local_storage = LocalStorageAdapter(Path(DATASTORE_DIR))

DATASTORE_VERSIONS_PATH = f"{DATASTORE_DIR}/datastore/datastore_versions.json"
DRAFT_METADATA_ALL_PATH = f"{DATASTORE_DIR}/datastore/metadata_all__draft.json"
DRAFT_VERSION_PATH = f"{DATASTORE_DIR}/datastore/draft_version.json"
DATA_VERSIONS_PATH = f"{DATASTORE_DIR}/datastore/data_versions__1_0.json"
METADATA_ALL_PATH = f"{DATASTORE_DIR}/datastore/metadata_all__1_0_0.json"

DRAFT_DATASET_NAME = "UTDANNING"
DRAFT_DATA_PATH = f"{DATASTORE_DATA_DIR}/UTDANNING/UTDANNING__DRAFT.parquet"

DRAFT2_DATASET_NAME = "BRUTTO_INNTEKT"
RELEASED_DRAFT2_DATA_PATH = (
    f"{DATASTORE_DATA_DIR}/BRUTTO_INNTEKT/BRUTTO_INNTEKT__1_1"
)

WORKING_DIR_DATASET = "FOEDESTED"
MOVED_WORKING_DIR_DATASET_DATA_PATH = (
    f"{DATASTORE_DATA_DIR}/FOEDESTED/FOEDESTED__DRAFT.parquet"
)


def setup_function():
    if os.path.isdir("tests/resources_backup"):
        shutil.rmtree("tests/resources_backup")
    shutil.copytree("tests/resources", "tests/resources_backup")


def teardown_function():
    shutil.rmtree("tests/resources")
    shutil.move("tests/resources_backup", "tests/resources")


def read_json(file_path: str) -> dict:
    with open(file_path, encoding="utf-8") as f:
        return json.load(f)


def test_make_dataset_dir():
    local_storage.datastore_dir.make_dataset_dir(WORKING_DIR_DATASET)
    assert os.path.isdir(f"{DATASTORE_DATA_DIR}/{WORKING_DIR_DATASET}")


def test_get_data_versions():
    assert local_storage.datastore_dir.get_data_versions("1_0_0") == read_json(
        DATA_VERSIONS_PATH
    )


def test_write_data_versions():
    local_storage.datastore_dir.write_data_versions({}, "1_0_0")
    assert read_json(DATA_VERSIONS_PATH) == {}


def test_get_draft_version():
    assert isinstance(
        local_storage.datastore_dir.get_draft_version(), DraftVersion
    )


def test_write_draft_version():
    draft_version = local_storage.datastore_dir.get_draft_version()
    draft_version.description = "updated"
    local_storage.datastore_dir.write_draft_version(draft_version)
    assert (
        local_storage.datastore_dir.get_draft_version().description == "updated"
    )


def test_get_datastore_versions():
    assert isinstance(
        local_storage.datastore_dir.get_datastore_versions(), DatastoreVersions
    )


def test_write_datastore_versions():
    datastore_versions = local_storage.datastore_dir.get_datastore_versions()
    datastore_versions.description = "updated"
    local_storage.datastore_dir.write_datastore_versions(datastore_versions)
    assert (
        local_storage.datastore_dir.get_datastore_versions().description
        == "updated"
    )


def test_get_metadata_all():
    assert isinstance(
        local_storage.datastore_dir.get_metadata_all("1_0_0"), MetadataAll
    )


def test_write_metadata_all():
    metadata_all = local_storage.datastore_dir.get_metadata_all("1_0_0")
    metadata_all.data_structures = []
    local_storage.datastore_dir.write_metadata_all(metadata_all, "1_0_0")
    assert (
        local_storage.datastore_dir.get_metadata_all("1_0_0").data_structures
        == []
    )


def delete_parquet_draft():
    local_storage.datastore_dir.delete_parquet_draft(DRAFT_DATASET_NAME)
    assert not os.path.isfile(DRAFT_DATA_PATH)


def test_rename_parquet_draft_to_release():
    release_path = local_storage.datastore_dir.rename_parquet_draft_to_release(
        DRAFT2_DATASET_NAME, "1_1_0"
    )
    assert os.path.isdir(RELEASED_DRAFT2_DATA_PATH)
    assert release_path == f"{DRAFT2_DATASET_NAME}__1_1"


def test_move_working_dir_parquet_to_datastore():
    local_storage.datastore_dir.make_dataset_dir(WORKING_DIR_DATASET)
    local_storage.move_working_dir_parquet_to_datastore(WORKING_DIR_DATASET)
    assert os.path.isfile(MOVED_WORKING_DIR_DATASET_DATA_PATH)


def test_make_temp_directory():
    datastore_content = os.listdir(DATASTORE_DIR)
    local_storage.datastore_dir.save_temporary_backup()
    datastore_content_backup = os.listdir(DATASTORE_DIR)
    assert len(datastore_content) == 3
    assert len(datastore_content_backup) == 4
    tmp_dir = Path(DATASTORE_DIR) / "tmp"
    assert os.path.isdir(tmp_dir)
    tmp_actual_content = os.listdir(tmp_dir)
    tmp_expected_content = [
        "metadata_all__DRAFT.json",
        "datastore_versions.json",
        "draft_version.json",
    ]
    assert len(tmp_actual_content) == 3
    for content in tmp_expected_content:
        assert content in tmp_actual_content


def test_make_temp_directory_already_exists():
    local_storage.datastore_dir.save_temporary_backup()
    datastore_content = os.listdir(DATASTORE_DIR)
    assert "tmp" in datastore_content
    with pytest.raises(LocalStorageError) as e:
        local_storage.datastore_dir.save_temporary_backup()
    assert "tmp directory already exists" in str(e)


def test_archive_temp_directory():
    local_storage.datastore_dir.save_temporary_backup()
    datastore_content = os.listdir(DATASTORE_DIR)
    local_storage.datastore_dir.archive_temporary_backup()
    datastore_content_archived = os.listdir(DATASTORE_DIR)
    for dir in ["datastore", "data", "tmp", "vault"]:
        assert dir in datastore_content
    for dir in ["datastore", "data", "archive", "vault"]:
        assert dir in datastore_content_archived
    assert len(datastore_content) == 4
    assert len(datastore_content_archived) == 4
    assert not os.path.isdir(Path(DATASTORE_DIR) / "tmp")


def test_archived_temp_directory_unrecognized_files():
    local_storage.datastore_dir.save_temporary_backup()
    tmp_dir = Path(DATASTORE_DIR) / "tmp"
    assert os.path.isdir(tmp_dir)
    (tmp_dir / "newfile.txt").touch()

    with pytest.raises(LocalStorageError) as e:
        local_storage.datastore_dir.archive_temporary_backup()
    assert "Found unrecognized files" in str(e)


def test_archive_or_delete_non_existent_tmp_dir():
    with pytest.raises(LocalStorageError) as e:
        local_storage.datastore_dir.archive_temporary_backup()
    assert "Could not find a tmp directory to archive." in str(e)
    with pytest.raises(LocalStorageError) as e:
        local_storage.datastore_dir.delete_temporary_backup()
    assert "Could not find a tmp directory to delete." in str(e)
