import json
import os
import shutil

from job_executor.adapter.fs.models.metadata import (
    Metadata,
    MetadataAll,
)


def load_json(file_path):
    return json.load(open(file_path, encoding="utf"))


TEST_DIR = "tests/unit/resources/adapter/fs/model/metadata"
METADATA_ALL_PATH = f"{TEST_DIR}/metadata_all.json"

ENUMERATED_METADATA = load_json(f"{TEST_DIR}/enumerated_metadata.json")
DESCRIBED_METADATA = load_json(f"{TEST_DIR}/described_metadata.json")

METADATA_IN_DATASTORE = load_json(f"{TEST_DIR}/metadata.json")
UPDATED_METADATA = load_json(f"{TEST_DIR}/updated_metadata.json")
PATCHED_METADATA = load_json(f"{TEST_DIR}/patched_metadata.json")

PATCH_UNIT_TYPE_METADATA_IN_DATASTORE = load_json(
    f"{TEST_DIR}/patch_unit_type/metadata.json"
)
PATCH_UNIT_TYPE_UPDATED_METADATA = load_json(
    f"{TEST_DIR}/patch_unit_type/updated_metadata.json"
)
PATCH_UNIT_TYPE_PATCHED_METADATA = load_json(
    f"{TEST_DIR}/patch_unit_type/patched_metadata.json"
)


def setup_module():
    if os.path.isdir("tests/resources_backup"):
        shutil.rmtree("tests/resources_backup")

    shutil.copytree("tests/resources", "tests/resources_backup")


def teardown_module():
    shutil.rmtree("tests/resources")
    shutil.move("tests/resources_backup", "tests/resources")


def test_metadata_all():
    metadata_all = MetadataAll(**load_json(METADATA_ALL_PATH))
    assert metadata_all.model_dump(
        by_alias=True, exclude_none=True
    ) == load_json(METADATA_ALL_PATH)


def test_metadata():
    enumerated_metadata = Metadata(**ENUMERATED_METADATA)
    assert (
        enumerated_metadata.model_dump(by_alias=True, exclude_none=True)
        == ENUMERATED_METADATA
    )

    described_metadata = Metadata(**DESCRIBED_METADATA)
    assert (
        described_metadata.model_dump(by_alias=True, exclude_none=True)
        == DESCRIBED_METADATA
    )


def test_patch():
    metadata_in_datastore = Metadata(**METADATA_IN_DATASTORE)
    updated_metadata = Metadata(**UPDATED_METADATA)
    patched_metadata = metadata_in_datastore.patch(updated_metadata)
    assert (
        patched_metadata.model_dump(by_alias=True, exclude_none=True)
        == PATCHED_METADATA
    )


def test_patch_change_name_desc_when_measure_has_a_unit_type():
    metadata_in_datastore = Metadata(**PATCH_UNIT_TYPE_METADATA_IN_DATASTORE)
    updated_metadata = Metadata(**PATCH_UNIT_TYPE_UPDATED_METADATA)
    patched_metadata = metadata_in_datastore.patch(updated_metadata)
    assert (
        patched_metadata.model_dump(by_alias=True, exclude_none=True)
        == PATCH_UNIT_TYPE_PATCHED_METADATA
    )
