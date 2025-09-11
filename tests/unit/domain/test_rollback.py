import json
import os
import shutil
from pathlib import Path

from job_executor.adapter.local_storage import DATASTORE_DIR, WORKING_DIR
from job_executor.domain import rollback

JOB_ID = "123-123-123-123"
BUMP_MANIFESTO = {
    "version": "0.0.0.1635299291",
    "description": "Draft",
    "releaseTime": 1635299291,
    "languageCode": "no",
    "dataStructureUpdates": [
        {
            "name": "FOEDSELSVEKT",
            "description": "Første publisering",
            "operation": "ADD",
            "releaseStatus": "PENDING_RELEASE",
        },
        {
            "name": "BRUTTO_INNTEKT",
            "description": "Første publisering",
            "operation": "ADD",
            "releaseStatus": "PENDING_RELEASE",
        },
        {
            "name": "KJOENN",
            "description": "Første publisering",
            "operation": "ADD",
            "releaseStatus": "PENDING_RELEASE",
        },
    ],
    "updateType": "MINOR",
}

BUMP_MANIFESTO_PATCH = {
    "version": "0.0.0.1635299291",
    "description": "Draft",
    "releaseTime": 1635299291,
    "languageCode": "no",
    "dataStructureUpdates": [
        {
            "name": "FOEDSELSVEKT",
            "description": "Første publisering",
            "operation": "PATCH_METADATA",
            "releaseStatus": "PENDING_RELEASE",
        }
    ],
    "updateType": "PATCH",
}


def _read_json(file_path: Path) -> dict:
    with open(file_path, encoding="utf-8") as f:
        return json.load(f)


DATASTORE_INFO_DIR = Path(DATASTORE_DIR) / "datastore"
DATASTORE_DATA_DIR = Path(DATASTORE_DIR) / "data"
DATASTORE_METADATA_DIR = Path(DATASTORE_DIR) / "metadata"
DATASTORE_TEMP_DIR = Path(DATASTORE_DIR) / "tmp"
WORKING_DIR_PATH = Path(WORKING_DIR)


def setup_function():
    if os.path.isdir("tests/resources_backup"):
        shutil.rmtree("tests/resources_backup")
    shutil.copytree("tests/resources", "tests/resources_backup")
    shutil.rmtree("tests/resources/datastores/TEST_DATASTORE")
    shutil.move(
        "tests/resources/datastores/ROLLBACK_DATASTORE",
        "tests/resources/datastores/TEST_DATASTORE",
    )


def teardown_function():
    shutil.rmtree("tests/resources")
    shutil.move("tests/resources_backup", "tests/resources")


def test_rollback_interrupted_bump():
    draft_version_backup = _read_json(DATASTORE_TEMP_DIR / "draft_version.json")
    metadata_all_draft_backup = _read_json(
        DATASTORE_TEMP_DIR / "metadata_all__DRAFT.json"
    )
    datastore_versions_backup = _read_json(
        DATASTORE_TEMP_DIR / "datastore_versions.json"
    )
    rollback.rollback_bump(JOB_ID, BUMP_MANIFESTO)

    restored_draft_version = _read_json(
        DATASTORE_INFO_DIR / "draft_version.json"
    )
    restored_datastore_versions = _read_json(
        DATASTORE_INFO_DIR / "datastore_versions.json"
    )
    restored_metadata_all_draft = _read_json(
        DATASTORE_INFO_DIR / "metadata_all__DRAFT.json"
    )

    assert restored_draft_version == draft_version_backup
    assert restored_datastore_versions == datastore_versions_backup
    assert restored_metadata_all_draft == metadata_all_draft_backup
    assert not (DATASTORE_INFO_DIR / "metadata_all__1_0_0.json").exists()
    assert not (DATASTORE_INFO_DIR / "data_versions__1_0.json").exists()

    assert os.listdir(DATASTORE_DATA_DIR / "KJOENN") == [
        "KJOENN__DRAFT.parquet"
    ]
    assert os.listdir(DATASTORE_DATA_DIR / "FOEDSELSVEKT") == [
        "FOEDSELSVEKT__DRAFT.parquet"
    ]
    assert os.listdir(DATASTORE_DATA_DIR / "BRUTTO_INNTEKT") == [
        "BRUTTO_INNTEKT__DRAFT"
    ]


def test_rollback_interrupted_bump_patch():
    shutil.rmtree("tests/resources/datastores/TEST_DATASTORE")
    shutil.move(
        "tests/resources/datastores/ROLLBACK_DATASTORE_PATCH",
        "tests/resources/datastores/TEST_DATASTORE",
    )
    draft_version_backup = _read_json(DATASTORE_TEMP_DIR / "draft_version.json")
    rollback.rollback_bump(JOB_ID, BUMP_MANIFESTO_PATCH)

    restored_draft_version = _read_json(
        DATASTORE_INFO_DIR / "draft_version.json"
    )
    assert restored_draft_version == draft_version_backup

    restored_datastore_versions = _read_json(
        DATASTORE_INFO_DIR / "datastore_versions.json"
    )
    assert len(restored_datastore_versions["versions"]) == 1

    assert os.path.exists(
        f"{DATASTORE_DATA_DIR}/FOEDSELSVEKT/FOEDSELSVEKT__1_0.parquet"
    )
    assert not os.path.exists(
        f"{DATASTORE_DATA_DIR}/FOEDSELSVEKT/FOEDSELSVEKT__DRAFT.parquet"
    )
    assert os.path.exists(f"{DATASTORE_INFO_DIR}/data_versions__1_0.json")


def test_rollback_interrupted_worker():
    pre_rollback_working_dir = os.listdir(WORKING_DIR_PATH)
    rollback.rollback_worker_phase_import_job(
        JOB_ID, "PATCH_METADATA", "SIVSTAND"
    )
    post_rollback_working_dir = os.listdir(WORKING_DIR_PATH)
    assert len(pre_rollback_working_dir) - len(post_rollback_working_dir) == 2
    assert not os.path.isfile(WORKING_DIR_PATH / "SIVSTAND.JSON")
    assert not os.path.isfile(WORKING_DIR_PATH / "SIVSTAND__DRAFT.JSON")

    generated_files_foedested = [
        "FOEDESTED.json",
        "FOEDESTED__DRAFT.json",
        "FOEDESTED.parquet",
        "FOEDESTED_pseudonymized.parquet",
        "FOEDESTED__DRAFT.parquet",
    ]
    pre_rollback_working_dir = os.listdir(WORKING_DIR_PATH)
    rollback.rollback_worker_phase_import_job(JOB_ID, "ADD", "FOEDESTED")
    post_rollback_working_dir = os.listdir(WORKING_DIR_PATH)
    assert len(pre_rollback_working_dir) - len(
        generated_files_foedested
    ) == len(post_rollback_working_dir)
    for generated_file in generated_files_foedested:
        assert not os.path.isfile(WORKING_DIR_PATH / generated_file)


def test_rollback_interrupted_import():
    draft_version_backup = _read_json(DATASTORE_TEMP_DIR / "draft_version.json")
    metadata_all_draft_backup = _read_json(
        DATASTORE_TEMP_DIR / "metadata_all__DRAFT.json"
    )
    datastore_versions_backup = _read_json(
        DATASTORE_TEMP_DIR / "datastore_versions.json"
    )
    rollback.rollback_manager_phase_import_job(JOB_ID, "ADD", "SIVSTAND")

    restored_draft_version = _read_json(
        DATASTORE_INFO_DIR / "draft_version.json"
    )
    restored_datastore_versions = _read_json(
        DATASTORE_INFO_DIR / "datastore_versions.json"
    )
    restored_metadata_all_draft = _read_json(
        DATASTORE_INFO_DIR / "metadata_all__DRAFT.json"
    )

    assert restored_draft_version == draft_version_backup
    assert restored_datastore_versions == datastore_versions_backup
    assert restored_metadata_all_draft == metadata_all_draft_backup
    assert not os.path.isfile(
        DATASTORE_DATA_DIR / "SIVSTAND" / "SIVSTAND__DRAFT.parquet"
    )
