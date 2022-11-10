import os
import shutil
from pathlib import Path

from job_executor.adapter.local_storage import DATASTORE_DIR
from job_executor.domain import rollback


JOB_ID = '123-123-123-123'
BUMP_MANIFESTO = {
    'version': '0.0.0.1635299291',
    'description': 'Draft',
    'releaseTime': 1635299291,
    'languageCode': 'no',
    'dataStructureUpdates': [
      {
        'name': 'UTDANNING',
        'description': 'Første publisering',
        'operation': 'ADD',
        'releaseStatus': 'PENDING_RELEASE'
      },
      {
        'name': 'BRUTTO_INNTEKT',
        'description': 'Første publisering',
        'operation': 'ADD',
        'releaseStatus': 'PENDING_RELEASE'
      },
      {
        'name': 'KJOENN',
        'description': 'Første publisering',
        'operation': 'ADD',
        'releaseStatus': 'PENDING_RELEASE'
      }
    ],
    'updateType': 'MINOR'
}
DATASTORE_INFO_DIR = Path(DATASTORE_DIR) / 'datastore'
DATASTORE_DATA_DIR = Path(DATASTORE_DIR) / 'data'
DATASTORE_METADATA_DIR = Path(DATASTORE_DIR) / 'metadata'


def setup_function():
    if os.path.isdir('tests/resources_backup'):
        shutil.rmtree('tests/resources_backup')
    shutil.copytree(
        'tests/resources',
        'tests/resources_backup'
    )
    shutil.rmtree('tests/resources/TEST_DATASTORE')
    shutil.move(
        'tests/resources/ROLLBACK_DATASTORE',
        'tests/resources/TEST_DATASTORE'
    )


def teardown_function():
    shutil.rmtree('tests/resources')
    shutil.move(
        'tests/resources_backup',
        'tests/resources'
    )


def test_rollback_interrupted_bump():
    backup_draft_version = ''
    backup_datastore_versions = ''
    backup_metadata_all_draft = ''

    rollback.rollback_bump(JOB_ID, BUMP_MANIFESTO)

    restored_draft_version = ''
    restored_datastore_versions = ''
    restored_metadata_all_draft = ''

    assert restored_draft_version == backup_draft_version
    assert restored_datastore_versions == backup_datastore_versions
    assert restored_metadata_all_draft == backup_metadata_all_draft

    assert (
        os.listdir(DATASTORE_DATA_DIR / 'KJOENN')
        == ['KJOENN__DRAFT.parquet']
    )
    assert (
        os.listdir(DATASTORE_DATA_DIR / 'FOEDSELSVEKT')
        == ['FOEDSELSVEKT__DRAFT.parquet']
    )
    assert (
        os.listdir(DATASTORE_DATA_DIR / 'BRUTTO_INNTEKT')
        == ['BRUTTO_INNTEKT__DRAFT']
    )
    assert (
        os.listdir(DATASTORE_METADATA_DIR / 'KJOENN')
        == ['KJOENN__DRAFT.json']
    )
    assert (
        os.listdir(DATASTORE_METADATA_DIR / 'FOEDSELSVEKT')
        == ['FOEDSELSVEKT__DRAFT.json']
    )
    assert (
        os.listdir(DATASTORE_METADATA_DIR / 'BRUTTO_INNTEKT')
        == ['BRUTTO_INNTEKT__DRAFT.json']
    )
