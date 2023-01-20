import os
import logging
import shutil
from pathlib import Path

from job_executor.adapter import local_storage
from job_executor.adapter.local_storage import WORKING_DIR
from job_executor.exception import LocalStorageError
from job_executor.model.datastore_versions import (
    underscored_to_dotted_version,
    bump_dotted_version_number,
    dotted_to_underscored_version
)

WORKING_DIR_PATH = Path(WORKING_DIR)
logger = logging.getLogger()


def rollback_bump(job_id: str, bump_manifesto: dict):
    try:
        logger.info(f'{job_id}: Restoring files from temporary backup')
        restored_version_number = (
            local_storage.restore_from_temporary_backup()
        )
        update_type = bump_manifesto['updateType']
        bumped_version_number = (
            '1.0.0.0' if restored_version_number is None
            else bump_dotted_version_number(
                underscored_to_dotted_version(restored_version_number),
                update_type
            )
        )
        logger.info(
            f'{job_id}: Rolling back to {restored_version_number} '
            f'from bump to {bumped_version_number}'
        )
        bumped_version_metadata = dotted_to_underscored_version(
            bumped_version_number
        )
        bumped_version_data = '_'.join(bumped_version_metadata.split('_')[:-1])
        manifesto_datasets = [
            dataset['name']
            for dataset in bump_manifesto['dataStructureUpdates']
        ]
        logger.info(
            f'{job_id}: Found {len(manifesto_datasets)} '
            ' datasets in bump_manifesto'
        )

        logger.info(f'{job_id}: Removing generated datastore files')
        datastore_dir = Path(local_storage.DATASTORE_DIR)
        datastore_info_dir = datastore_dir / 'datastore'
        data_versions_path = (
            datastore_info_dir /
            f'data_versions__{bumped_version_data}.json'
        )
        if data_versions_path.exists():
            logger.info(f'{job_id}: Deleting {data_versions_path}')
            os.remove(data_versions_path)
        metadata_all_path = (
            datastore_info_dir /
            f'metadata_all__{bumped_version_metadata}.json'
        )
        if metadata_all_path.exists():
            logger.info(f'{job_id}: Deleting {metadata_all_path}')
            os.remove(metadata_all_path)

        logger.info(f'{job_id}: Reverting name change of DRAFT files')
        for dataset in manifesto_datasets:
            logger.info(f'{job_id}: Reverting files to draft for {dataset}')
            dataset_data_dir = datastore_dir / 'data' / dataset
            dataset_metadata_dir = datastore_dir / 'metadata' / dataset
            partitioned_data_path = (
                dataset_data_dir / f'{dataset}__{bumped_version_data}'
            )
            if partitioned_data_path.exists():
                logger.info(
                    f'{job_id}: Renaming {partitioned_data_path} '
                    'back to draft'
                )
                shutil.move(
                    partitioned_data_path,
                    dataset_data_dir / f'{dataset}__DRAFT'
                )

            data_path = (
                dataset_data_dir / f'{dataset}__{bumped_version_data}.parquet'
            )
            if data_path.exists():
                logger.info(f'{job_id}: Renaming {data_path} back to draft')
                shutil.move(
                    data_path,
                    dataset_data_dir / f'{dataset}__DRAFT.parquet'
                )
            metadata_path = (
                dataset_metadata_dir /
                f'{dataset}__{bumped_version_metadata}.json'
            )
            logger.info(metadata_path)
            if metadata_path.exists():
                logger.info(
                    f'{job_id}: Renaming {metadata_path} back to draft'
                )
                shutil.move(
                    metadata_path,
                    dataset_metadata_dir / f'{dataset}__DRAFT.json'
                )
        logger.info(f'{job_id}: Deleting temporary backup')
        local_storage.delete_temporary_backup()
    except LocalStorageError as e:
        logger.error(f'{job_id}: LocalStorageError when rolling back job')
        logger.exception(e)
        raise e
    except Exception as e:
        logger.error(f'{job_id}: Unexpected error when rolling back job')
        logger.exception(e)


def rollback_worker_phase_import_job(
    job_id: str, operation: str, dataset_name: str
):
    logger.info(
        f'{job_id}: Rolling back worker job '
        f'with target: "{dataset_name}" and operation "{operation}"'
    )
    generated_metadata_files = [
        f'{dataset_name}.json',
        f'{dataset_name}__DRAFT.json'
    ]
    generated_data_files = [
        f'{dataset_name}.db',
        f'{dataset_name}.csv',
        f'{dataset_name}_pseudonymized.csv',
        f'{dataset_name}_pseudonymized_enriched.csv',
        f'{dataset_name}__DRAFT.parquet'
    ]
    generated_data_directory = f'{dataset_name}__DRAFT'

    for file in generated_metadata_files:
        filepath = WORKING_DIR_PATH / file
        if filepath.exists():
            logger.info(f'{job_id}: Deleting metadata file "{filepath}"')
            os.remove(filepath)

    if operation in ['ADD', 'CHANGE']:
        for file in generated_data_files:
            filepath = WORKING_DIR_PATH / file
            if filepath.exists():
                logger.info(f'{job_id}: Deleting data file "{filepath}"')
                os.remove(filepath)
        parquet_directory = WORKING_DIR_PATH / generated_data_directory
        if parquet_directory.exists() and os.path.isdir(parquet_directory):
            logger.info(
                f'{job_id}: Deleting data directory "{parquet_directory}"'
            )
            shutil.rmtree(parquet_directory)


def rollback_manager_phase_import_job(
    job_id: str, operation: str, dataset_name: str
):
    logger.info(
        f'{job_id}: Rolling back import job '
        f'with target: "{dataset_name}" and operation "{operation}"'
    )
    logger.info(f'{job_id}: Restoring files from temporary backup')
    local_storage.restore_from_temporary_backup()

    logger.info(f'{job_id}: Deleting metadata draft file')
    local_storage.delete_metadata_draft(dataset_name)
    if operation in ['ADD', 'CHANGE']:
        logger.info(f'{job_id}: Deleting data file/directory')
        local_storage.delete_parquet_draft(dataset_name)
    logger.info(f'{job_id}: Deleting temporary backup')
    local_storage.delete_temporary_backup()
