from pathlib import Path

from job_executor.adapter import local_storage
from job_executor.exception import LocalStorageError
from job_executor.model import Job
from job_executor.model.datastore_versions import (
    underscored_to_dotted_version,
    bump_dotted_version_number,
    dotted_to_underscored_version
)


def rollback_bump(job: Job):
    try:
        # Restore files from /tmp backup
        restored_version_number = local_storage.restore_from_temporary_backup()
        bump_manifesto = job.parameters.bump_manifesto
        update_type = bump_manifesto.update_type
        bumped_version_number = (
            '1.0.0.0' if restored_version_number is None
            else bump_dotted_version_number(
                underscored_to_dotted_version(restored_version_number),
                update_type
            )
        )
        bumped_version_metadata = dotted_to_underscored_version(
            bumped_version_number
        )
        bumped_version_data = '_'.join(bumped_version_metadata.split('_')[:-1])
        pending_datasets = [
            dataset.name for dataset in bump_manifesto.data_structure_updates
        ]
        # TODO: remove generated datastore files
        #       ( data_versions, metadata_all__)
        # rename files back to __DRAFT
        for dataset in pending_datasets:
            datastore_dir = Path(local_storage.DATASTORE_DIR)
            dataset_data_dir = datastore_dir / 'data' / dataset
            dataset_metadata_dir = datastore_dir / 'metadata' / dataset
            partitioned_data_path = (
                dataset_data_dir / f'{dataset}__{bumped_version_data}'
            )
            data_path = (
                dataset_data_dir / f'{dataset}__{bumped_version_data}.parquet'
            )
            metadata_path = (
                dataset_metadata_dir /
                f'{dataset}__{bumped_version_metadata}.json'
            )
    except LocalStorageError as e:
        raise e
