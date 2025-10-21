import logging
import os
import shutil
from pathlib import Path

from job_executor.adapter import datastore_api
from job_executor.adapter.datastore_api.models import Job, JobStatus
from job_executor.adapter.fs import LocalStorageAdapter
from job_executor.adapter.fs.models.datastore_versions import (
    bump_dotted_version_number,
    dotted_to_underscored_version,
    underscored_to_dotted_version,
)
from job_executor.common.exceptions import (
    LocalStorageError,
    RollbackException,
    StartupException,
)
from job_executor.config import environment

WORKING_DIR_PATH = Path(environment.datastore_dir + "_working")
logger = logging.getLogger()


def rollback_bump(job_id: str, bump_manifesto: dict) -> None:
    local_storage = LocalStorageAdapter(Path(environment.datastore_dir))
    try:
        logger.info(f"{job_id}: Restoring files from temporary backup")
        restored_version_number = (
            local_storage.datastore_dir.restore_from_temporary_backup()
        )
        update_type = bump_manifesto["updateType"]
        bumped_version_number = (
            "1.0.0.0"
            if restored_version_number is None
            else bump_dotted_version_number(
                underscored_to_dotted_version(restored_version_number),
                update_type,
            )
        )
        logger.warning(
            f"{job_id}: Rolling back to {restored_version_number} "
            f"from bump to {bumped_version_number}"
        )
        bumped_version_metadata = dotted_to_underscored_version(
            bumped_version_number
        )
        bumped_version_data = "_".join(bumped_version_metadata.split("_")[:-1])
        manifesto_datasets = [
            dataset["name"]
            for dataset in bump_manifesto["dataStructureUpdates"]
            if dataset["releaseStatus"] != "DRAFT"
        ]
        logger.info(
            f"{job_id}: Found {len(manifesto_datasets)} "
            " datasets in bump_manifesto"
        )

        logger.info(f"{job_id}: Removing generated datastore files")
        datastore_info_dir = local_storage.datastore_dir.metadata_dir

        # No new data version has been built if update type was PATCH
        if update_type in ["MAJOR", "MINOR"]:
            logger.info(
                f"{job_id}: Update type was {update_type}: "
                f"Deleting data_versions__{bumped_version_data}"
            )
            data_versions_path = (
                datastore_info_dir
                / f"data_versions__{bumped_version_data}.json"
            )
            if data_versions_path.exists():
                logger.info(f"{job_id}: Deleting {data_versions_path}")
                os.remove(data_versions_path)

        metadata_all_path = (
            datastore_info_dir / f"metadata_all__{bumped_version_metadata}.json"
        )
        if metadata_all_path.exists():
            logger.info(f"{job_id}: Deleting {metadata_all_path}")
            os.remove(metadata_all_path)

        logger.info(f"{job_id}: Reverting back to DRAFT for dataset files")
        for dataset in manifesto_datasets:
            if update_type in ["MAJOR", "MINOR"]:
                logger.info(
                    f"{job_id}: Update type is {update_type}. "
                    f"Reverting {dataset} data file to DRAFT"
                )
                dataset_data_dir: Path = (
                    local_storage.datastore_dir.data_dir / dataset
                )
                partitioned_data_path: Path = (
                    dataset_data_dir / f"{dataset}__{bumped_version_data}"
                )
                if partitioned_data_path.exists():
                    logger.info(
                        f"{job_id}: Renaming {partitioned_data_path} "
                        "back to draft"
                    )
                    shutil.move(
                        partitioned_data_path,
                        dataset_data_dir / f"{dataset}__DRAFT",
                    )
                else:
                    data_path = (
                        dataset_data_dir
                        / f"{dataset}__{bumped_version_data}.parquet"
                    )
                    if data_path.exists():
                        logger.info(
                            f"{job_id}: Renaming {data_path} back to draft"
                        )
                        shutil.move(
                            data_path,
                            dataset_data_dir / f"{dataset}__DRAFT.parquet",
                        )
        logger.info(f"{job_id}: Deleting temporary backup")
        local_storage.datastore_dir.archive_temporary_backup()
    except LocalStorageError as e:
        logger.error(f"{job_id}: LocalStorageError when rolling back job")
        logger.exception(e)
        raise e
    except Exception as e:
        logger.error(f"{job_id}: Unexpected error when rolling back job")
        logger.exception(e)


def rollback_worker_phase_import_job(
    job_id: str, operation: str, dataset_name: str
) -> None:
    local_storage = LocalStorageAdapter(Path(environment.datastore_dir))
    logger.warning(
        f"{job_id}: Rolling back worker job "
        f'with target: "{dataset_name}" and operation "{operation}"'
    )
    generated_metadata_files = [
        f"{dataset_name}.json",
        f"{dataset_name}__DRAFT.json",
    ]
    generated_data_files = [
        f"{dataset_name}.db",
        f"{dataset_name}.parquet",
        f"{dataset_name}_pseudonymized.parquet",
        f"{dataset_name}__DRAFT.parquet",
    ]
    generated_data_directory = f"{dataset_name}__DRAFT"

    for file in generated_metadata_files:
        filepath = local_storage.working_dir.path / file
        if filepath.exists():
            logger.info(f'{job_id}: Deleting metadata file "{filepath}"')
            os.remove(filepath)

    if operation in ["ADD", "CHANGE"]:
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
        dataset_directory = WORKING_DIR_PATH / dataset_name
        if dataset_directory.exists() and os.path.isdir(dataset_directory):
            logger.info(
                f'{job_id}: Deleting dataset directory "{dataset_directory}"'
            )
            shutil.rmtree(dataset_directory)


def rollback_manager_phase_import_job(
    job_id: str, operation: str, dataset_name: str
) -> None:
    """
    Rolls back manager phase import job.
    Exceptions are not handled here on purpose. It is a catastrophic thing
    if a rollback fails.
    """
    local_storage = LocalStorageAdapter(Path(environment.datastore_dir))
    logger.warning(
        f"{job_id}: Rolling back import job "
        f'with target: "{dataset_name}" and operation "{operation}"'
    )
    logger.info(f"{job_id}: Restoring files from temporary backup")
    local_storage.datastore_dir.restore_from_temporary_backup()

    if operation in ["ADD", "CHANGE"]:
        logger.info(f"{job_id}: Deleting data file/directory")
        local_storage.datastore_dir.delete_parquet_draft(dataset_name)
    logger.info(f"{job_id}: Deleting temporary backup")
    local_storage.datastore_dir.archive_temporary_backup()


def fix_interrupted_jobs() -> None:
    logger.info("Querying for interrupted jobs")
    in_progress_jobs = datastore_api.get_jobs(ignore_completed=True)
    queued_statuses = ["queued", "built"]
    interrupted_jobs = [
        job for job in in_progress_jobs if job.status not in queued_statuses
    ]
    logger.info(f"Found {len(interrupted_jobs)} interrupted jobs")
    try:
        for job in interrupted_jobs:
            fix_interrupted_job(job)
    except RollbackException as e:
        raise StartupException(e) from e


def fix_interrupted_job(job: Job) -> None:
    job_operation = job.parameters.operation
    logger.warning(
        f'{job.job_id}: Rolling back job with operation "{job_operation}"'
    )
    if job_operation in ["ADD", "CHANGE", "PATCH_METADATA"]:
        if job.status == "importing":
            rollback_manager_phase_import_job(
                job.job_id, job_operation, job.parameters.target
            )
            logger.info(
                f"{job.job_id}: Rolled back importing of job with "
                f'operation "{job_operation}". Retrying from status '
                '"built"'
            )
            datastore_api.update_job_status(
                job.job_id,
                JobStatus.BUILT,
                "Reset to built status will be due to unexpected interruption",
            )
        else:
            rollback_worker_phase_import_job(
                job.job_id, job_operation, job.parameters.target
            )
            logger.info(
                f'{job.job_id}: Setting status to "failed" for interrupted job'
            )
            datastore_api.update_job_status(
                job.job_id,
                JobStatus.FAILED,
                "Job was failed due to an unexpected interruption",
            )
    elif job_operation in [
        "SET_STATUS",
        "DELETE_DRAFT",
        "REMOVE",
        "ROLLBACK_REMOVE",
    ]:
        logger.info(
            'Setting status to "queued" for '
            f"interrupted job with id {job.job_id}"
        )
        datastore_api.update_job_status(
            job.job_id,
            JobStatus.QUEUED,
            "Retrying due to an unexpected interruption.",
        )
    elif job_operation == "BUMP":
        try:
            bump_manifesto = job.parameters.bump_manifesto
            if bump_manifesto is None:
                raise RollbackException("No bump manifesto available")
            rollback_bump(job.job_id, bump_manifesto.model_dump())
        except Exception as exc:
            error_message = f"Failed rollback for {job.job_id}"
            logger.exception(error_message, exc_info=exc)
            raise RollbackException(error_message) from exc
        logger.info(
            'Setting status to "failed" for '
            f"interrupted job with id {job.job_id}"
        )
        datastore_api.update_job_status(
            job.job_id,
            JobStatus.FAILED,
            "Bump operation was interrupted and rolled back.",
        )
    else:
        log_message = (
            f"Unrecognized job operation {job_operation} for job {job.job_id}"
        )
        logger.error(log_message)
        raise RollbackException(log_message)
