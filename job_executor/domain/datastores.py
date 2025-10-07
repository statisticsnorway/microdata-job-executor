import logging

from job_executor.adapter import job_service, local_storage
from job_executor.domain.rollback import (
    rollback_bump,
    rollback_manager_phase_import_job,
)
from job_executor.exception import (
    NoSuchDraftException,
    PatchingError,
    UnnecessaryUpdateException,
    VersioningException,
)
from job_executor.model.data_structure_update import DataStructureUpdate
from job_executor.model.datastore_version import DatastoreVersion, DraftVersion
from job_executor.model.datastore_versions import DatastoreVersions
from job_executor.model.job import JobStatus
from job_executor.model.metadata import Metadata
from job_executor.model.metadata_all import MetadataAll, MetadataAllDraft

logger = logging.getLogger()


class Datastore:
    metadata_all_latest: MetadataAll | None
    metadata_all_draft: MetadataAllDraft
    datastore_versions: DatastoreVersions
    draft_version: DraftVersion
    latest_version_number: str | None

    def __init__(self) -> None:
        self.draft_version = DraftVersion()  # type: ignore
        self.datastore_versions = DatastoreVersions()  # type: ignore
        self.latest_version_number = (
            self.datastore_versions.get_latest_version_number()
        )
        self.metadata_all_draft = MetadataAllDraft()  # type: ignore
        if self.latest_version_number is None:
            self.metadata_all_latest = None
        else:
            self.metadata_all_latest = MetadataAll(
                **local_storage.get_metadata_all(self.latest_version_number)
            )


def _get_release_status(datastore: Datastore, dataset_name: str) -> str | None:
    release_status = datastore.draft_version.get_dataset_release_status(
        dataset_name
    )
    return (
        release_status
        if release_status is not None
        else datastore.datastore_versions.get_dataset_release_status(
            dataset_name
        )
    )


def _generate_new_metadata_all(
    datastore: Datastore, new_version: str, new_version_metadata: list[Metadata]
) -> None:
    new_metadata_all_dict = datastore.metadata_all_draft.model_dump(
        by_alias=True, exclude_none=True
    )
    del new_metadata_all_dict["dataStructures"]
    new_metadata_all_dict["dataStructures"] = [
        dataset.model_dump(by_alias=True, exclude_none=True)
        for dataset in new_version_metadata
    ]
    new_metadata_all = MetadataAll(**new_metadata_all_dict)
    local_storage.write_metadata_all(
        new_metadata_all.model_dump(by_alias=True, exclude_none=True),
        new_version,
    )
    datastore.metadata_all_latest = MetadataAll(
        **local_storage.get_metadata_all(new_version)
    )


def _version_pending_operations(
    datastore: Datastore,
    job_id: str,
    release_updates: list[DataStructureUpdate],
    new_version: str,
) -> tuple[list[Metadata], dict]:
    logger.info(f"{job_id}: Generating new metadata_all")
    new_metadata_datasets = (
        []
        if datastore.metadata_all_latest is None
        else [ds for ds in datastore.metadata_all_latest]
    )

    logger.info(f"{job_id}: Generating new data_versions")
    latest_data_versions = local_storage.get_data_versions(
        datastore.latest_version_number
    )
    new_data_versions = {
        dataset_name: path
        for dataset_name, path in latest_data_versions.items()
    }
    logger.info(f"{job_id}: Versioning each pending operation in BUMP")
    for release_update in release_updates:
        operation = release_update.operation
        dataset_name = release_update.name
        logger.info(
            f"{job_id}: Versioning {dataset_name} with operation {operation}"
        )

        if operation == "REMOVE":
            logger.info(f"{job_id}: Removing from metadata_all")
            new_metadata_datasets = [
                dataset
                for dataset in new_metadata_datasets
                if dataset.name != dataset_name
            ]
            logger.info(f"{job_id}: Removing from data_versions")
            del new_data_versions[dataset_name]

        if operation in ["PATCH_METADATA", "CHANGE", "ADD"]:
            logger.info(f"{job_id}: Renaming metadata file")
            logger.info(f"{job_id}: Updating metadata into metadata_all")
            new_metadata_datasets = [
                dataset
                for dataset in new_metadata_datasets
                if dataset.name != dataset_name
            ]
            updated_dataset = datastore.metadata_all_draft.get(dataset_name)
            if updated_dataset is None:
                raise NoSuchDraftException(
                    f"Could not find draft metadata for {dataset_name}"
                    " when up versioning the pending operations"
                )
            new_metadata_datasets.append(updated_dataset)
        if operation in ["ADD", "CHANGE"]:
            logger.info(
                f"{job_id}: Renaming data file and updating data_versions"
            )
            new_data_versions[dataset_name] = (
                local_storage.rename_parquet_draft_to_release(
                    dataset_name, new_version
                )
            )
    return new_metadata_datasets, new_data_versions


def patch_metadata(
    datastore: Datastore, job_id: str, dataset_name: str, description: str
) -> None:
    """
    Patch metadata for a released dataset with updated metadata
    file.
    """
    if datastore.metadata_all_latest is None:
        raise NoSuchDraftException("There are no released versions to patch")
    logger.info(f"{job_id}: Saving temporary backup")
    local_storage.save_temporary_backup()
    try:
        logger.info(f"{job_id}: importing")
        job_service.update_job_status(job_id, JobStatus.IMPORTING)
        dataset_release_status = _get_release_status(datastore, dataset_name)
        if dataset_release_status != "RELEASED":
            raise VersioningException(
                "Can't patch metadata of dataset with status "
                f"{dataset_release_status}"
            )
        draft_metadata = Metadata(
            **local_storage.get_working_dir_metadata(dataset_name)
        )
        released_metadata = datastore.metadata_all_latest.get(dataset_name)
        if released_metadata is None:
            raise NoSuchDraftException(
                f"Dataset {dataset_name} has no released version"
            )
        patched_metadata = released_metadata.patch(draft_metadata)
        datastore.metadata_all_draft.update_one(dataset_name, patched_metadata)
        datastore.draft_version.add(
            DataStructureUpdate(
                name=dataset_name,
                operation="PATCH_METADATA",
                description=description,
                release_status="DRAFT",
            )
        )
        logger.info(f"{job_id}: completed")
        job_service.update_job_status(job_id, JobStatus.COMPLETED)
        logger.info(f"{job_id}: Deleting temporary backup")
        local_storage.delete_temporary_backup()
        local_storage.delete_working_dir_metadata(dataset_name)
        local_storage.delete_archived_input(dataset_name)
    except PatchingError as e:
        logger.error(f"{job_id}: Patching error occured")
        logger.exception(f"{job_id}: {str(e)}", exc_info=e)
        rollback_manager_phase_import_job(
            job_id, "PATCH_METADATA", dataset_name
        )
        job_service.update_job_status(job_id, JobStatus.FAILED, str(e))
    except Exception as e:
        logger.error(f"{job_id}: An unexpected error occured")
        logger.exception(f"{job_id}: {str(e)}", exc_info=e)
        rollback_manager_phase_import_job(
            job_id, "PATCH_METADATA", dataset_name
        )
        job_service.update_job_status(job_id, JobStatus.FAILED)


def add(
    datastore: Datastore, job_id: str, dataset_name: str, description: str
) -> None:
    """
    Import metadata and data as draft for a new dataset that
    has not been released in a previous versions.
    """
    logger.info(f"{job_id}: Saving temporary backup")
    local_storage.save_temporary_backup()
    try:
        logger.info(f"{job_id}: importing")
        job_service.update_job_status(job_id, JobStatus.IMPORTING)
        dataset_release_status = _get_release_status(datastore, dataset_name)
        if dataset_release_status not in [None, "DELETED"]:
            raise VersioningException(
                f"Can't add dataset with status {dataset_release_status}"
            )
        datastore.draft_version.add(
            DataStructureUpdate(
                name=dataset_name,
                operation="ADD",
                description=description,
                release_status="DRAFT",
            )
        )
        draft_metadata = Metadata(
            **local_storage.get_working_dir_metadata(dataset_name)
        )
        local_storage.make_dataset_dir(dataset_name)
        datastore.metadata_all_draft.add(draft_metadata)
        local_storage.move_working_dir_parquet_to_datastore(dataset_name)
        logger.info(f"{job_id}: completed")
        job_service.update_job_status(job_id, JobStatus.COMPLETED)
        logger.info(f"{job_id}: Deleting temporary backup")
        local_storage.delete_temporary_backup()
        local_storage.delete_working_dir_metadata(dataset_name)
        local_storage.delete_archived_input(dataset_name)
    except Exception as e:
        logger.error(f"{job_id}: An unexpected error occured")
        logger.exception(f"{job_id}: {str(e)}", exc_info=e)
        rollback_manager_phase_import_job(job_id, "ADD", dataset_name)
        job_service.update_job_status(job_id, JobStatus.FAILED)


def change(
    datastore: Datastore, job_id: str, dataset_name: str, description: str
) -> None:
    """
    Import metadata and data as draft for as an update
    for a dataset that has already been released in a
    previous version.
    """
    try:
        logger.info(f"{job_id}: Saving temporary backup")
        local_storage.save_temporary_backup()

        logger.info(f"{job_id}: importing")
        job_service.update_job_status(job_id, JobStatus.IMPORTING)
        dataset_release_status = _get_release_status(datastore, dataset_name)
        if dataset_release_status != "RELEASED":
            raise VersioningException(
                "Can't change data for dataset with release status"
                f"{dataset_release_status}"
            )
        draft_metadata = Metadata(
            **local_storage.get_working_dir_metadata(dataset_name)
        )
        datastore.metadata_all_draft.update_one(dataset_name, draft_metadata)
        datastore.draft_version.add(
            DataStructureUpdate(
                name=dataset_name,
                operation="CHANGE",
                description=description,
                release_status="DRAFT",
            )
        )
        local_storage.move_working_dir_parquet_to_datastore(dataset_name)
        logger.info(f"{job_id}: completed")
        job_service.update_job_status(job_id, JobStatus.COMPLETED)
        logger.info(f"{job_id}: Deleting temporary backup")
        local_storage.delete_temporary_backup()
        local_storage.delete_working_dir_metadata(dataset_name)
        local_storage.delete_archived_input(dataset_name)
    except Exception as e:
        logger.error(f"{job_id}: An unexpected error occured")
        logger.exception(f"{job_id}: {str(e)}", exc_info=e)
        rollback_manager_phase_import_job(job_id, "CHANGE", dataset_name)
        job_service.update_job_status(job_id, JobStatus.FAILED)


def remove(
    datastore: Datastore, job_id: str, dataset_name: str, description: str
) -> None:
    """
    Remove a released dataset that has been released in
    a previous version from future versions of the datastore.
    """
    logger.info(f"{job_id}: initiated")
    job_service.update_job_status(job_id, JobStatus.INITIATED)
    dataset_release_status = _get_release_status(datastore, dataset_name)
    dataset_is_draft = datastore.draft_version.contains(dataset_name)
    dataset_operation = datastore.draft_version.get_dataset_operation(
        dataset_name
    )
    if dataset_is_draft and dataset_operation == "REMOVE":
        datastore.metadata_all_draft.remove(dataset_name)
        log_message = "Dataset already in draft with operation REMOVE."
        logger.info(f"{job_id}: {log_message}")
        job_service.update_job_status(job_id, JobStatus.COMPLETED, log_message)
        logger.info(f"{job_id}: completed")
    elif dataset_release_status != "RELEASED":
        log_message = (
            f"Can't remove dataset with release status {dataset_release_status}"
        )
        logger.error(f"{job_id}: {log_message}")
        job_service.update_job_status(job_id, JobStatus.FAILED, log_message)
    else:
        datastore.metadata_all_draft.remove(dataset_name)
        datastore.draft_version.add(
            DataStructureUpdate(
                name=dataset_name,
                operation="REMOVE",
                description=description,
                release_status="PENDING_DELETE",
            )
        )
        job_service.update_job_status(job_id, JobStatus.COMPLETED)
        logger.info(f"{job_id}: completed")


def delete_draft(
    datastore: Datastore, job_id: str, dataset_name: str, rollback_remove: bool
) -> None:
    """
    Delete a dataset from the draft version of the datastore.
    """
    logger.info(f"{job_id}: initiated")
    job_service.update_job_status(job_id, JobStatus.INITIATED)
    dataset_is_draft = datastore.draft_version.contains(dataset_name)
    dataset_operation = datastore.draft_version.get_dataset_operation(
        dataset_name
    )
    if dataset_operation != "REMOVE" and rollback_remove:
        log_message = f"{dataset_name} is not scheduled for removal"
        logger.error(f"{job_id}: {log_message}")
        job_service.update_job_status(job_id, JobStatus.FAILED, log_message)
        return
    if (not dataset_is_draft) or (
        dataset_operation == "REMOVE" and not rollback_remove
    ):
        log_message = f'Draft not found for dataset name: "{dataset_name}"'
        logger.error(f"{job_id}: {log_message}")
        job_service.update_job_status(job_id, JobStatus.FAILED, log_message)
        return
    # If dataset has previously released data/metadata that needs to
    # be restored
    if dataset_operation in ["CHANGE", "PATCH_METADATA", "REMOVE"]:
        released_metadata = None
        if datastore.metadata_all_latest is not None:
            released_metadata = datastore.metadata_all_latest.get(dataset_name)
        if released_metadata is None:
            log_message = (
                f"Can't find released metadata for {dataset_name} "
                "when attempting to delete draft."
            )
            logger.error(f"{job_id}: {log_message}")
            raise VersioningException(log_message)
        datastore.metadata_all_draft.remove(dataset_name)
        datastore.metadata_all_draft.add(released_metadata)
    if dataset_operation == "ADD":
        datastore.metadata_all_draft.remove(dataset_name)
    if dataset_operation in ["ADD", "CHANGE"]:
        local_storage.delete_parquet_draft(dataset_name)
    datastore.draft_version.delete_draft(dataset_name)
    job_service.update_job_status(job_id, JobStatus.COMPLETED)


def set_draft_release_status(
    datastore: Datastore, job_id: str, dataset_name: str, new_status: str
) -> None:
    """
    Set a new release status for a dataset in the draft version.
    """
    try:
        logger.info(f"{job_id}: initiated")
        job_service.update_job_status(job_id, JobStatus.INITIATED)
        datastore.draft_version.set_draft_release_status(
            dataset_name, new_status
        )
        job_service.update_job_status(job_id, JobStatus.COMPLETED)
        logger.info(f"{job_id}: completed")
    except UnnecessaryUpdateException as e:
        job_service.update_job_status(job_id, JobStatus.COMPLETED, f"{e}")
        logger.exception(f"{job_id}: {str(e)}", exc_info=e)
        logger.info(f"{job_id}: completed")
    except NoSuchDraftException as e:
        logger.exception(f"{job_id}: {str(e)}", exc_info=e)
        job_service.update_job_status(job_id, JobStatus.FAILED, f"{e}")


def bump_version(
    datastore: Datastore,
    job_id: str,
    bump_manifesto: DatastoreVersion,
    description: str,
) -> None:
    """
    Release a new version of the datastore with the pending
    operations in the draft version of the datastore.
    """
    logger.info(f"{job_id}: Saving temporary backup")
    local_storage.save_temporary_backup()

    try:
        logger.info(f"{job_id}: initiated")
        job_service.update_job_status(job_id, JobStatus.INITIATED)

        logger.info(f"{job_id}: Validating bump manifesto")
        if not datastore.draft_version.validate_bump_manifesto(bump_manifesto):
            log_message = (
                "Changes were made to the datastore after bump was requested"
            )
            logger.error(f"{job_id}: {log_message}")
            job_service.update_job_status(job_id, JobStatus.FAILED, log_message)
            logger.info(f"{job_id}: Archiving temporary backup")
            local_storage.archive_temporary_backup()
            return

        logger.info(f"{job_id}: Archiving draft version")
        local_storage.archive_draft_version(
            datastore.latest_version_number or "0.0.0.0"
        )

        logger.info(f"{job_id}: Release pending operations from draft_version")
        release_updates, update_type = datastore.draft_version.release_pending()
        # If there are no released versions update type is MAJOR
        if datastore.metadata_all_latest is None:
            update_type = "MAJOR"
        new_version = datastore.datastore_versions.add_new_release_version(
            release_updates, description, update_type
        )
        logger.info(
            f"{job_id}: "
            f"Bumping from {datastore.latest_version_number} => {new_version}"
            f"({update_type})",
        )
        (
            new_metadata_datasets,
            new_data_versions,
        ) = _version_pending_operations(
            datastore, job_id, release_updates, new_version
        )
        if update_type in ["MINOR", "MAJOR"]:
            logger.info(f"{job_id}: Writing new data_versions to file")
            local_storage.write_data_versions(new_data_versions, new_version)

        logger.info(f"{job_id}: Writing new metadata_all to file")
        _generate_new_metadata_all(
            datastore, new_version, new_metadata_datasets
        )
        datastore.latest_version_number = new_version
        assert datastore.metadata_all_latest is not None

        logger.info(f"{job_id}: Rebuilding metadata_all_DRAFT")
        datastore.metadata_all_draft.rebuild(
            datastore.metadata_all_latest.data_structures,
            datastore.draft_version,
        )

        logger.info(f"{job_id}: completed BUMP")
        job_service.update_job_status(job_id, JobStatus.COMPLETED)
        logger.info(f"{job_id}: Archiving temporary backup")
        local_storage.archive_temporary_backup()
    except Exception as e:
        logger.error(f"{job_id}: An unexpected error occured")
        logger.exception(f"{job_id}: {str(e)}", exc_info=e)
        rollback_bump(
            job_id,
            bump_manifesto.model_dump(by_alias=True, exclude_none=True),
        )
        job_service.update_job_status(job_id, JobStatus.FAILED)


def delete_archived_input(job_id: str, dataset_name: str) -> None:
    """
    Delete the archived dataset from archive directory.
    """
    try:
        logger.info(f"{job_id}: initiated")
        job_service.update_job_status(job_id, JobStatus.INITIATED)
        local_storage.delete_archived_input(dataset_name)
        job_service.update_job_status(job_id, JobStatus.COMPLETED)
    except Exception as e:
        logger.error(f"{job_id}: An unexpected error occured")
        logger.exception(f"{job_id}: {str(e)}", exc_info=e)
        job_service.update_job_status(job_id, JobStatus.FAILED)
