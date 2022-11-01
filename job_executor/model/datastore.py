from job_executor.adapter import local_storage

from job_executor.exception import VersioningException
from job_executor.model.metadata import Metadata
from job_executor.model.metadata_all import MetadataAll, MetadataAllDraft
from job_executor.model.datastore_versions import DatastoreVersions
from job_executor.model.datastore_version import DatastoreVersion, DraftVersion
from job_executor.model.data_structure_update import DataStructureUpdate


class Datastore():
    metadata_all_latest: MetadataAll
    metadata_all_draft: MetadataAllDraft
    datastore_versions: DatastoreVersions
    draft_version: DraftVersion
    latest_version_number: str

    def __init__(self):
        self.draft_version = DraftVersion()
        self.datastore_versions = DatastoreVersions()
        self.latest_version_number = (
            self.datastore_versions.get_latest_version_number()
        )
        self.metadata_all_draft = MetadataAllDraft()
        if self.latest_version_number is None:
            self.metadata_all_latest = None
        else:
            self.metadata_all_latest = MetadataAll(
                **local_storage.get_metadata_all(
                    self.latest_version_number
                )
            )

    def _get_release_status(self, dataset_name: str):
        release_status = self.draft_version.get_dataset_release_status(
            dataset_name
        )
        return (
            release_status if release_status is not None
            else self.datastore_versions.get_dataset_release_status(
                dataset_name
            )
        )

    def patch_metadata(self, dataset_name: str, description: str):
        """
        Patch metadata for a released dataset with updated metadata
        file.
        """
        dataset_release_status = self._get_release_status(dataset_name)
        if dataset_release_status != 'RELEASED':
            raise VersioningException(
                'Can\'t patch metadata of dataset with status '
                f'{dataset_release_status}'
            )
        draft_metadata = Metadata(
            **local_storage.get_working_dir_metadata(dataset_name)
        )
        released_metadata = self.metadata_all_latest.get(dataset_name)
        patched_metadata = released_metadata.patch(draft_metadata)
        self.metadata_all_draft.remove(dataset_name)
        self.metadata_all_draft.add(patched_metadata)
        self.draft_version.add(
            DataStructureUpdate(
                name=dataset_name,
                operation='PATCH_METADATA',
                description=description,
                releaseStatus='DRAFT'
            )
        )
        local_storage.write_metadata(
            patched_metadata.dict(by_alias=True), dataset_name, 'DRAFT'
        )

    def add(self, dataset_name: str, description: str):
        """
        Import metadata and data as draft for a new dataset that
        has not been released in a previous versions.
        """
        dataset_release_status = self._get_release_status(dataset_name)
        if dataset_release_status is not None:
            raise VersioningException(
                f'Can\'t add dataset with status {dataset_release_status}'
            )
        self.draft_version.add(
            DataStructureUpdate(
                name=dataset_name,
                operation='ADD',
                description=description,
                releaseStatus='DRAFT'
            )
        )
        draft_metadata = Metadata(
            **local_storage.get_working_dir_metadata(dataset_name)
        )
        local_storage.make_dataset_dir(dataset_name)
        local_storage.write_metadata(
            draft_metadata.dict(by_alias=True), dataset_name, 'DRAFT'
        )
        self.metadata_all_draft.add(draft_metadata)
        local_storage.move_working_dir_parquet_to_datastore(
            dataset_name
        )

    def change_data(self, dataset_name: str, description: str):
        """
        Import metadata and data as draft for as an update
        for a dataset that has already been released in a
        previous version.
        """
        dataset_release_status = self._get_release_status(dataset_name)
        if dataset_release_status != 'RELEASED':
            raise VersioningException(
                'Can\'t change data for dataset with release status'
                f'{dataset_release_status}'
            )
        draft_metadata = Metadata(
            **local_storage.get_working_dir_metadata(dataset_name)
        )
        self.metadata_all_draft.remove(dataset_name)
        self.metadata_all_draft.add(draft_metadata)
        self.draft_version.add(
            DataStructureUpdate(
                name=dataset_name,
                operation='CHANGE_DATA',
                description=description,
                releaseStatus='DRAFT'
            )
        )
        local_storage.write_metadata(
            draft_metadata.dict(by_alias=True), dataset_name, 'DRAFT'
        )
        local_storage.move_working_dir_parquet_to_datastore(dataset_name)

    def remove(self, dataset_name: str, description: str):
        """
        Remove a released dataset that has been released in
        a previous version from future versions of the datastore.
        """
        dataset_release_status = self._get_release_status(dataset_name)
        if dataset_release_status != 'RELEASED':
            raise VersioningException(
                'Can\'t remove dataset with release status '
                f'{dataset_release_status}'
            )
        self.draft_version.add(
            DataStructureUpdate(
                name=dataset_name,
                operation='REMOVE',
                description=description,
                releaseStatus='DRAFT'
            )
        )
        self.metadata_all_draft.remove(dataset_name)

    def delete_draft(self, dataset_name: str):
        """
        Delete a dataset from the draft version of the datastore.
        """
        deleted_draft = self.draft_version.delete_draft(dataset_name)
        if deleted_draft.operation == 'REMOVE':
            released_metadata = self.metadata_all_latest.get(dataset_name)
            self.metadata_all_draft.add(released_metadata)
        if deleted_draft.operation in ['ADD', 'CHANGE_DATA', 'PATCH_METADATA']:
            self.metadata_all_draft.remove(dataset_name)
            local_storage.delete_metadata_draft(dataset_name)
        if deleted_draft.operation in ['ADD', 'CHANGE_DATA']:
            local_storage.delete_parquet_draft(dataset_name)

    def set_draft_release_status(self, dataset_name: str, new_status: str):
        """
        Set a new release status for a dataset in the draft version.
        """
        self.draft_version.set_draft_release_status(
            dataset_name, new_status
        )

    def bump_version(self, bump_manifesto: DatastoreVersion, description: str):
        """
        Release a new version of the datastore with the pending
        operations in the draft version of the datastore.
        """
        latest_data_versions = local_storage.get_data_versions(
            self.latest_version_number
        )
        if not self.draft_version.validate_bump_manifesto(bump_manifesto):
            raise VersioningException(
                'Invalid Bump: Changes were made to the datastore '
                'after bump was requested'
            )
        release_updates, update_type = self.draft_version.release_pending()
        # If there are no released versions update type will always be MAJOR
        if self.metadata_all_latest is None:
            update_type = 'MAJOR'
        new_version = self.datastore_versions.add_new_release_version(
            release_updates, description, update_type
        )
        new_metadata_datasets = (
            [] if self.metadata_all_latest is None
            else [ds for ds in self.metadata_all_latest]
        )

        local_storage.archive_draft_version(new_version)

        new_data_versions = {
            dataset_name: path
            for dataset_name, path in latest_data_versions.items()
        }
        for release_update in release_updates:
            operation = release_update.operation
            dataset_name = release_update.name
            if operation == 'REMOVE':
                new_metadata_datasets = [
                    dataset for dataset in new_metadata_datasets
                    if dataset.name != dataset_name
                ]
                del new_data_versions[dataset_name]
            if operation in ['PATCH_METADATA', 'CHANGE_DATA', 'ADD']:
                released_metadata = (
                    local_storage.rename_metadata_draft_to_release(
                        dataset_name, new_version
                    )
                )
                new_metadata_datasets = [
                    dataset for dataset in new_metadata_datasets
                    if dataset.name != dataset_name
                ]
                new_metadata_datasets.append(Metadata(**released_metadata))
            if operation in ['ADD', 'CHANGE_DATA']:
                new_data_versions[dataset_name] = (
                    local_storage.rename_parquet_draft_to_release(
                        dataset_name, new_version
                    )
                )
        if update_type in ['MINOR', 'MAJOR']:
            local_storage.write_data_versions(new_data_versions, new_version)
        new_metadata_all_dict = self.metadata_all_draft.dict(by_alias=True)
        del new_metadata_all_dict['dataStructures']
        new_metadata_all_dict['dataStructures'] = [
            dataset.dict(by_alias=True) for dataset in new_metadata_datasets
        ]
        local_storage.write_metadata_all(
            new_metadata_all_dict, new_version
        )
        self.metadata_all_latest = MetadataAll(
            **local_storage.get_metadata_all(new_version)
        )
        self.latest_version_number = new_version
        self.metadata_all_draft.remove_all()
        for metadata in self.metadata_all_latest:
            self.metadata_all_draft.add(metadata)
        for draft in self.draft_version:
            self.metadata_all_draft.remove(draft.name)
            if draft.operation == 'REMOVE':
                continue
            else:
                self.metadata_all_draft.add(
                    Metadata(**local_storage.get_metadata(draft.name, 'DRAFT'))
                )
