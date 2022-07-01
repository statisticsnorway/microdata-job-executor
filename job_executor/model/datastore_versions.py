import json
from typing import List, Tuple, Union
from datetime import datetime

from pydantic import BaseModel, Extra, root_validator

from job_executor.adapter import local_storage_adapter
from job_executor.exception.exception import (
    NoSuchDraftException,
    ReleaseStatusException
)


class DataStructureUpdate(BaseModel, extra=Extra.forbid):
    name: str
    description: str
    operation: str
    releaseStatus: str

    def set_release_status(self, new_status: str):
        if new_status == 'PENDING_RELEASE':
            if self.operation not in ['ADD', 'CHANGE_DATA', 'PATCH_METADATA']:
                ReleaseStatusException(
                    f'Can\'t set release status: {new_status} '
                    f'for dataset with operation: {self.operation}'
                )
        elif new_status == 'PENDING_DELETE':
            if self.operation != 'REMOVE':
                ReleaseStatusException(
                    f'Can\'t set release status: {new_status} '
                    f'for dataset with operation: {self.operation}'
                )
        elif new_status != 'DRAFT':
            ReleaseStatusException(f'Invalid release status: {new_status}')
        self.releaseStatus = new_status


class DatastoreVersion(BaseModel):
    version: str
    description: str
    releaseTime: int
    languageCode: str
    updateType: str
    dataStructureUpdates: List[DataStructureUpdate]
    iter: int = 0

    def dict(self):
        return {
            "version": self.version,
            "description": self.description,
            "releaseTime": self.releaseTime,
            "languageCode": self.languageCode,
            "updateType": self.updateType,
            "dataStructureUpdates": [
                update.dict() for update in self.dataStructureUpdates
            ]
        }

    def validate_bump_manifesto(
        self, bump_manifesto: 'DatastoreVersion'
    ) -> bool:
        pending_operations = [
            update.dict() for update in self.dataStructureUpdates
            if update.releaseStatus != 'DRAFT'
        ]
        other_pending_operations = [
            update.dict() for update in bump_manifesto.dataStructureUpdates
            if update.releaseStatus != 'DRAFT'
        ]
        if len(pending_operations) != len(other_pending_operations):
            return False
        for pending_operation in other_pending_operations:
            if pending_operation not in pending_operations:
                return False
        return True

    def __iter__(self):
        self.iter = 0
        return self

    def __next__(self):
        if self.iter < len(self.dataStructureUpdates):
            index = self.iter
            self.iter = self.iter + 1
            return DataStructureUpdate(
                **self.dataStructureUpdates[index].dict()
            )
        else:
            raise StopIteration

    def _calculate_update_type(self):
        pending_operations = [
            update.operation for update in self.dataStructureUpdates
            if update.releaseStatus != 'DRAFT'
        ]
        if (
            'CHANGE_DATA' in pending_operations or
            'REMOVE' in pending_operations
        ):
            self.updateType = 'MAJOR'
        elif 'ADD' in pending_operations:
            self.updateType = 'MINOR'
        elif 'PATCH_METADATA' in pending_operations:
            self.updateType = 'PATCH'
        else:
            self.updateType = None

    def get_dataset_release_status(
        self, dataset_name: str
    ) -> Union[str, None]:
        for update in self.dataStructureUpdates:
            if update.name == dataset_name:
                return update.releaseStatus
        return None


class DraftVersion(DatastoreVersion):

    @root_validator(skip_on_failure=True, pre=True)
    @classmethod
    def read_file(cls, _):
        return local_storage_adapter.get_draft_version()

    def add(self, data_structure_update: DataStructureUpdate):
        self.dataStructureUpdates.append(data_structure_update)
        self._write_to_file()

    def delete_draft(self, dataset_name: str) -> DataStructureUpdate:
        deleted_draft = next(
            update for update in self.dataStructureUpdates
            if update.name == dataset_name
        )
        self.dataStructureUpdates = [
            update for update in self.dataStructureUpdates
            if update.name != dataset_name
        ]
        self._write_to_file()
        return deleted_draft

    def release_pending(self) -> Tuple[List[DataStructureUpdate], str]:
        draft_updates = []
        pending_updates = []
        for update in self.dataStructureUpdates:
            if update.releaseStatus == 'DRAFT':
                draft_updates.append(update)
            else:
                pending_updates.append(update)
        update_type = self.updateType
        self.dataStructureUpdates = draft_updates
        self._calculate_update_type()
        self._write_to_file()
        return pending_updates, update_type

    def _write_to_file(self):
        self.releaseTime = (datetime.now() - datetime.utcfromtimestamp(0)).days
        self._calculate_update_type()
        local_storage_adapter.write_draft_version(self.dict())

    def set_draft_release_status(self, dataset_name: str, new_status: str):
        dataset_update = next(
            update for update in self.dataStructureUpdates
            if update.name == dataset_name
        )
        if dataset_update is None:
            raise NoSuchDraftException(f'No draft for dataset {dataset_name}')
        dataset_update.set_release_status(new_status)
        self._calculate_update_type()
        self._write_to_file()


class DatastoreVersions():
    name: str
    label: str
    description: str
    versions = List[DatastoreVersion]

    def __init__(self):
        datastore_versions_dict = (
            local_storage_adapter.get_datastore_versions()
        )
        self.name = datastore_versions_dict['name']
        self.label = datastore_versions_dict['label']
        self.description = datastore_versions_dict['description']
        self.versions = [
            DatastoreVersion(**version)
            for version in datastore_versions_dict['versions']
        ]

    def dict(self):
        return {
            'name': self.name,
            'label': self.label,
            'description': self.description,
            'versions': [
                version.dict()
                for version in self.versions
            ]
        }

    def _write_to_file(self):
        local_storage_adapter.write_datastore_versions(
            self.dict()
        )

    def add_new_release_version(
        self,
        data_structure_updates: List[DataStructureUpdate],
        description: str,
        update_type: str
    ) -> str:
        released_data_structure_updates = [
            DataStructureUpdate(
                name=data_structure.name,
                description=data_structure.description,
                operation=data_structure.operation,
                releaseStatus=(
                    "DELETED" if data_structure.operation == "REMOVE"
                    else "RELEASED"
                )
            ) for data_structure in data_structure_updates
        ]
        latest_version_number = self.versions[0].version
        new_version_number = bump_dotted_version_number(
                latest_version_number, update_type
        )
        new_release_version = DatastoreVersion(
            version=new_version_number,
            description=description,
            releaseTime=(datetime.now() - datetime.utcfromtimestamp(0)).days,
            languageCode='no',
            updateType=update_type,
            dataStructureUpdates=released_data_structure_updates
        )
        self.versions = [new_release_version] + self.versions
        self._write_to_file()
        return dotted_to_underscored_version(new_version_number)

    def get_dataset_release_status(
        self, dataset_name: str
    ) -> Union[str, None]:
        for version in self.versions:
            release_status = version.get_dataset_release_status(
                dataset_name
            )
            if release_status is not None:
                return release_status
        return None

    def get_latest_version_number(self) -> str:
        return dotted_to_underscored_version(
            self.versions[0].version
        )


def dotted_to_underscored_version(version: str) -> str:
    return version.replace('.', '_')[:5]


def underscored_to_dotted_version(version: str) -> str:
    return f'{version.replace("_", ".")}.0'


def bump_dotted_version_number(version: str, update_type: str) -> str:
    version_list = [int(number) for number in version.split('.')]
    if update_type == 'MAJOR':
        version_list[0] += 1
    if update_type == 'MINOR':
        version_list[1] += 1
    if update_type == 'PATCH':
        version_list[2] += 1
    return '.'.join([str(number) for number in version_list])
