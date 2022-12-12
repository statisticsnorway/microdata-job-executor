from typing import List, Union
from datetime import datetime

from pydantic import Extra, root_validator
from job_executor.exception import VersioningException

from job_executor.model.camelcase_model import CamelModel
from job_executor.model.data_structure_update import DataStructureUpdate
from job_executor.model.datastore_version import DatastoreVersion
from job_executor.adapter import local_storage


class DatastoreVersions(CamelModel, extra=Extra.forbid):
    name: str
    label: str
    description: str
    versions: List[DatastoreVersion]

    @root_validator(skip_on_failure=True, pre=True)
    @classmethod
    def read_file(cls, _):
        return local_storage.get_datastore_versions()

    def _write_to_file(self):
        local_storage.write_datastore_versions(self.dict(by_alias=True))

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
                release_status=(
                    "DELETED" if data_structure.operation == "REMOVE"
                    else "RELEASED"
                )
            ) for data_structure in data_structure_updates
        ]
        new_version_number = (
            '1.0.0.0' if self.versions == []
            else bump_dotted_version_number(
                self.versions[0].version, update_type
            )
        )
        new_release_version = DatastoreVersion(
            version=new_version_number,
            description=description,
            release_time=(
                (datetime.now() - datetime.utcfromtimestamp(0)).seconds
            ),
            language_code='no',
            updateType=update_type,
            data_structure_updates=released_data_structure_updates
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
        if len(self.versions):
            return dotted_to_underscored_version(
                self.versions[0].version
            )
        else:
            return None


def dotted_to_underscored_version(version: str) -> str:
    return '_'.join(version.split('.')[:-1])


def underscored_to_dotted_version(version: str) -> str:
    return f'{version.replace("_", ".")}.0'


def bump_dotted_version_number(version: str, update_type: str) -> str:
    version_list = [int(number) for number in version.split('.')]
    if update_type == 'MAJOR':
        return '.'.join([
            str(version_list[0] + 1),
            '0',
            '0',
            '0'
        ])
    elif update_type == 'MINOR':
        return '.'.join([
            str(version_list[0]),
            str(version_list[1] + 1),
            '0',
            '0'
        ])
    elif update_type == 'PATCH':
        return '.'.join([
            str(version_list[0]),
            str(version_list[1]),
            str(version_list[2] + 1),
            '0'
        ])
    else:
        raise VersioningException(f'Invalid update_type {update_type}')
