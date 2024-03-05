from typing import List, Union, Tuple
from datetime import datetime

from pydantic import model_validator

from job_executor.model.camelcase_model import CamelModel
from job_executor.model.data_structure_update import DataStructureUpdate
from job_executor.adapter import local_storage
from job_executor.exception import (
    BumpException,
    ExistingDraftException,
    NoSuchDraftException,
    UnnecessaryUpdateException,
)


class DatastoreVersion(CamelModel):
    version: str
    description: str
    release_time: int
    language_code: str
    update_type: Union[str, None]
    data_structure_updates: List[DataStructureUpdate]

    def __iter__(self):
        return iter(
            [
                DataStructureUpdate(**update.model_dump(by_alias=True, exclude_none=True))
                for update in self.data_structure_updates
            ]
        )

    def _get_current_epoch_seconds(self):
        return int(
            (datetime.now() - datetime.utcfromtimestamp(0)).total_seconds()
        )

    def _calculate_update_type(self):
        pending_operations = [
            update.operation
            for update in self.data_structure_updates
            if update.release_status != "DRAFT"
        ]
        if "CHANGE" in pending_operations or "REMOVE" in pending_operations:
            self.update_type = "MAJOR"
        elif "ADD" in pending_operations:
            self.update_type = "MINOR"
        elif "PATCH_METADATA" in pending_operations:
            self.update_type = "PATCH"
        else:
            self.update_type = None

    def get_dataset_release_status(
        self, dataset_name: str
    ) -> Union[str, None]:
        for update in self.data_structure_updates:
            if update.name == dataset_name:
                return update.release_status
        return None

    def get_dataset_operation(self, dataset_name: str) -> Union[str, None]:
        for update in self.data_structure_updates:
            if update.name == dataset_name:
                return update.operation
        return None

    def contains(self, dataset_name: str) -> bool:
        return any(
            [
                update.name == dataset_name
                for update in self.data_structure_updates
            ]
        )


class DraftVersion(DatastoreVersion):
    @model_validator(mode="before")
    @classmethod
    def read_file(cls, _):
        return local_storage.get_draft_version()

    def add(self, data_structure_update: DataStructureUpdate):
        current_update_names = [update.name for update in self]
        if data_structure_update.name in current_update_names:
            raise ExistingDraftException(
                f"Draft for {data_structure_update.name} already exists"
            )
        self.data_structure_updates.append(data_structure_update)
        self._set_release_time()
        self._calculate_update_type()
        self._write_to_file()

    def delete_draft(self, dataset_name: str) -> DataStructureUpdate:
        deleted_draft = next(
            (
                update
                for update in self.data_structure_updates
                if update.name == dataset_name
            ),
            None,
        )
        if deleted_draft is None:
            raise NoSuchDraftException(
                f"Can't delete draft for {dataset_name}"
                " as no such draft exists"
            )
        self.data_structure_updates = [
            update
            for update in self.data_structure_updates
            if update.name != dataset_name
        ]
        self._set_release_time()
        self._calculate_update_type()
        self._write_to_file()
        return deleted_draft

    def validate_bump_manifesto(
        self, bump_manifesto: "DatastoreVersion"
    ) -> bool:
        pending_operations = [
            update.model_dump()
            for update in self.data_structure_updates
            if update.release_status != "DRAFT"
        ]
        other_pending_operations = [
            update.model_dump()
            for update in bump_manifesto.data_structure_updates
            if update.release_status != "DRAFT"
        ]
        if len(pending_operations) != len(other_pending_operations):
            return False
        for pending_operation in other_pending_operations:
            if pending_operation not in pending_operations:
                return False
        return True

    def release_pending(self) -> Tuple[List[DataStructureUpdate], str]:
        if self.update_type is None:
            raise BumpException("No pending operations in draft version")
        draft_updates = []
        pending_updates = []
        for update in self.data_structure_updates:
            if update.release_status == "DRAFT":
                draft_updates.append(update)
            else:
                pending_updates.append(update)
        update_type = self.update_type
        self.data_structure_updates = draft_updates
        self._set_release_time()
        self._calculate_update_type()
        self._write_to_file()
        return pending_updates, update_type

    def _write_to_file(self):
        local_storage.write_draft_version(self.model_dump(by_alias=True))

    def set_draft_release_status(self, dataset_name: str, new_status: str):
        dataset_update = next(
            update
            for update in self.data_structure_updates
            if update.name == dataset_name
        )
        if dataset_update is None:
            raise NoSuchDraftException(f"No draft for dataset {dataset_name}")
        if dataset_update.release_status == new_status:
            raise UnnecessaryUpdateException(
                f"Status already set to {new_status}"
            )
        dataset_update.set_release_status(new_status)
        self._set_release_time()
        self._calculate_update_type()
        self._write_to_file()

    def _set_release_time(self):
        self.release_time = self._get_current_epoch_seconds()
        version_list = list(self.version.split("."))
        version_list[-1] = str(self.release_time)
        self.version = ".".join(version_list)
