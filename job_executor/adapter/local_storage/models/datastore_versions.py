from datetime import UTC, datetime
from typing import Iterator

from job_executor.adapter import local_storage
from job_executor.common.exceptions import (
    BumpException,
    ExistingDraftException,
    NoSuchDraftException,
    ReleaseStatusException,
    UnnecessaryUpdateException,
    VersioningException,
)
from job_executor.common.models import CamelModel


class DataStructureUpdate(CamelModel, extra="forbid"):
    name: str
    description: str
    operation: str
    release_status: str

    def set_release_status(self, new_status: str) -> None:
        if new_status == "PENDING_RELEASE":
            if self.operation not in ["ADD", "CHANGE", "PATCH_METADATA"]:
                raise ReleaseStatusException(
                    f"Can't set release status: {new_status} "
                    f"for dataset with operation: {self.operation}"
                )
        elif new_status == "PENDING_DELETE":
            if self.operation != "REMOVE":
                raise ReleaseStatusException(
                    f"Can't set release status: {new_status} "
                    f"for dataset with operation: {self.operation}"
                )
        elif new_status == "DRAFT":
            if self.operation == "REMOVE":
                raise ReleaseStatusException(
                    f"Can't set release status: {new_status} "
                    f"for dataset with operation: {self.operation}"
                )
        elif new_status != "DRAFT":
            raise ReleaseStatusException(
                f"Invalid release status: {new_status}"
            )
        self.release_status = new_status


class DatastoreVersion(CamelModel):
    version: str
    description: str
    release_time: int
    language_code: str
    update_type: str | None
    data_structure_updates: list[DataStructureUpdate]

    def __iter__(self) -> Iterator[DataStructureUpdate]:  # type: ignore
        return iter(
            [
                DataStructureUpdate(
                    **update.model_dump(by_alias=True, exclude_none=True)
                )
                for update in self.data_structure_updates
            ]
        )

    def _get_current_epoch_seconds(self) -> int:
        return int(
            (
                datetime.now(UTC).replace(tzinfo=None)
                - datetime.fromtimestamp(0, UTC).replace(tzinfo=None)
            ).total_seconds()
        )

    def _calculate_update_type(self) -> None:
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

    def get_dataset_release_status(self, dataset_name: str) -> str | None:
        for update in self.data_structure_updates:
            if update.name == dataset_name:
                return update.release_status
        return None

    def get_dataset_operation(self, dataset_name: str) -> str | None:
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
    def add(self, data_structure_update: DataStructureUpdate) -> None:
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
                f"Can't delete draft for {dataset_name} as no such draft exists"
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
            update.model_dump(by_alias=True, exclude_none=True)
            for update in self.data_structure_updates
            if update.release_status != "DRAFT"
        ]
        other_pending_operations = [
            update.model_dump(by_alias=True, exclude_none=True)
            for update in bump_manifesto.data_structure_updates
            if update.release_status != "DRAFT"
        ]
        if len(pending_operations) != len(other_pending_operations):
            return False
        for pending_operation in other_pending_operations:
            if pending_operation not in pending_operations:
                return False
        return True

    def release_pending(self) -> tuple[list[DataStructureUpdate], str]:
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

    def _write_to_file(self) -> None:
        local_storage.write_draft_version(self.model_dump(by_alias=True))

    def set_draft_release_status(
        self, dataset_name: str, new_status: str
    ) -> None:
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

    def _set_release_time(self) -> None:
        self.release_time = self._get_current_epoch_seconds()
        version_list = list(self.version.split("."))
        version_list[-1] = str(self.release_time)
        self.version = ".".join(version_list)


class DatastoreVersions(CamelModel, extra="forbid"):
    name: str
    label: str
    description: str
    versions: list[DatastoreVersion]

    def _write_to_file(self) -> None:
        local_storage.write_datastore_versions(self.model_dump(by_alias=True))

    def _get_current_epoch_seconds(self) -> int:
        return int(
            (
                datetime.now(UTC).replace(tzinfo=None)
                - datetime.fromtimestamp(0, UTC).replace(tzinfo=None)
            ).total_seconds()
        )

    def add_new_release_version(
        self,
        data_structure_updates: list[DataStructureUpdate],
        description: str,
        update_type: str,
    ) -> str:
        released_data_structure_updates = [
            DataStructureUpdate(
                name=data_structure.name,
                description=data_structure.description,
                operation=data_structure.operation,
                release_status=(
                    "DELETED"
                    if data_structure.operation == "REMOVE"
                    else "RELEASED"
                ),
            )
            for data_structure in data_structure_updates
        ]
        new_version_number = (
            "1.0.0.0"
            if self.versions == []
            else bump_dotted_version_number(
                self.versions[0].version, update_type
            )
        )
        new_release_version = DatastoreVersion(
            version=new_version_number,
            description=description,
            release_time=self._get_current_epoch_seconds(),
            language_code="no",
            update_type=update_type,
            data_structure_updates=released_data_structure_updates,
        )
        self.versions = [new_release_version] + self.versions
        self._write_to_file()
        return dotted_to_underscored_version(new_version_number)

    def get_dataset_release_status(self, dataset_name: str) -> str | None:
        for version in self.versions:
            release_status = version.get_dataset_release_status(dataset_name)
            if release_status is not None:
                return release_status
        return None

    def get_latest_version_number(self) -> str | None:
        if len(self.versions):
            return dotted_to_underscored_version(self.versions[0].version)
        else:
            return None


def dotted_to_underscored_version(version: str) -> str:
    return "_".join(version.split(".")[:-1])


def underscored_to_dotted_version(version: str) -> str:
    return f"{version.replace('_', '.')}.0"


def bump_dotted_version_number(version: str, update_type: str) -> str:
    version_list = [int(number) for number in version.split(".")]
    if update_type == "MAJOR":
        return ".".join([str(version_list[0] + 1), "0", "0", "0"])
    elif update_type == "MINOR":
        return ".".join(
            [str(version_list[0]), str(version_list[1] + 1), "0", "0"]
        )
    elif update_type == "PATCH":
        return ".".join(
            [
                str(version_list[0]),
                str(version_list[1]),
                str(version_list[2] + 1),
                "0",
            ]
        )
    else:
        raise VersioningException(f"Invalid update_type {update_type}")
