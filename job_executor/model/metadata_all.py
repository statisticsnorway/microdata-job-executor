from collections.abc import Iterator

from pydantic import model_validator

from job_executor.adapter import local_storage
from job_executor.exception import BumpException
from job_executor.model import Metadata
from job_executor.model.camelcase_model import CamelModel
from job_executor.model.datastore_version import DatastoreVersion


class DataStoreInfo(CamelModel):
    name: str
    label: str
    description: str
    language_code: str


class LanguageInfo(CamelModel):
    code: str
    label: str


class MetadataAll(CamelModel):
    data_store: DataStoreInfo
    data_structures: list[Metadata]
    languages: list[LanguageInfo]

    def __iter__(self) -> Iterator[Metadata]:  # type: ignore
        return iter(
            [
                Metadata(
                    **data_structure.model_dump(
                        by_alias=True, exclude_none=True
                    )
                )
                for data_structure in self.data_structures
            ]
        )

    def get(self, dataset_name: str) -> Metadata | None:
        for metadata in self.data_structures:
            if metadata.name == dataset_name:
                return Metadata(
                    **metadata.model_dump(by_alias=True, exclude_none=True)
                )
        return None


class MetadataAllDraft(MetadataAll):
    @model_validator(mode="before")
    @classmethod
    def read_file(cls, _) -> dict:  # noqa
        return local_storage.get_metadata_all("DRAFT")

    def _write_to_file(self) -> None:
        local_storage.write_metadata_all(
            self.model_dump(by_alias=True, exclude_none=True), "DRAFT"
        )

    def remove(self, dataset_name: str) -> None:
        self.data_structures = [
            metadata
            for metadata in self.data_structures
            if metadata.name != dataset_name
        ]
        self._write_to_file()

    def update_one(self, dataset_name: str, metadata: Metadata) -> None:
        self.data_structures = [
            metadata
            for metadata in self.data_structures
            if metadata.name != dataset_name
        ]
        self.data_structures.append(metadata)
        self._write_to_file()

    def remove_all(self) -> None:
        self.data_structures = []
        self._write_to_file()

    def add(self, metadata: Metadata) -> None:
        self.data_structures.append(metadata)
        self._write_to_file()

    def rebuild(
        self,
        released_metadata: list[Metadata],
        draft_version: DatastoreVersion,
    ) -> None:
        previous_data_structures = {ds.name: ds for ds in self.data_structures}
        new_data_structures = {
            ds.name: Metadata(**ds.model_dump(by_alias=True, exclude_none=True))
            for ds in released_metadata
        }
        for draft in draft_version:
            if draft.operation == "REMOVE":
                del new_data_structures[draft.name]
            else:
                draft_metadata = previous_data_structures.get(draft.name, None)
                if draft_metadata is None:
                    raise BumpException(
                        "Could not rebuild metadata_all__DRAFT. "
                        f"No metadata for {draft.name} in previous "
                        "metadata_all__DRAFT"
                    )
                new_data_structures[draft.name] = draft_metadata
        self.data_structures = list(new_data_structures.values())
        self._write_to_file()
