from typing import List, Union
from pydantic import root_validator, model_validator
from job_executor.exception import BumpException

from job_executor.model.camelcase_model import CamelModel
from job_executor.model import Metadata
from job_executor.adapter import local_storage
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
    data_structures: List[Metadata]
    languages: List[LanguageInfo]

    def __iter__(self):
        return iter(
            [
                Metadata(**data_structure.model_dump(by_alias=True, exclude_none=True))
                for data_structure in self.data_structures
            ]
        )

    def get(self, dataset_name: str) -> Union[Metadata, None]:
        for metadata in self.data_structures:
            if metadata.name == dataset_name:
                return Metadata(**metadata.model_dump())
        return None


class MetadataAllDraft(MetadataAll):
    @root_validator(pre=True)
    @classmethod
    def read_file(cls, _):
        return local_storage.get_metadata_all("DRAFT")

    def _write_to_file(self):
        local_storage.write_metadata_all(self.model_dump(by_alias=True, exclude_none=True), "DRAFT")

    def remove(self, dataset_name: str):
        self.data_structures = [
            metadata
            for metadata in self.data_structures
            if metadata.name != dataset_name
        ]
        self._write_to_file()

    def remove_all(self):
        self.data_structures = []
        self._write_to_file()

    def add(self, metadata: Metadata):
        self.data_structures.append(metadata)
        self._write_to_file()

    def rebuild(
        self,
        released_metadata: List[Metadata],
        draft_version: DatastoreVersion,
    ):
        previous_data_structures = [ds for ds in self.data_structures]
        self.data_structures = [
            Metadata(**m.model_dump(by_alias=True, exclude_none=True)) for m in released_metadata
        ]
        for draft in draft_version:
            self.remove(draft.name)
            if draft.operation != "REMOVE":
                draft_metadata = next(
                    ds
                    for ds in previous_data_structures
                    if ds.name == draft.name
                )
                if draft_metadata is None:
                    raise BumpException(
                        "Could not rebuild metadata_all__DRAFT. "
                        f"No metadata for {draft.name} in previous "
                        "metadata_all__DRAFT"
                    )
                self.add(draft_metadata)
        self._write_to_file()
