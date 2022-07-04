from typing import List, Union
from pydantic import root_validator

from job_executor.model.camelcase_model import CamelModel
from job_executor.model import Metadata
from job_executor.adapter import local_storage


class DataStoreInfo(CamelModel):
    name: str
    label: str
    description: str
    language_code: str


class MetadataAll(CamelModel):
    data_store: DataStoreInfo
    data_structures: List[Metadata]

    def __iter__(self):
        return iter([
            Metadata(**data_structure.dict(by_alias=True))
            for data_structure in self.data_structures
        ])

    def get(self, dataset_name: str) -> Union[Metadata, None]:
        for metadata in self.data_structures:
            if metadata.name == dataset_name:
                return Metadata(**metadata.dict())
        return None


class MetadataAllDraft(MetadataAll):

    @root_validator(pre=True)
    @classmethod
    def read_file(cls, _):
        return local_storage.get_metadata_all('DRAFT')

    def _write_to_file(self):
        local_storage.write_metadata_all(
            self.dict(by_alias=True),
            'DRAFT'
        )

    def remove(self, dataset_name: str):
        self.data_structures = [
            metadata for metadata in self.data_structures
            if metadata.name != dataset_name
        ]
        self._write_to_file()

    def remove_all(self):
        self.data_structures = []
        self._write_to_file()

    def add(self, metadata: Metadata):
        self.data_structures.append(metadata)
        self._write_to_file()
