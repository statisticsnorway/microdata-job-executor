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
    iter: int = 0

    def __iter__(self):
        self.iter = 0
        return self

    def __next__(self):
        if self.iter < len(self.data_structures):
            index = self.iter
            self.iter = self.iter + 1
            return Metadata(**self.data_structures[index].dict())
        else:
            raise StopIteration

    def get(self, dataset_name: str) -> Union[Metadata, None]:
        for metadata in self.data_structures:
            if metadata.name == dataset_name:
                return Metadata(**metadata.dict())
        return None

    def dict(self):
        return {
            "dataStore": self.data_store.dict(by_alias=True),
            "dataStructures": [
                metadata.dict(by_alias=True)
                for metadata in self.data_structures
            ]
        }


class MetadataAllDraft(MetadataAll):

    @root_validator(pre=True)
    @classmethod
    def read_file(cls, _):
        return local_storage.get_metadata_all('DRAFT')

    def _write_to_file(self):
        local_storage.write_metadata_all(
            self.dict(),
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
