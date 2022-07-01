from typing import List, Union
from pydantic import BaseModel, root_validator

from job_executor.model import Metadata
from job_executor.adapter import local_storage_adapter


class DataStoreInfo(BaseModel):
    name: str
    label: str
    description: str
    languageCode: str


class MetadataAll(BaseModel):
    dataStore: DataStoreInfo
    dataStructures: List[Metadata]
    iter: int = 0

    def __iter__(self):
        self.iter = 0
        return self

    def __next__(self):
        if self.iter < len(self.dataStructures):
            index = self.iter
            self.iter = self.iter + 1
            return Metadata(**self.dataStructures[index].dict())
        else:
            raise StopIteration

    def get(self, dataset_name: str) -> Union[Metadata, None]:
        for metadata in self.dataStructures:
            if metadata.name == dataset_name:
                return Metadata(**metadata.dict())
        return None

    def dict(self):
        return {
            "dataStore": self.dataStore.dict(),
            "dataStructures": [
                metadata.dict() for metadata in self.dataStructures
            ]
        }


class MetadataAllDraft(MetadataAll):

    @root_validator(pre=True)
    @classmethod
    def read_file(cls, _):
        return local_storage_adapter.get_metadata_all('DRAFT')

    def _write_to_file(self):
        local_storage_adapter.write_metadata_all(
            self.dict(),
            'DRAFT'
        )

    def remove(self, dataset_name: str):
        self.dataStructures = [
            metadata for metadata in self.dataStructures
            if metadata.name != dataset_name
        ]
        self._write_to_file()

    def remove_all(self):
        self.dataStructures = []
        self._write_to_file()

    def add(self, metadata: Metadata):
        self.dataStructures.append(metadata)
        self._write_to_file()
