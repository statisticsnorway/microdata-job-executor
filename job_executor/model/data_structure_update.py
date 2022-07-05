from pydantic import Extra

from job_executor.model.camelcase_model import CamelModel
from job_executor.exception.exception import (
    ReleaseStatusException
)


class DataStructureUpdate(CamelModel, extra=Extra.forbid):
    name: str
    description: str
    operation: str
    release_status: str

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
        self.release_status = new_status
