from typing import Optional, List
from enum import Enum
import datetime

from pydantic import Extra, root_validator

from job_executor.model.camelcase_model import CamelModel
from job_executor.model import DatastoreVersion


class JobStatus(str, Enum):
    QUEUED = 'queued'
    INITIATED = 'initiated'
    VALIDATING = 'validating'
    TRANSFORMING = 'transforming'
    PSEUDONYMIZING = 'pseudonymizing'
    ENRICHING = 'enriching'
    CONVERTING = 'converting'
    BUILT = 'built'
    IMPORTING = 'importing'
    COMPLETED = 'completed'
    FAILED = 'failed'


class Operation(str, Enum):
    BUMP = 'BUMP'
    ADD = 'ADD'
    CHANGE = 'CHANGE'
    PATCH_METADATA = 'PATCH_METADATA'
    SET_STATUS = 'SET_STATUS'
    DELETE_DRAFT = 'DELETE_DRAFT'
    REMOVE = 'REMOVE'
    DELETE_ARCHIVE = 'DELETE_ARCHIVE'


class ReleaseStatus(str, Enum):
    DRAFT = 'DRAFT'
    PENDING_RELEASE = 'PENDING_RELEASE'
    PENDING_DELETE = 'PENDING_DELETE'


class UserInfo(CamelModel, extra=Extra.forbid):
    user_id: str
    first_name: str
    last_name: str


class JobParameters(CamelModel, use_enum_values=True):
    operation: Operation
    target: str
    bump_manifesto: Optional[DatastoreVersion]
    description: Optional[str]
    release_status: Optional[ReleaseStatus]
    bump_from_version: Optional[str]
    bump_to_version: Optional[str]

    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_job_type(cls, values):
        operation: Operation = values.get('operation')
        if (
            operation == Operation.BUMP
            and (
                values.get('bump_manifesto') is None or
                values.get('description') is None or
                values.get('bump_from_version') is None or
                values.get('bump_to_version') is None or
                values.get('target') != 'DATASTORE'
            )
        ):
            raise ValueError(
                'No supplied bump manifesto for BUMP operation'
            )
        elif (
            operation == Operation.REMOVE
            and values.get('description') is None
        ):
            raise ValueError(
                'Missing parameters for REMOVE operation'
            )
        elif (
            operation == Operation.SET_STATUS
            and values.get('release_status') is None
        ):
            raise ValueError(
                'Missing parameters for SET STATUS operation'
            )
        else:
            return {
                key: value for key, value in values.items()
                if value is not None
            }


class Log(CamelModel, extra=Extra.forbid):
    at: datetime.datetime
    message: str

    def dict(self, **kwargs):   # pylint: disable=unused-argument
        return {'at': self.at.isoformat(), 'message': self.message}


class Job(CamelModel, use_enum_values=True):
    job_id: str
    status: JobStatus
    parameters: JobParameters
    log: Optional[List[Log]] = []
    created_at: str
    created_by: UserInfo
