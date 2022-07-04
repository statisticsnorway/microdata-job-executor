from typing import Optional
from pydantic import Extra, ValidationError, root_validator
from enum import Enum

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
    CHANGE_DATA = 'CHANGE_DATA'
    PATCH_METADATA = 'PATCH_METADATA'
    SET_STATUS = 'SET_STATUS'
    DELETE_DRAFT = 'DELETE_DRAFT'
    REMOVE = 'REMOVE'


class ReleaseStatus(str, Enum):
    DRAFT = 'DRAFT'
    PENDING_RELEASE = 'PENDING_RELEASE'
    PENDING_DELETE = 'PENDING_DELETE'


class JobParameters(CamelModel, use_enum_values=True):
    dataset_name: str
    bump_manifesto: Optional[DatastoreVersion]
    description: Optional[str]
    release_status: Optional[ReleaseStatus]

    @root_validator(skip_on_failure=True)
    @classmethod
    def remove_none_values(cls, values):
        return {
            key: value for key, value in values.items()
            if value is not None
        }


class Job(CamelModel, extra=Extra.forbid, use_enum_values=True):
    job_id: str
    operation: Operation
    status: JobStatus
    parameters: JobParameters

    @root_validator(skip_on_failure=True)
    @classmethod
    def validate_job_type(cls, values):
        operation: Operation = values['operation']
        parameters: JobParameters = values['parameters']
        if operation == Operation.BUMP:
            if (
                parameters.bump_manifesto is None or
                parameters.description is None
            ):
                raise ValidationError(
                    'No supplied bump manifesto for BUMP operation'
                )
        elif operation == Operation.SET_STATUS:
            if (
                parameters.dataset_name is None or
                parameters.release_status is None
            ):
                raise ValidationError(
                    'Missing parameters for SET STATUS operation'
                )
        else:
            if parameters.dataset_name is None:
                raise ValidationError(
                    f'Missing parameter for {operation} operation'
                )
        return values
