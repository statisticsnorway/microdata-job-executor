from typing import Optional
from pydantic import BaseModel, Extra, ValidationError, root_validator
from enum import Enum

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


class JobParameters(BaseModel, use_enum_values=True):
    datasetName: str
    bumpManifesto: Optional[DatastoreVersion]
    description: Optional[str]
    releaseStatus: Optional[ReleaseStatus]

    @root_validator(skip_on_failure=True)
    def remove_none_values(cls, values):
        return {
            key: value for key, value in values.items()
            if value is not None
        }


class Job(BaseModel, extra=Extra.forbid, use_enum_values=True):
    jobId: str
    operation: Operation
    status: JobStatus
    parameters: JobParameters

    @root_validator(skip_on_failure=True)
    def validate_job_type(cls, values):
        operation = values['operation']
        parameters = values['parameters']
        if operation == Operation.BUMP:
            if (
                parameters.bumpManifesto is None or
                parameters.description is None
            ):
                raise ValidationError(
                    'No supplied bump manifesto for BUMP operation'
                )
        elif operation == Operation.SET_STATUS:
            if (
                parameters.datasetName is None or
                parameters.releaseStatus is None
            ):
                raise ValidationError(
                    'Missing parameters for SET STATUS operation'
                )
        else:
            if parameters.datasetName is None:
                raise ValidationError(
                    f'Missing parameter for {operation} operation'
                )
        return values
