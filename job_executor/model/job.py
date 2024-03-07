from typing import Optional, List
from enum import StrEnum
import datetime

from pydantic import model_validator

from job_executor.model.camelcase_model import CamelModel
from job_executor.model import DatastoreVersion


class JobStatus(StrEnum):
    QUEUED = "queued"
    INITIATED = "initiated"
    DECRYPTING = "decrypting"
    VALIDATING = "validating"
    TRANSFORMING = "transforming"
    PSEUDONYMIZING = "pseudonymizing"
    ENRICHING = "enriching"
    CONVERTING = "converting"
    BUILT = "built"
    IMPORTING = "importing"
    COMPLETED = "completed"
    FAILED = "failed"


class Operation(StrEnum):
    BUMP = "BUMP"
    ADD = "ADD"
    CHANGE = "CHANGE"
    PATCH_METADATA = "PATCH_METADATA"
    SET_STATUS = "SET_STATUS"
    DELETE_DRAFT = "DELETE_DRAFT"
    REMOVE = "REMOVE"
    DELETE_ARCHIVE = "DELETE_ARCHIVE"


class ReleaseStatus(StrEnum):
    DRAFT = "DRAFT"
    PENDING_RELEASE = "PENDING_RELEASE"
    PENDING_DELETE = "PENDING_DELETE"


class UserInfo(CamelModel, extra="forbid"):
    user_id: str
    first_name: str
    last_name: str


class JobParameters(CamelModel, use_enum_values=True):
    operation: Operation
    target: str
    bump_manifesto: Optional[DatastoreVersion] = None
    description: Optional[str] = None
    release_status: Optional[ReleaseStatus] = None
    bump_from_version: Optional[str] = None
    bump_to_version: Optional[str] = None

    @model_validator(mode="after")
    def validate_job_type(self: "JobParameters"):
        operation: Operation = self.operation
        if operation == Operation.BUMP and (
            self.bump_manifesto is None
            or self.description is None
            or self.bump_from_version is None
            or self.bump_to_version is None
            or self.target != "DATASTORE"
        ):
            raise ValueError("No supplied bump manifesto for BUMP operation")
        elif operation == Operation.REMOVE and self.description is None:
            raise ValueError("Missing parameters for REMOVE operation")
        elif operation == Operation.SET_STATUS and self.release_status is None:
            raise ValueError("Missing parameters for SET STATUS operation")
        else:
            return self


class Log(CamelModel, extra="forbid"):
    at: datetime.datetime
    message: str


class Job(CamelModel, use_enum_values=True):
    job_id: str
    status: JobStatus
    parameters: JobParameters
    log: Optional[List[Log]] = []
    created_at: str
    created_by: UserInfo
