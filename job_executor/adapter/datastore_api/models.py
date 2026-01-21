import datetime
from enum import StrEnum

from pydantic import model_validator

from job_executor.adapter.fs.models.datastore_versions import (
    DatastoreVersion,
)
from job_executor.common.models import CamelModel


class DatastoreResponse(CamelModel):
    datastore_id: int
    name: str
    rdn: str
    description: str
    directory: str
    bump_enabled: bool


class MaintenanceStatus(CamelModel):
    paused: bool
    msg: str
    timestamp: str


class JobStatus(StrEnum):
    QUEUED = "queued"
    INITIATED = "initiated"
    DECRYPTING = "decrypting"
    VALIDATING = "validating"
    TRANSFORMING = "transforming"
    PSEUDONYMIZING = "pseudonymizing"
    ENRICHING = "enriching"
    CONVERTING = "converting"
    PARTITIONING = "partitioning"
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
    ROLLBACK_REMOVE = "ROLLBACK_REMOVE"
    DELETE_ARCHIVE = "DELETE_ARCHIVE"
    GENERATE_RSA_KEYS = "GENERATE_RSA_KEYS"


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
    bump_manifesto: DatastoreVersion | None = None
    description: str | None = None
    release_status: ReleaseStatus | None = None
    bump_from_version: str | None = None
    bump_to_version: str | None = None

    @model_validator(mode="after")
    def validate_job_type(self) -> "JobParameters":
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
    datastore_rdn: str
    status: JobStatus
    parameters: JobParameters
    log: list[Log] | None = []
    created_at: str
    created_by: UserInfo


class JobQueryResult:
    queued_worker_jobs: list[Job]
    built_jobs: list[Job]
    queued_manager_jobs: list[Job]

    def __init__(
        self,
        queued_worker_jobs: list[Job] = [],
        built_jobs: list[Job] = [],
        queued_manager_jobs: list[Job] = [],
    ) -> None:
        self.queued_worker_jobs = queued_worker_jobs
        self.built_jobs = built_jobs
        self.queued_manager_jobs = queued_manager_jobs

    @property
    def available_jobs_count(self) -> int:
        return (
            len(self.queued_worker_jobs)
            + len(self.built_jobs)
            + len(self.queued_manager_jobs)
        )

    def queued_manager_and_built_jobs(self) -> list[Job]:
        return self.queued_manager_jobs + self.built_jobs
