from dataclasses import dataclass
from typing import Literal

from job_executor.adapter import datastore_api
from job_executor.adapter.datastore_api.models import Job
from job_executor.adapter.fs import LocalStorageAdapter

handler_type = Literal["worker"] | Literal["manager"]


@dataclass
class JobContext:
    job: Job
    handler: handler_type
    local_storage: LocalStorageAdapter
    job_size: int | None = None


def build_job_context(job: Job, handler: handler_type) -> JobContext:
    local_storage = LocalStorageAdapter(
        datastore_api.get_datastore_directory(job.datastore_rdn)
    )
    job_size = (
        local_storage.input_dir.get_importable_tar_size_in_bytes(
            job.parameters.target
        )
        if handler == "worker"
        else None
    )
    return JobContext(
        job=job, handler=handler, local_storage=local_storage, job_size=job_size
    )
