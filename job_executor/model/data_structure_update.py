from job_executor.exception import ReleaseStatusException
from job_executor.model.camelcase_model import CamelModel


class DataStructureUpdate(CamelModel, extra="forbid"):
    name: str
    description: str
    operation: str
    release_status: str

    def set_release_status(self, new_status: str):
        if new_status == "PENDING_RELEASE":
            if self.operation not in ["ADD", "CHANGE", "PATCH_METADATA"]:
                raise ReleaseStatusException(
                    f"Can't set release status: {new_status} "
                    f"for dataset with operation: {self.operation}"
                )
        elif new_status == "PENDING_DELETE":
            if self.operation != "REMOVE":
                raise ReleaseStatusException(
                    f"Can't set release status: {new_status} "
                    f"for dataset with operation: {self.operation}"
                )
        elif new_status == "DRAFT":
            if self.operation == "REMOVE":
                raise ReleaseStatusException(
                    f"Can't set release status: {new_status} "
                    f"for dataset with operation: {self.operation}"
                )
        elif new_status != "DRAFT":
            raise ReleaseStatusException(
                f"Invalid release status: {new_status}"
            )
        self.release_status = new_status
