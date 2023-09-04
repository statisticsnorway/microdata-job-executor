from job_executor.model.camelcase_model import CamelModel


class MaintenanceStatus(CamelModel):
    paused: bool
    msg: str
