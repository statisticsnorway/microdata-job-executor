from job_executor.model.camelcase_model import CamelModel


class MaintenanceStatus(CamelModel):
    id: str
    paused: bool
    msg: str
    created_at: str
