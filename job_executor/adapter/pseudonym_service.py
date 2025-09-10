from typing import List

import requests
from microdata_tools.validation.model.metadata import UnitIdType

from job_executor.config import environment, secrets
from job_executor.exception import HttpResponseError

PSEUDONYM_SERVICE_URL = environment.get("PSEUDONYM_SERVICE_URL")
PSEUDONYM_SERVICE_API_KEY = secrets.get("PSEUDONYM_SERVICE_API_KEY")


def pseudonymize(
    idents: List[str], unit_id_type: UnitIdType, job_id: str
) -> dict:
    response = requests.post(
        f"{PSEUDONYM_SERVICE_URL}?unit_id_type={unit_id_type.value}&job_id={job_id}",
        json=idents,
        headers={
            "Content-Type": "application/json",
            "X-API-Key": PSEUDONYM_SERVICE_API_KEY,
        },
        timeout=4000,
    )
    if response.status_code != 200:
        raise HttpResponseError(f"{response.status_code}: {response.text}")
    return response.json()
