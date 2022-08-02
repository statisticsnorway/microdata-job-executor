import logging
from typing import List
import requests
from requests.exceptions import HTTPError

from job_executor.config import environment, secrets


PSEUDONYM_SERVICE_URL = environment.get('PSEUDONYM_SERVICE_URL')
PSEUDONYM_SERVICE_API_KEY = secrets.get('PSEUDONYM_SERVICE_API_KEY')


logger = logging.getLogger()


def pseudonymize(idents: List[str], unit_id: str, job_id: str) -> dict:
    response = requests.post(
        f'{PSEUDONYM_SERVICE_URL}?unit_id_type={unit_id}&job_id={job_id}',
        json=idents,
        headers={
            'Content-Type': 'application/json',
            'X-API-Key': PSEUDONYM_SERVICE_API_KEY
        }
    )
    if response.status_code != 200:
        raise HTTPError(
            'Error in request to pseudonym-service with status code '
            f'{response.status_code}: {response.text}'
        )
    return response.json()
