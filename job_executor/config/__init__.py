import json
import os
from dataclasses import dataclass


@dataclass
class Environment:
    datastore_dir: str
    datastore_rdn: str
    secrets_file: str
    pseudonym_service_url: str
    datastore_api_url: str
    number_of_workers: int
    docker_host_name: str
    commit_id: str
    max_gb_all_workers: int
    private_keys_dir: str


def _initialize_environment() -> Environment:
    return Environment(
        datastore_dir=os.environ["DATASTORE_DIR"],
        datastore_rdn=os.environ["DATASTORE_RDN"],
        secrets_file=os.environ["SECRETS_FILE"],
        pseudonym_service_url=os.environ["PSEUDONYM_SERVICE_URL"],
        datastore_api_url=os.environ["DATASTORE_API_URL"],
        number_of_workers=int(os.environ["NUMBER_OF_WORKERS"]),
        docker_host_name=os.environ["DOCKER_HOST_NAME"],
        commit_id=os.environ["COMMIT_ID"],
        max_gb_all_workers=int(os.environ["MAX_GB_ALL_WORKERS"]),
        private_keys_dir=os.environ["PRIVATE_KEYS_DIR"],
    )


environment = _initialize_environment()


@dataclass
class Secrets:
    pseudonym_service_api_key: str
    datastore_api_service_key: str


def _initialize_secrets() -> Secrets:
    with open(environment.secrets_file, encoding="utf-8") as f:
        secrets_file = json.load(f)
    return Secrets(
        pseudonym_service_api_key=secrets_file["PSEUDONYM_SERVICE_API_KEY"],
        datastore_api_service_key=secrets_file["DATASTORE_API_SERVICE_KEY"],
    )


secrets = _initialize_secrets()
