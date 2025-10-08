import json
import os
from dataclasses import dataclass


@dataclass
class Environment:
    datastore_dir: str
    working_dir: str
    input_dir: str
    rsa_keys_directory: str
    secrets_file: str
    pseudonym_service_url: str
    datastore_api_url: str
    number_of_workers: int
    docker_host_name: str
    commit_id: str
    max_gb_all_workers: int


def _initialize_environment() -> Environment:
    return Environment(
        input_dir=os.environ["INPUT_DIR"],
        working_dir=os.environ["WORKING_DIR"],
        datastore_dir=os.environ["DATASTORE_DIR"],
        rsa_keys_directory=os.environ["RSA_KEYS_DIRECTORY"],
        secrets_file=os.environ["SECRETS_FILE"],
        pseudonym_service_url=os.environ["PSEUDONYM_SERVICE_URL"],
        datastore_api_url=os.environ["DATASTORE_API_URL"],
        number_of_workers=int(os.environ["NUMBER_OF_WORKERS"]),
        docker_host_name=os.environ["DOCKER_HOST_NAME"],
        commit_id=os.environ["COMMIT_ID"],
        max_gb_all_workers=int(os.environ["MAX_GB_ALL_WORKERS"]),
    )


environment = _initialize_environment()


@dataclass
class Secrets:
    pseudonym_service_api_key: str


def _initialize_secrets() -> Secrets:
    with open(environment.secrets_file, encoding="utf-8") as f:
        secrets_file = json.load(f)
    return Secrets(
        pseudonym_service_api_key=secrets_file["PSEUDONYM_SERVICE_API_KEY"]
    )


secrets = _initialize_secrets()
