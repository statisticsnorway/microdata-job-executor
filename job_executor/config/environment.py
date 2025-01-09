import os


def _initialize_environment() -> dict:
    return {
        "INPUT_DIR": os.environ["INPUT_DIR"],
        "WORKING_DIR": os.environ["WORKING_DIR"],
        "DATASTORE_DIR": os.environ["DATASTORE_DIR"],
        "RSA_KEYS_DIRECTORY": os.environ["RSA_KEYS_DIRECTORY"],
        "PSEUDONYM_SERVICE_URL": os.environ["PSEUDONYM_SERVICE_URL"],
        "JOB_SERVICE_URL": os.environ["JOB_SERVICE_URL"],
        "NUMBER_OF_WORKERS": int(os.environ["NUMBER_OF_WORKERS"]),
        "SECRETS_FILE": os.environ["SECRETS_FILE"],
        "DOCKER_HOST_NAME": os.environ["DOCKER_HOST_NAME"],
        "COMMIT_ID": os.environ["COMMIT_ID"],
        "DYNAMIC_WORKER_THRESHOLD": os.environ["DYNAMIC_WORKER_THRESHOLD"],
    }


_ENVIRONMENT_VARIABLES = _initialize_environment()


def get(key: str) -> str:
    return _ENVIRONMENT_VARIABLES[key]
