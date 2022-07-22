import json

from job_executor.config import environment


def _initialize_secrets() -> dict:
    with open(environment.get('SECRETS_FILE'), encoding='utf-8') as f:
        secrets_file = json.load(f)
    return {
        'PSEUDONYM_SERVICE_API_KEY': secrets_file['PSEUDONYM_SERVICE_API_KEY']
    }


_SECRETS = _initialize_secrets()


def get(key: str) -> str:
    return _SECRETS[key]
