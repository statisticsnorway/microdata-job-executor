import os

from job_executor.config import environment


def test_config_from_environment():
    assert environment.get("INPUT_DIR") == os.environ.get("INPUT_DIR")
    assert environment.get("WORKING_DIR") == os.environ.get("WORKING_DIR")
    assert environment.get("DATASTORE_DIR") == os.environ.get("DATASTORE_DIR")
    assert environment.get("PSEUDONYM_SERVICE_URL") == (
        os.environ.get("PSEUDONYM_SERVICE_URL")
    )
    assert environment.get("JOB_SERVICE_URL") == (
        os.environ.get("JOB_SERVICE_URL")
    )
