import os

from job_executor.config import environment


def test_config_from_environment():
    assert environment.input_dir == os.environ.get("INPUT_DIR")
    assert environment.working_dir == os.environ.get("WORKING_DIR")
    assert environment.datastore_dir == os.environ.get("DATASTORE_DIR")
    assert environment.pseudonym_service_url == (
        os.environ.get("PSEUDONYM_SERVICE_URL")
    )
    assert environment.datastore_api_url == (
        os.environ.get("DATASTORE_API_URL")
    )
