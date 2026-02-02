import os

os.environ["DATASTORE_DIR"] = "tests/resources/datastores/TEST_DATASTORE"
os.environ["DATASTORE_RDN"] = "no.ssb.test"
os.environ["PSEUDONYM_SERVICE_URL"] = "http://mock.pseudonym.service"
os.environ["DATASTORE_API_URL"] = "http://mock.job.service"
os.environ["NUMBER_OF_WORKERS"] = "4"
os.environ["SECRETS_FILE"] = "tests/resources/secrets/secrets.json"
os.environ["DOCKER_HOST_NAME"] = "localhost"
os.environ["COMMIT_ID"] = "abc123"
os.environ["MAX_GB_ALL_WORKERS"] = "50"
os.environ["PRIVATE_KEYS_DIR"] = "tests/integration/resources/private_keys"
