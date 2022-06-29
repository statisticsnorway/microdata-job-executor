export INPUT_DIR=''
export WORKING_DIR=''
export DATASTORE_DIR=''
export PSEUDONYM_SERVICE_URL=''
export JOB_SERVICE_URL=''

docker compose up -d
python3 local_test.py
