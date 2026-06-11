# JOB-EXECUTOR

This application is the data import pipeline for the microdata.no platform.
Because the system is multi-tenant, it can operate on multiple datastores, and it is the only
process on the server allowed to update a datastore.

## Program flow
- The manager process starts up and verifies the state of the system
    - If any jobs are in progress but not finished on startup, we can assume that they
      got interrupted from a job-executor restart.
    - The manager resets these jobs in the appropriate manner to ensure no bad state
      before continuing
- The manager asks the datastore-api for jobs that are queued
- If a job entails a new dataset import, the manager assigns a worker
    - A worker starts in a subprocess and unpackages and validates the dataset
      using the microdata-tools package
    - The worker then transforms the metadata to the correct format for storage
    - The worker pseudonymizes columns of the data depending on metadata definitions
    - The worker optionally partitions the parquet
    - The manager can now see that the job has registered the dataset as built and
      can import it into the datastore
- If a job entails a change to the datastore, or datasets in the datastore
    - The manager processes the jobs one by one and makes changes to the datastore
      as requested

## Modules
- **job_executor/**: directory containing the source code
    - **domain/**: all core domain logic for the application
        - **manager/**: logic concerning the manager process
        - **worker/**: logic concerning the worker processes
        - **datastores.py**: logic concerning the updating of the datastores
        - **rollback.py**: logic concerning rolling back incomplete jobs
    - **adapter/**: adapters for filesystem and external services
        - **datastore_api/**: client for datastore-api that holds job information
        - **fs/**: client for the filesystem
        - **pseudonym_service.py**: client for the pseudonymization service
    - **common/**: common modules used by the whole stack
    - **config/**: configuration for application and logging
    - **app.py**: the entry point for application startup


## Development Workflow (uv)
- Use `uv` for Python package and environment management.
- Add dependencies with `uv add <package>` (and dev dependencies with `uv add --dev <package>`).
- Format code with `uv run ruff format`.
- Run autofixes with `uv run ruff check --fix`.
- Sort imports specifically with `uv run ruff check --fix --select I`.
- Run tests with `uv run pytest`
