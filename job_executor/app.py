import logging
import time

from job_executor.adapter import datastore_api
from job_executor.adapter.fs import LocalStorageAdapter
from job_executor.common.exceptions import StartupException
from job_executor.config import environment
from job_executor.config.log import setup_logging
from job_executor.domain import rollback
from job_executor.domain.manager import Manager

logger = logging.getLogger()
setup_logging()


def initialize_app() -> Manager:
    """
    Initializing the datastore by rolling back any unfinished jobs in any
    datastore, and checking if any datastores have unresolved temporary backups.

    Returns a manager if all datastores appear healthy.
    """
    try:
        rollback.fix_interrupted_jobs()
        for rdn in datastore_api.get_datastores():
            local_storage = LocalStorageAdapter(
                datastore_api.get_datastore_directory(rdn)
            )
            if local_storage.datastore_dir.temporary_backup_exists():
                raise StartupException(f"tmp directory exists for {rdn}")
        return Manager(
            max_workers=environment.number_of_workers,
            max_bytes_all_workers=(
                environment.max_gb_all_workers * 1024**3
            ),  # Covert from GB to bytes
        )
    except Exception as e:
        raise StartupException("Exception when initializing") from e


def main() -> None:
    manager = initialize_app()
    try:
        while True:
            time.sleep(5)
            job_query_result = datastore_api.query_for_jobs()
            manager.handle_jobs(job_query_result)
    except Exception as e:
        raise e
    finally:
        manager.close_logging_thread()


if __name__ == "__main__":
    logger.info("Polling for jobs...")
    try:
        main()
    except Exception as e:
        logger.exception("Service stopped by exception", exc_info=e)
