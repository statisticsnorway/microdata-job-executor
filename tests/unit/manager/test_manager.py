import time
from multiprocessing import Process

from job_executor.manager import Manager
from job_executor.model.worker import Worker


def dummy():
    time.sleep(10)
    print("hello")  # noqa


def test_initial_state():
    manager = Manager(
        max_workers=4,
        max_bytes_all_workers=50 * 1024**3,
        this_datastore=None,  # type: ignore
    )

    assert manager.current_total_size == 0
    assert len(manager.workers) == 0


def test_can_spawn_worker():
    manager = Manager(
        max_workers=4,
        max_bytes_all_workers=50 * 1024**3,
        this_datastore=None,  # type: ignore
    )

    can_spawn = manager.can_spawn_new_worker(new_job_size=1)
    assert can_spawn is True


def test_cannot_spawn_worker_too_many_workers():
    manager = Manager(
        max_workers=4,
        max_bytes_all_workers=50 * 1024**3,
        this_datastore=None,  # type: ignore
    )

    # Register 4 jobs
    for i in range(4):
        worker = Worker(
            process=Process(target=dummy),
            job_id=f"job_{i}",
            job_size=1024,
        )
        manager.workers.append(worker)
        worker.start()

    can_spawn = manager.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is False


def test_cannot_spawn_worker_size_limit_reached():
    TWENTY_GB = 20 * 1024**3
    manager = Manager(
        max_workers=20,
        max_bytes_all_workers=TWENTY_GB,
        this_datastore=None,  # type: ignore
    )

    large_job = Worker(
        process=Process(target=dummy),
        job_id="job_large",
        job_size=TWENTY_GB,
    )
    manager.workers.append(large_job)
    large_job.start()

    # Only one job active but size limit is reached cannot spawn new job
    can_spawn = manager.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is False


def test_oversized_jobs():
    FIFTY_GB = 50 * 1024**3
    TEN_GB = 10 * 1024**3
    manager = Manager(
        max_workers=4,
        max_bytes_all_workers=20 * 1024**3,
        this_datastore=None,  # type: ignore
    )

    # This job will never be processed
    can_spawn = manager.can_spawn_new_worker(new_job_size=FIFTY_GB)
    assert can_spawn is False

    # This job will be accepted
    can_spawn = manager.can_spawn_new_worker(new_job_size=TEN_GB)
    assert can_spawn is True
    if can_spawn:
        worker = Worker(
            process=Process(target=dummy),
            job_id="job_2",
            job_size=TEN_GB,
        )
        manager.workers.append(worker)
        worker.start()


def test_unregister_job():
    manager = Manager(
        max_workers=4,
        max_bytes_all_workers=50 * 1024**3,
        this_datastore=None,  # type: ignore
    )

    # Register 4 jobs
    for i in range(4):
        worker = Worker(
            process=Process(target=dummy),
            job_id=f"job_{i}",
            job_size=1024,
        )
        manager.workers.append(worker)
        worker.start()

    can_spawn = manager.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is False

    manager.unregister_worker("job_1")
    can_spawn = manager.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is True
