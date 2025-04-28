import time
from job_executor.manager import Manager
from job_executor.model.worker import Worker
from multiprocessing import Process


def dummy():
    time.sleep(10)
    print("hello")


def test_initial_state():
    manager_state = Manager(max_workers=4, max_bytes_all_workers=50 * 1024**3)

    assert manager_state.current_total_size == 0
    assert len(manager_state.workers) == 0


def test_can_spawn_worker():
    manager_state = Manager(max_workers=4, max_bytes_all_workers=50 * 1024**3)

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1)
    assert can_spawn is True


def test_cannot_spawn_worker_too_many_workers():
    manager_state = Manager(max_workers=4, max_bytes_all_workers=50 * 1024**3)

    # Register 4 jobs
    for i in range(4):
        worker = Worker(
            process=Process(target=dummy),
            job_id=f"job_{i}",
            job_size=1024,
        )
        manager_state.register_job(worker)
        worker.start()

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is False


def test_cannot_spawn_worker_size_limit_reached():
    TWENTY_GB = 20 * 1024**3
    manager_state = Manager(max_workers=20, max_bytes_all_workers=TWENTY_GB)

    large_job = Worker(
        process=Process(target=dummy),
        job_id="job_large",
        job_size=TWENTY_GB,
    )
    manager_state.register_job(large_job)
    large_job.start()

    # Only one job active but size limit is reached cannot spawn new job
    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is False


def test_oversized_jobs():
    FIFTY_GB = 50 * 1024**3
    TEN_GB = 10 * 1024**3
    manager_state = Manager(max_workers=4, max_bytes_all_workers=20 * 1024**3)

    # This job will never be processed
    can_spawn = manager_state.can_spawn_new_worker(new_job_size=FIFTY_GB)
    assert can_spawn is False

    # This job will be accepted
    can_spawn = manager_state.can_spawn_new_worker(new_job_size=TEN_GB)
    assert can_spawn is True
    if can_spawn:
        worker = Worker(
            process=Process(target=dummy),
            job_id="job_2",
            job_size=TEN_GB,
        )
        manager_state.register_job(worker)
        worker.start()


def test_unregister_job():
    manager_state = Manager(max_workers=4, max_bytes_all_workers=50 * 1024**3)

    # Register 4 jobs
    for i in range(4):
        worker = Worker(
            process=Process(target=dummy),
            job_id=f"job_{i}",
            job_size=1024,
        )
        manager_state.register_job(worker)
        worker.start()

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is False

    manager_state.unregister_job("job_1")
    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is True
