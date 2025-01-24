import time
from job_executor.worker.manager_state import ManagerState
from job_executor.model.worker import Worker
from multiprocessing import Process


def dummy():
    time.sleep(10)
    print("hello")


def test_initial_state():
    manager_state = ManagerState()

    assert manager_state.current_total_size == 0
    assert len(manager_state.workers) == 0


def test_can_spawn_worker():
    manager_state = ManagerState()

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1)
    assert can_spawn is True


def test_cannot_spawn_worker_too_many_workers():
    manager_state = ManagerState(default_max_workers=4)

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
    manager_state = ManagerState(max_gb_all_workers=20)

    # register large job
    large_job = Worker(
        process=Process(target=dummy),
        job_id="job_large",
        job_size=TWENTY_GB,
    )
    manager_state.register_job(large_job)
    large_job.start()

    # Max worker should now be 2, We can still spawn a second job
    can_spawn = manager_state.can_spawn_new_worker(new_job_size=TWENTY_GB)
    assert can_spawn is True

    large_job = Worker(
        process=Process(target=dummy),
        job_id="job_large_2",
        job_size=TWENTY_GB,
    )
    manager_state.register_job(large_job)
    large_job.start()

    # We should not be able to spawn a third job
    can_spawn = manager_state.can_spawn_new_worker(new_job_size=TWENTY_GB)
    assert can_spawn is False


def test_oversized_jobs():
    # we can run jobs which exceeds the limit for the threshold,
    # but only two of them
    FIFTY_GB = 50 * 1024**3
    TEN_GB = 10 * 1024**3
    manager_state = ManagerState(max_gb_all_workers=10)

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=FIFTY_GB)
    large_job = Worker(
        process=Process(target=dummy),
        job_id="job_1",
        job_size=FIFTY_GB,
    )
    manager_state.register_job(large_job)
    large_job.start()
    assert can_spawn is True

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=FIFTY_GB)
    large_job = Worker(
        process=Process(target=dummy),
        job_id="job_2",
        job_size=FIFTY_GB,
    )
    manager_state.register_job(large_job)
    large_job.start()
    assert can_spawn is True

    assert manager_state.max_bytes_all_workers == TEN_GB
    assert manager_state.current_total_size == (FIFTY_GB + FIFTY_GB)

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=FIFTY_GB)
    assert can_spawn is False


def test_unregister_job():
    manager_state = ManagerState(default_max_workers=4)

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
