from dataclasses import dataclass

from job_executor.domain.manager import Manager


@dataclass
class MockedWorker:
    job_id: str
    job_size: int

    def is_alive(self) -> bool:
        return True

    def start(self) -> None: ...


def test_initial_state():
    manager = Manager(
        max_workers=4,
        max_bytes_all_workers=50 * 1024**3,
    )

    assert manager.current_total_size == 0
    assert len(manager.workers) == 0
    manager.close_logging_thread()


def test_can_spawn_worker():
    manager = Manager(
        max_workers=4,
        max_bytes_all_workers=50 * 1024**3,
    )

    can_spawn = manager.can_spawn_new_worker(new_job_size=1)
    assert can_spawn is True
    manager.close_logging_thread()


def test_cannot_spawn_worker_too_many_workers():
    manager = Manager(
        max_workers=4,
        max_bytes_all_workers=50 * 1024**3,
    )

    # Register 4 jobs
    for i in range(4):
        worker = MockedWorker(
            job_id=f"job_{i}",
            job_size=1024,
        )
        manager.workers.append(worker)  # type: ignore
        worker.start()

    can_spawn = manager.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is False
    manager.close_logging_thread()


def test_cannot_spawn_worker_size_limit_reached():
    TWENTY_GB = 20 * 1024**3
    manager = Manager(
        max_workers=20,
        max_bytes_all_workers=TWENTY_GB,
    )

    large_job = MockedWorker(
        job_id="job_large",
        job_size=TWENTY_GB,
    )
    manager.workers.append(large_job)  # type: ignore
    large_job.start()

    # Only one job active but size limit is reached cannot spawn new job
    can_spawn = manager.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is False
    manager.close_logging_thread()


def test_oversized_jobs():
    FIFTY_GB = 50 * 1024**3
    TEN_GB = 10 * 1024**3
    manager = Manager(
        max_workers=4,
        max_bytes_all_workers=20 * 1024**3,
    )

    # This job will never be processed
    can_spawn = manager.can_spawn_new_worker(new_job_size=FIFTY_GB)
    assert can_spawn is False

    # This job will be accepted
    can_spawn = manager.can_spawn_new_worker(new_job_size=TEN_GB)
    assert can_spawn is True
    worker = MockedWorker(
        job_id="job_2",
        job_size=TEN_GB,
    )
    manager.workers.append(worker)  # type: ignore
    worker.start()
    manager.close_logging_thread()


def test_unregister_job():
    manager = Manager(
        max_workers=4,
        max_bytes_all_workers=50 * 1024**3,
    )

    # Register 4 jobs
    for i in range(4):
        worker = MockedWorker(
            job_id=f"job_{i}",
            job_size=1024,
        )
        manager.workers.append(worker)  # type: ignore
        worker.start()

    can_spawn = manager.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is False

    manager.unregister_worker("job_1")
    can_spawn = manager.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is True
    manager.close_logging_thread()
