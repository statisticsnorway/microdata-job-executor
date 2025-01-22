from job_executor.worker.manager_state import ManagerState


def test_initial_state():
    manager_state = ManagerState()

    assert manager_state.current_total_size == 0
    assert len(manager_state.datasets) == 0


def test_can_spawn_worker():
    manager_state = ManagerState()

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1)
    assert can_spawn is True


def test_cannot_spawn_worker_too_many_workers():
    manager_state = ManagerState(default_max_workers=4)

    # Register 4 jobs
    for i in range(4):
        manager_state.register_job(f"job_{i}", 1024)

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is False


def test_cannot_spawn_worker_size_limit_reached():
    TWENTY_GB = 20 * 1024**3
    manager_state = ManagerState(max_gb_all_workers=20)

    # register large job
    manager_state.register_job("job_1", TWENTY_GB)  # 20 GB

    # Max worker should now be 2, We can still spawn a second job
    can_spawn = manager_state.can_spawn_new_worker(new_job_size=TWENTY_GB)
    assert can_spawn is True

    manager_state.register_job("job_2", TWENTY_GB)

    # We should not be able to spawn a third job
    can_spawn = manager_state.can_spawn_new_worker(new_job_size=TWENTY_GB)
    assert can_spawn is False


def test_oversized_jobs():
    # we can run jobs which exceeds the limit for the threshold,
    # but only two of them
    FIFTY_GB = 50 * 1024**3
    manager_state = ManagerState(max_gb_all_workers=10)

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=FIFTY_GB)
    manager_state.register_job("job_1", FIFTY_GB)
    assert can_spawn is True

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=FIFTY_GB)
    manager_state.register_job("job_2", FIFTY_GB)
    assert can_spawn is True

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=FIFTY_GB)
    assert can_spawn is False


def test_unregister_job():
    manager_state = ManagerState(default_max_workers=4)

    for i in range(4):
        manager_state.register_job(f"job_{i}", 1024)

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is False

    manager_state.unregister_job("job_1")
    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is True


def test_reset():
    manager_state = ManagerState(default_max_workers=4)

    for i in range(4):
        manager_state.register_job(f"job_{i}", 1024)

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is False

    manager_state.reset()

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is True
