from job_executor.worker.manager_state import ManagerState


def test_initial_state():
    manager_state = ManagerState()

    assert manager_state.current_total_size == 0
    assert len(manager_state.datasets) == 0


def test_can_spawn_worker():
    manager_state = ManagerState()

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1)
    assert can_spawn is True


def test_can_not_spawn_worker_to_many_workers():
    manager_state = ManagerState(default_max_workers=4)

    # Register 4 jobs
    for i in range(4):
        manager_state.register_job(f"job_{i}", 1024)

    can_spawn = manager_state.can_spawn_new_worker(new_job_size=1024)
    assert can_spawn is False


def test_can_not_spawn_worker_size_limit_reached():
    manager_state = ManagerState(dynamic_worker_threshold=20 * 1024**3)  # 20GB

    # register large job
    manager_state.register_job("job_1", 20 * 1024**3)  # 20 GB

    # Max worker should now be 2, We can still spawn a second job
    can_spawn = manager_state.can_spawn_new_worker(new_job_size=10 * 1024**3)
    assert can_spawn is True

    manager_state.register_job("job_2", 10 * 1024**3)

    # We should not be able to spawn a third job
    can_spawn = manager_state.can_spawn_new_worker(new_job_size=10 * 1024**3)
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
