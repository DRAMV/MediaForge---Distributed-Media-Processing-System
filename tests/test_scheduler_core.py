from scheduler.heartbeats import DEAD
from scheduler.node import Manager
from scheduler.scheduler_core import SchedulerCore


def test_round_robin_without_health_info():
    core = SchedulerCore(["a:1", "b:1", "c:1"])
    assert core.order() == ["a:1", "b:1", "c:1"]
    assert core.order() == ["b:1", "c:1", "a:1"]
    assert SchedulerCore([]).order() == []
    print("PASS plain round-robin")


def test_dead_workers_go_last():
    core = SchedulerCore(["a:1", "b:1", "c:1"], is_dead=lambda w: w == "b:1")
    assert core.order() == ["a:1", "c:1", "b:1"]     # b is dead -> last
    assert core.order() == ["c:1", "a:1", "b:1"]     # still last, turn moves on
    print("PASS dead workers are tried last")


def test_manager_asks_the_heartbeat_monitor():
    manager = Manager()
    manager.monitor.record_heartbeat("w1")
    manager.monitor._state["w1"] = DEAD               # pretend the monitor marked it
    assert manager._worker_is_dead("w1:50052") is True
    assert manager._worker_is_dead("w2:50052") is False
    assert manager._worker_is_dead("localhost:50052") is False
    print("PASS manager knows which worker is DEAD")


if __name__ == "__main__":
    test_round_robin_without_health_info()
    test_dead_workers_go_last()
    test_manager_asks_the_heartbeat_monitor()