from common import config
from scheduler.heartbeats import HeartbeatMonitor

def test_first_heartbeats_from_unknown_worker_does_not_raise():
    m = HeartbeatMonitor(dead_after=15, clock=lambda: 0.0)
    assert m.record_heartbeat("never-seen") is False

def test_defaults_come_from_config_when_nothing_is_passed():
    m = HeartbeatMonitor()
    assert m.dead_after == config.DEAD_AFTER_SILENCE
    assert callable(m._clock) and isinstance(m._clock(), float)

if __name__ == "__main__":
    test_defaults_come_from_config_when_nothing_is_passed()
    test_first_heartbeats_from_unknown_worker_does_not_raise()