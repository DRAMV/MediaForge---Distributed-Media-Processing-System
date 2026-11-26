from scheduler.leases import LeaseTable

def test_grant_accepts_float_lease_time():
    """Regression for PR #66: self.lease_time() called as a function -> TypeError."""
    t = {"now": 100.0}
    lt = LeaseTable(lease_time=15.0, clock=lambda: t["now"])
    assert lt.grant("job-1", "worker-1") == 1
    assert lt.get_holder("job-1") == "worker-1"

def test_complete_only_by_holder():
    lt = LeaseTable(lease_time=15.0, clock=lambda: 0.0)
    lt.grant("job-1", "w1")
    assert lt.complete("job-1", "w2") is False
    assert lt.complete("job-1", "w1") is True
    assert lt.get_holder("job-1") is None

def test_expiry_fires_callback():
    fired = []
    t = {"now": 0.0}
    lt = LeaseTable(lease_time=15.0, clock=lambda: t["now"],
                    on_expire=lambda j, w: fired.append((j, w)))
    lt.grant("job-1", "w1")
    t["now"] = 16.0
    assert lt.check() == [("job-1", "w1")]
    assert fired == [("job-1", "w1")]
    assert lt.complete("job-1", "w1") is False

def test_superseded_attempt_cannot_complete():
    lt = LeaseTable(lease_time=15.0, clock=lambda: 0.0)
    lt.grant("job-1", "slow")
    lt.grant("job-1", "fast")
    assert lt.attempts("job-1") == 2
    assert lt.complete("job-1", "slow") is False
    assert lt.complete("job-1", "fast") is True

def test_release_worker():
    lt = LeaseTable(lease_time=15.0, clock=lambda: 0.0)
    lt.grant("a", "w1"); lt.grant("b", "w1"); lt.grant("c", "w2")
    assert sorted(lt.release_worker("w1")) == ["a", "b"]
    assert lt.get_holder("c") == "w2"

if __name__ == "__main__":
    test_grant_accepts_float_lease_time()
    test_complete_only_by_holder()
    test_expiry_fires_callback()
    test_superseded_attempt_cannot_complete()
    test_release_worker()