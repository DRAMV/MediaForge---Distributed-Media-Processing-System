import threading

from scheduler.dedup import Dedup, DONE, IN_PROGRESS

def test_first_claim_wins():
    d = Dedup(enabled=True)
    assert d.claim("k1") == (True, None)
    fresh, entry = d.claim("k1")
    assert fresh is False and entry.status == IN_PROGRESS

def test_complete_then_blocked():
    d = Dedup(enabled=True)
    d.claim("k1")
    assert d.complete("k1", "w1", "ok", 100) is True
    assert d.complete("k1", "w2", "again", 0) is False
    fresh, entry = d.claim("k1")
    assert fresh is False and entry.status == DONE and entry.size == 100

def test_lookup_returns_snapshot_copy():
    d = Dedup(enabled=True)
    d.claim("k1"); d.complete("k1", "w1", "ok", 5)
    snap = d.lookup("k1")
    snap.size = 999
    assert d.lookup("k1").size == 5
    assert d.lookup("missing") is None

def test_fail_releases_key_for_retry():
    d = Dedup(enabled=True)
    d.claim("k1")
    assert d.fail("k1") is True
    assert d.claim("k1")[0] is True

def test_disabled_passthrough():
    d = Dedup(enabled=False)
    assert d.claim("k1")[0] is True
    assert d.complete("k1", "w", "", 0) is False
    assert d.fail("k1") is False

def test_concurrent_claims_single_winner():
    d = Dedup(enabled=True)
    winners = []
    def racer():
        if d.claim("race")[0]:
            winners.append(1)
    threads = [threading.Thread(target=racer) for _ in range(16)]
    for th in threads: th.start()
    for th in threads: th.join()
    assert len(winners) == 1

if __name__ == "__main__":
    test_first_claim_wins()
    test_complete_then_blocked()
    test_lookup_returns_snapshot_copy()
    test_fail_releases_key_for_retry()
    test_disabled_passthrough()
    test_concurrent_claims_single_winner()