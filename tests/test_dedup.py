import threading
from scheduler.dedup import Dedup

N = 20


def run_in_threads(fn):
    barrier = threading.Barrier(N)          # releases all threads at once
    results = []

    def runner():
        barrier.wait()
        results.append(fn())

    threads = [threading.Thread(target=runner) for _ in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def test_one_winner_on_claim():
    d = Dedup(enabled=True)
    results = run_in_threads(lambda: d.claim("J-1")[0])
    assert results.count(True) == 1 and results.count(False) == N - 1


def test_one_winner_on_complete():
    d = Dedup(enabled=True)
    d.claim("J-1")
    results = run_in_threads(lambda: d.complete("J-1", "w1", "ok", 10))
    assert results.count(True) == 1


def test_duplicate_gets_original_outcome():
    d = Dedup(enabled=True)
    d.claim("J-1")
    d.complete("J-1", "w1", "done by w1", 10)
    first, entry = d.claim("J-1")
    assert first is False and entry.status == "DONE" and entry.detail == "done by w1"


def test_disabled_never_blocks():
    d = Dedup(enabled=False)
    assert d.claim("J-1") == (True, None)
    assert d.claim("J-1") == (True, None)


def test_unknown_complete_is_refused():
    assert Dedup(enabled=True).complete("nope", "w1", "x", 1) is False


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)