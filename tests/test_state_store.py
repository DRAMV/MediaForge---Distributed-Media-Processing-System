from scheduler.state_store import JobStore


def test_assign_unknown_job_does_not_crash():
    store = JobStore()
    assert store.assign("J-nope", "worker-1") is False
    print("PASS assign on an unknown job returns False")


def test_assign_known_job():
    store = JobStore()
    store.create("J-1", "a.mp4", "transcode")
    assert store.assign("J-1", "worker-1") is True
    assert store.state("J-1") == "ASSIGNED"
    print("PASS assign marks the job ASSIGNED")


if __name__ == "__main__":
    test_assign_unknown_job_does_not_crash()
    test_assign_known_job()