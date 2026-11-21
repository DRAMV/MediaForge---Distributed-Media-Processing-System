from scheduler.node import Manager
from scheduler.replicated_log import ReplicatedLog

JOB = {"op": "ENQUEUE_JOB", "job_id": "J-1", "filename": "a.mp4", "task_type": "transcode"}


def test_follower_creates_then_forgets_a_job():
    log = ReplicatedLog("m1", total_nodes=1)       # one manager: commits at once
    manager = Manager(None, log)

    log.append(1, JOB)
    manager.apply_committed()
    assert manager.store.state("J-1") == "QUEUED"
    print("PASS committed ENQUEUE_JOB creates the job")

    log.append(1, {"op": "JOB_DONE", "job_id": "J-1"})
    manager.apply_committed()
    assert manager.store.state("J-1") is None
    print("PASS committed JOB_DONE removes the job")


def test_applying_twice_changes_nothing():
    log = ReplicatedLog("m1", total_nodes=1)
    manager = Manager(None, log)
    log.append(1, JOB)
    manager.apply_committed()
    manager.apply_committed()
    assert manager.store.state("J-1") == "QUEUED"
    print("PASS applying twice is harmless")


if __name__ == "__main__":
    test_follower_creates_then_forgets_a_job()
    test_applying_twice_changes_nothing()