from protos import farm_pb2
from scheduler.node import Manager


def test_heartbeat_registers_a_worker_and_list_shows_it():
    manager = Manager()
    reply = manager.Heartbeat(farm_pb2.HeartbeatRequest(worker_id="worker-1", lamport=4), None)
    assert reply.ok and reply.lamport > 4          # clock moved past the worker's
    print("PASS heartbeat answered, clocks merged")

    workers = manager.ListWorkers(farm_pb2.Empty(), None).workers
    assert [w.worker_id for w in workers] == ["worker-1"]
    assert workers[0].state == "ALIVE" and workers[0].last_seen_epoch > 0
    print("PASS ListWorkers shows the worker as ALIVE")


def test_list_workers_is_empty_at_start():
    assert list(Manager().ListWorkers(farm_pb2.Empty(), None).workers) == []
    print("PASS no workers before any heartbeat")


if __name__ == "__main__":
    test_heartbeat_registers_a_worker_and_list_shows_it()
    test_list_workers_is_empty_at_start()