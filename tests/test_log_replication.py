import time
from concurrent import futures

import grpc

from protos import farm_pb2_grpc
from scheduler.election import ElectionNode, LEADER
from scheduler.node import GrpcTransport, RaftService
from scheduler.replicated_log import ReplicatedLog

ADDRESSES = ["localhost:50071", "localhost:50072", "localhost:50073"]


def start_manager(address, log=None):
    log = log or ReplicatedLog(address, len(ADDRESSES))
    peers = [a for a in ADDRESSES if a != address]
    node = ElectionNode(address, peers, GrpcTransport(log=log),
                        timeout_range=(0.3, 0.6), heartbeat_every=0.1)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    farm_pb2_grpc.add_RaftServicer_to_server(RaftService(node, log), server)
    server.add_insecure_port(address)
    server.start()
    node.start()
    return {"node": node, "server": server, "log": log}


def stop(m):
    m["node"].stop()
    m["server"].stop(0)


def wait_until(check, seconds=5):
    end = time.time() + seconds
    while time.time() < end:
        if check():
            return True
        time.sleep(0.05)
    return False


def find_leader(managers):
    assert wait_until(lambda: any(
        m["node"].state == LEADER for m in managers.values()))
    return next(a for a, m in managers.items() if m["node"].state == LEADER)


def test_job_reaches_all_managers_and_commits():
    managers = {a: start_manager(a) for a in ADDRESSES}
    leader = managers[find_leader(managers)]
    index = leader["log"].append(leader["node"].term, {"op": "ENQUEUE_JOB", "job_id": "J-1"})
    assert wait_until(lambda: leader["log"].commit_index >= index)
    print("PASS leader commits after a majority has the entry")
    assert wait_until(lambda: all(
        m["log"].commit_index == 1 and m["log"].last_index() == 1
        for m in managers.values()))
    print("PASS every manager has the entry and knows it is committed")
    for m in managers.values():
        stop(m)


def test_commit_with_one_manager_down_and_catch_up():
    managers = {a: start_manager(a) for a in ADDRESSES}
    leader_addr = find_leader(managers)
    leader = managers[leader_addr]
    follower_addr = next(a for a in ADDRESSES if a != leader_addr)
    old_log = managers[follower_addr]["log"]
    stop(managers[follower_addr])                 # one follower goes down

    for n in (1, 2):
        index = leader["log"].append(leader["node"].term, {"op": "ENQUEUE_JOB", "job_id": f"J-{n}"})
    assert wait_until(lambda: leader["log"].commit_index >= index)
    print("PASS commits with 1 of 3 managers down")

    managers[follower_addr] = start_manager(follower_addr, log=old_log)   # it comes back
    assert wait_until(lambda: old_log.last_index() == 2 and old_log.commit_index == 2)
    print("PASS restarted manager catches up")
    for m in managers.values():
        stop(m)


if __name__ == "__main__":
    test_job_reaches_all_managers_and_commits()
    test_commit_with_one_manager_down_and_catch_up()