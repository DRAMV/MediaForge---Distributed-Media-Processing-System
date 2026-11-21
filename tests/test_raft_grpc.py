import time
from concurrent import futures

import grpc

from protos import farm_pb2_grpc
from scheduler.election import ElectionNode, LEADER
from scheduler.node import GrpcTransport, RaftService

ADDRESSES = ["localhost:50061", "localhost:50062", "localhost:50063"]


def start_manager(address):
    peers = [a for a in ADDRESSES if a != address]
    node = ElectionNode(address, peers, GrpcTransport(),
                        timeout_range=(0.3, 0.6), heartbeat_every=0.1)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    farm_pb2_grpc.add_RaftServicer_to_server(RaftService(node), server)
    server.add_insecure_port(address)
    server.start()
    node.start()
    return node, server


def wait_for_leader(nodes, alive, seconds=6):
    end = time.time() + seconds
    while time.time() < end:
        found = [a for a in alive if nodes[a][0].state == LEADER]
        if len(found) == 1:
            return found[0]
        time.sleep(0.1)
    raise AssertionError("no single leader")


def test_real_grpc_election_and_failover():
    nodes = {a: start_manager(a) for a in ADDRESSES}
    alive = list(ADDRESSES)
    first = wait_for_leader(nodes, alive)
    old_term = nodes[first][0].term
    print(f"PASS leader elected over gRPC: {first}")

    node, server = nodes[first]          # kill the leader
    node.stop()
    server.stop(0)
    alive.remove(first)
    second = wait_for_leader(nodes, alive)
    assert second != first and nodes[second][0].term > old_term
    print(f"PASS new leader after kill: {second}")

    for a in alive:
        nodes[a][0].stop()
        nodes[a][1].stop(0)


if __name__ == "__main__":
    test_real_grpc_election_and_failover()