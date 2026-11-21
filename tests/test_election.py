import time

from scheduler.election import ElectionNode, LEADER


class FakeNetwork:
    """Pretend network: managers call each other directly in memory."""

    def __init__(self):
        self.nodes = {}
        self.down = set()

    def transport_for(self, me):
        net = self

        class T:
            def request_vote(self, peer, term, cand, last_index=0, last_term=0):
                if me in net.down or peer in net.down:
                    return None
                return net.nodes[peer].handle_vote_request(
                    term, cand, last_index, last_term)

            def send_heartbeat(self, peer, term, leader):
                if me in net.down or peer in net.down:
                    return None
                return net.nodes[peer].handle_heartbeat(term, leader)

        return T()


def make_cluster(ids=("m1", "m2", "m3")):
    net = FakeNetwork()
    for i in ids:
        peers = [p for p in ids if p != i]
        net.nodes[i] = ElectionNode(
            i, peers, net.transport_for(i),
            timeout_range=(0.3, 0.6), heartbeat_every=0.1)
    for n in net.nodes.values():
        n.start()
    return net


def leaders(net):
    return [n for i, n in net.nodes.items()
            if n.state == LEADER and i not in net.down]


def wait_for_one_leader(net, seconds=4):
    end = time.time() + seconds
    while time.time() < end:
        if len(leaders(net)) == 1:
            return leaders(net)[0]
        time.sleep(0.05)
    raise AssertionError("no single leader appeared")


def test_one_leader_elected():
    net = make_cluster()
    leader = wait_for_one_leader(net)
    time.sleep(1)
    assert len(leaders(net)) == 1 and leaders(net)[0] is leader
    for n in net.nodes.values():
        n.stop()
    print("PASS one leader elected")


def test_new_leader_after_crash():
    net = make_cluster()
    first = wait_for_one_leader(net)
    old_term = first.term
    net.down.add(first.node_id)
    first.stop()
    second = wait_for_one_leader(net)
    assert second is not first and second.term > old_term
    for n in net.nodes.values():
        n.stop()
    print("PASS new leader after crash")


def test_no_leader_without_majority():
    net = make_cluster()
    net.down.update(["m2", "m3"])   # m1 is alone: it can never get 2 of 3 votes
    time.sleep(2)
    assert net.nodes["m1"].state != LEADER
    for n in net.nodes.values():
        n.stop()
    print("PASS no leader without majority")

def test_vote_refused_to_a_candidate_with_an_older_log():
    voter = ElectionNode("m1", [], None, log_info=lambda: (3, 2))
    # candidate's log ends at term 1 (older than my term 2) -> refuse
    assert voter.handle_vote_request(5, "m2", 4, 1) == (5, False)
    # same last term but shorter log -> refuse
    assert voter.handle_vote_request(6, "m2", 2, 2) == (6, False)
    # log at least as up to date -> grant
    assert voter.handle_vote_request(7, "m3", 3, 2) == (7, True)
    print("PASS vote refused for an older log")

if __name__ == "__main__":
    test_one_leader_elected()
    test_new_leader_after_crash()
    test_no_leader_without_majority()
    test_vote_refused_to_a_candidate_with_an_older_log()