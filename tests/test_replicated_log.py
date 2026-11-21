from scheduler.replicated_log import ReplicatedLog


def test_commit_needs_majority():
    leader = ReplicatedLog("m1", total_nodes=3)
    i = leader.append(1, {"job": "J-1"})
    assert leader.commit_index == 0          # only the leader has it
    assert leader.record_ack(i, "m2") is True  # 2 of 3 = majority
    assert leader.commit_index == 1
    print("PASS commit needs majority")


def test_follower_copies_and_catches_up():
    leader = ReplicatedLog("m1", 3)
    follower = ReplicatedLog("m2", 3)
    for n in range(3):
        leader.append(1, {"job": f"J-{n}"})
    # follower is empty, so sending only entry 3 must be refused
    assert follower.accept(2, 1, leader.entries_from(3)) is False
    # leader backs up and sends everything
    assert follower.accept(0, 0, leader.entries_from(1)) is True
    assert follower.last_index() == 3
    print("PASS follower catches up")


def test_conflicting_entries_are_replaced():
    follower = ReplicatedLog("m2", 3)
    follower.accept(0, 0, [{"term": 1, "command": "old"}])
    new = [{"term": 2, "command": "new"}]
    assert follower.accept(0, 0, new) is True
    assert follower.entries == new
    print("PASS conflicts replaced")

def test_heartbeat_does_not_cut_the_log():
    follower = ReplicatedLog("m2", 3)
    follower.accept(0, 0, [{"term": 1, "command": "a"}, {"term": 1, "command": "b"}])
    assert follower.accept(1, 1, []) is True      # empty heartbeat
    assert follower.last_index() == 2
    print("PASS heartbeat keeps entries")


def test_follower_commit_is_capped():
    follower = ReplicatedLog("m2", 3)
    follower.accept(0, 0, [{"term": 1, "command": "a"}])
    follower.commit_to(5)
    assert follower.commit_index == 1
    print("PASS commit capped at last entry")

if __name__ == "__main__":
    test_commit_needs_majority()
    test_follower_copies_and_catches_up()
    test_conflicting_entries_are_replaced()
    test_heartbeat_does_not_cut_the_log()
    test_follower_commit_is_capped()