import random
import threading
import time

FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"


class ElectionNode:
    """One manager's election brain. No network code in here.

    `transport` is anything with two methods:
        request_vote(peer, term, candidate_id)  -> (term, granted) or None
        send_heartbeat(peer, term, leader_id)   -> (term, ok) or None
    None means "peer did not answer".
    """

    def __init__(self, node_id, peers, transport,
                 timeout_range=(1.5, 3.0), heartbeat_every=0.5):
        self.node_id = node_id
        self.peers = list(peers)
        self.transport = transport
        self.timeout_range = timeout_range
        self.heartbeat_every = heartbeat_every

        self.state = FOLLOWER
        self.term = 0
        self.voted_for = None
        self.leader_id = None

        self._lock = threading.RLock()
        self._deadline = 0.0
        self._last_beat = 0.0
        self._running = False
        self._thread = None
        self._reset_deadline()

    # ---- small helpers -------------------------------------------------
    def _majority(self):
        return (len(self.peers) + 1) // 2 + 1

    def _reset_deadline(self):
        low, high = self.timeout_range
        self._deadline = time.monotonic() + random.uniform(low, high)

    def _become_follower(self, term):
        if term > self.term:
            self.term = term
            self.voted_for = None
        if self.state != FOLLOWER:
            print(f"[{self.node_id}] term={self.term} became FOLLOWER")
        self.state = FOLLOWER

    # ---- answering other managers (called by the network layer) ---------
    def handle_vote_request(self, term, candidate_id):
        with self._lock:
            if term > self.term:
                self._become_follower(term)
            granted = (term == self.term
                       and self.voted_for in (None, candidate_id))
            if granted:
                self.voted_for = candidate_id
                self._reset_deadline()
            return self.term, granted

    def handle_heartbeat(self, term, leader_id):
        with self._lock:
            if term < self.term:
                return self.term, False
            self._become_follower(term)
            self.leader_id = leader_id
            self._reset_deadline()
            return self.term, True