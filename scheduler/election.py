import logging
import random
import threading
import time

log = logging.getLogger(__name__)
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
                timeout_range=(1.5, 3.0), heartbeat_every=0.5,
                log_info=None):
        self.node_id = node_id
        self.peers = list(peers)
        self.transport = transport
        self.timeout_range = timeout_range
        self.heartbeat_every = heartbeat_every
        # log_info() -> (last_log_index, last_log_term) of my own log
        self.log_info = log_info or (lambda: (0, 0))

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
    def handle_vote_request(self, term, candidate_id,
                            last_log_index=0, last_log_term=0):
        with self._lock:
            if term > self.term:
                self._become_follower(term)
            my_index, my_term = self.log_info()
            # only vote for a candidate whose log is at least as up to date as mine
            log_ok = (last_log_term, last_log_index) >= (my_term, my_index)
            granted = (term == self.term
                       and self.voted_for in (None, candidate_id)
                       and log_ok)
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
        
    
    # ---- asking other managers -----------------------------------------
    def start_election(self):
        with self._lock:
            self.state = CANDIDATE
            self.term += 1
            self.voted_for = self.node_id
            my_term = self.term
            self._reset_deadline()
        print(f"[{self.node_id}] term={my_term} became CANDIDATE")

        last_index, last_term = self.log_info()
        votes = 1  # my own vote
        for peer in self.peers:
            reply = self.transport.request_vote(
                peer, my_term, self.node_id, last_index, last_term
            )
            if reply is None:
                continue
            peer_term, granted = reply
            with self._lock:
                if peer_term > self.term:
                    self._become_follower(peer_term)
                    return
            if granted:
                votes += 1

        with self._lock:
            if self.state == CANDIDATE and self.term == my_term \
                    and votes >= self._majority():
                self.state = LEADER
                self.leader_id = self.node_id
                self._last_beat = 0.0
                print(f"[{self.node_id}] term={my_term} became LEADER "
                      f"({votes} votes)")

    def _send_heartbeats(self):
        with self._lock:
            my_term = self.term
        for peer in self.peers:
            reply = self.transport.send_heartbeat(peer, my_term, self.node_id)
            if reply is None:
                continue
            with self._lock:
                if reply[0] > self.term:
                    self._become_follower(reply[0])
                    return

    # ---- the clock -------------------------------------------------------
    def tick(self):
        now = time.monotonic()
        with self._lock:
            state = self.state
            due_beat = (state == LEADER
                        and now - self._last_beat >= self.heartbeat_every)
            due_election = state != LEADER and now >= self._deadline
            if due_beat:
                self._last_beat = now
        if due_beat:
            self._send_heartbeats()
        elif due_election:
            self.start_election()

    def start(self):
        self._running = True
        self._reset_deadline()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False

    def _loop(self):
        while self._running:
            try:
                self.tick()
            except Exception:
                log.exception("election tick failed")   # keep the election alive
            time.sleep(0.05)