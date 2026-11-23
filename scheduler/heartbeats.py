import logging
import threading
import time

from common import config

log = logging.getLogger(__name__)

ALIVE = "ALIVE"
DEAD = "DEAD"


class HeartbeatMonitor:
    def __init__(self, dead_after=None, check_interval=1.0, clock=None, on_dead=None):
        self._lock = threading.Lock()
        self._last_seen = {}
        self._state = {}
        self.dead_after = config.DEAD_AFTER_SILENCE if dead_after is None else dead_after
        self._clock = time.monotonic if clock is None else clock
        self._on_dead = on_dead
        self._check_interval = check_interval
        self._stop = threading.Event()
        self._thread = None

    def record_heartbeat(self, worker_id):
        with self._lock:
            was_dead = self._state.get(worker_id) == DEAD
            self._last_seen[worker_id] = self._clock()
            self._state[worker_id] = ALIVE
            return was_dead

    def check(self):
        newly_dead =[]
        with self._lock:
            now = self._clock()
            for worker_id, last in self._last_seen.items():
                if (self._state[worker_id] == ALIVE) and (now-last > self.dead_after):
                    self._state[worker_id] == DEAD
                    newly_dead.append(worker_id)

        if self._on_dead is not None:
            for worker_id in newly_dead:
                self._on_dead(worker_id)

        return newly_dead

    def alive_workers(self):
        with self._lock:
            return (worker_id for worker_id, state in self._state.items() if state == ALIVE)


    def snapshot(self):
        # (worker_id, state, last_seen_unix_seconds)
        with self._lock:
            now = self._clock()
            wall = time.time()
            return [(worker_id, self._state[worker_id], wall-(now-last)) 
                    for worker_id, last in self._last_seen.items()]

    def start(self):
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
            self._thread = None

    def _loop(self):
        while not self._stop.wait(self._check_interval):
            try:
                self.check()
            except Exception:
                log.exception("heartbeat check failed")  # keep the reaper alive