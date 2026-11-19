import logging
import threading
import time
from dataclasses import dataclass

from common import config

log = logging.getLogger(__name__)


@dataclass
class Lease:
    worker_id: str
    attempt: int
    expires: float


class LeaseTable:
    def __init__(self, ttl=None, clock=None, on_expire=None, check_interval=1.0):
        self._lock = threading.Lock()
        self._leases: dict[str, Lease] = {}   # active rentals only
        self._attempts: dict[str, int] = {}   # survives after a lease ends
        self.ttl = config.LEASE_SECONDS if ttl is None else ttl
        self._clock = time.monotonic if clock is None else clock
        self._on_expire = on_expire           # called as on_expire(job_id, worker_id)
        self._check_interval = check_interval
        self._stop = threading.Event()
        self._thread = None

    def grant(self, job_id, worker_id):
        """Start a lease. Returns the attempt number (1 for the first try)."""
        with self._lock:
            attempt = self._attempts.get(job_id, 0) + 1
            self._attempts[job_id] = attempt
            self._leases[job_id] = Lease(worker_id, attempt, self._clock() + self.ttl)
            return attempt

    def complete(self, job_id, worker_id):
        """True if this worker held an active lease (now removed), else False."""
        with self._lock:
            lease = self._leases.get(job_id)
            if lease is None or lease.worker_id != worker_id:
                return False
            del self._leases[job_id]
            return True

    def check(self):
        """Expire overdue leases. Returns [(job_id, worker_id), ...] expired now."""
        expired = []
        with self._lock:
            now = self._clock()
            for job_id, lease in list(self._leases.items()):
                if now > lease.expires:
                    del self._leases[job_id]
                    expired.append((job_id, lease.worker_id))
        self._fire(expired)
        return expired

    def release_worker(self, worker_id):
        """Expire ALL leases held by a worker (the heartbeat monitor declared it dead).
        Returns the released job_ids."""
        released = []
        with self._lock:
            for job_id, lease in list(self._leases.items()):
                if lease.worker_id == worker_id:
                    del self._leases[job_id]
                    released.append((job_id, worker_id))
        self._fire(released)
        return [job_id for job_id, _ in released]

    def holder(self, job_id):
        with self._lock:
            lease = self._leases.get(job_id)
            return lease.worker_id if lease is not None else None

    def attempts(self, job_id):
        with self._lock:
            return self._attempts.get(job_id, 0)

    def _fire(self, pairs):
        # always called with the lock released
        if self._on_expire is not None:
            for job_id, worker_id in pairs:
                self._on_expire(job_id, worker_id)

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
                log.exception("lease check failed")  # keep the reaper alive