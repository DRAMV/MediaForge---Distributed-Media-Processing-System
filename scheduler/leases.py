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
    def __init__(self, lease_time=None, clock=None, on_expire=None, check_interval=1.0):
        self._lock = threading.Lock()
        self._leases: dict[str, Lease] = {}   # active rentals only
        self._attempts: dict[str, int] = {}   # survives after a lease ends
        self.lease_time = config.LEASE_SECONDS if lease_time is None else lease_time
        self._clock = time.monotonic if clock is None else clock
        self._on_expire = on_expire           # called as on_expire(job_id, worker_id)
        self._check_interval = check_interval
        self._stop = threading.Event()
        self._thread = None
