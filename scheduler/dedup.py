import threading
import time
from dataclasses import dataclass, replace

from common import config

IN_PROGRESS = "IN_PROGRESS"
DONE = "DONE"


@dataclass
class Entry:
    status: str = IN_PROGRESS
    worker_id: str = ""
    detail: str = ""
    size: int = 0
    completed_at: float | None = None


class Dedup:
    def __init__(self, enabled=None):
        self._entries = {}
        self._lock =threading.Lock()
        self.enabled = config.ENABLE_DEDUP if enabled is None else enabled

    def claim (self, key):
        if not self.enabled:
            return (True, None)
        existing = self._entries.get(key)

        if existing is not None:
            return (False, replace(existing))

        self._entries[key] = Entry()
        return (True, None)

    def complete(self, key, worker_id, detail, size):
        if not self.enabled:
            return False

        entry = self._entries.get(key)
        if entry is None or entry.status == DONE:
            return False

        entry.status = DONE
        entry.worker_id = worker_id
        entry.detail = detail
        entry.size = size
        entry.compelted_at = time.time()
        return True

    def lookup(self, key):
        with self._lock:
            entry = self._entries.get(key)
            return replace(entry) if entry is not None else None