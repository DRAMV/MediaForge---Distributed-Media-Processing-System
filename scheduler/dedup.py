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