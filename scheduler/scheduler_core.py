import threading

class SchedulerCore:
    """Keeps track of workers and picks one for each incoming job (round-robin), thread-safe."""

    def __init__(self, workers):
        self.workers = workers  # List of worker addresses/IDs
        self._index = 0
        self._lock = threading.Lock()

    def next_worker(self):
        """Selects the next worker in line to take a job safely across threads."""
        if not self.workers:
            return None
        with self._lock:
            worker = self.workers[self._index]
            self._index = (self._index + 1) % len(self.workers)
            return worker