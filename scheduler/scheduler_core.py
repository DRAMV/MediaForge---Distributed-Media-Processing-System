import threading


class SchedulerCore:
    """Keeps track of workers and picks one for each incoming job (round-robin), thread-safe."""

    def __init__(self, workers, is_dead=None):
        self.workers = workers  # List of worker addresses/IDs
        self.is_dead = is_dead or (lambda worker: False)   # True = known DEAD
        self._index = 0
        self._lock = threading.Lock()

    def order(self):
        """All workers, starting with the next one in line (round-robin).

        Workers known to be DEAD go to the back, so a healthy one is tried
        first. A dead worker is still tried last, in case it is back.
        """
        if not self.workers:
            return []
        with self._lock:
            start = self._index
            self._index = (self._index + 1) % len(self.workers)
        ordered = self.workers[start:] + self.workers[:start]
        healthy = [w for w in ordered if not self.is_dead(w)]
        dead = [w for w in ordered if self.is_dead(w)]
        return healthy + dead