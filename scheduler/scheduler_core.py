class SchedulerCore:
    """Keeps track of workers and picks one for each incoming job (round-robin)."""

    def __init__(self, workers):
        self.workers = workers  # List of worker addresses/IDs
        self._index = 0

    def next_worker(self):
        """Selects the next worker in line to take a job."""
        if not self.workers:
            return None
        worker = self.workers[self._index]
        # Move to the next worker, wrapping around to the start if at the end
        self._index = (self._index + 1) % len(self.workers)
        return worker