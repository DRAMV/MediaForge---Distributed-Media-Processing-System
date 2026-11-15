import threading


class JobStore:
    """The order board: one record per job, safe for many threads."""

    def __init__(self):
        self._jobs = {}
        self._lock = threading.Lock()

    def create(self, job_id, filename, task_type):
        with self._lock:
            self._jobs[job_id] = {
                "state": "QUEUED",
                "filename": filename,
                "task_type": task_type,
                "worker_id": None,
                "result": None,
                "done": threading.Event(),
            }

    def assign(self, job_id, worker_id):
        with self._lock:
            self._jobs[job_id]["state"] = "ASSIGNED"
            self._jobs[job_id]["worker_id"] = worker_id

    def complete(self, job_id, result):
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None or job["state"] == "DONE":
                return False
            job["state"] = "DONE"
            job["result"] = result
            job["done"].set()
            return True

    def fail(self, job_id):
        with self._lock:
            job = self._jobs.get(job_id)
            if job is not None:
                job["state"] = "FAILED"
                job["done"].set()

    def state(self, job_id):
        with self._lock:
            job = self._jobs.get(job_id)
            return job["state"] if job else None

    def result(self, job_id):
        with self._lock:
            job = self._jobs.get(job_id)
            return job["result"] if job else None

    def wait_done(self, job_id, timeout):
        with self._lock:
            job = self._jobs.get(job_id)
        if job is None:
            return False
        return job["done"].wait(timeout)

    def remove(self, job_id):
        with self._lock:
            self._jobs.pop(job_id, None)