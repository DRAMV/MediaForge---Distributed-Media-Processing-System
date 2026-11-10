"""

USAGE
-----
Create ONE clock per process, next to where you set up logging:

    from common.clock import LamportClock
    clock = LamportClock(owner="manager")

Then at the message touch points:

    # manager dispatching a job to a worker
    job.lamport = clock.send()

    # worker receiving that job
    clock.recv(job.lamport)
    ack.lamport = clock.send()

    # manager receiving a result report
    clock.recv(report.lamport)

    # anywhere: read the current value (never advances it)
    print(f"handled at lamport={clock.value}")

Rule of thumb: tick/send/recv advance the clock; .value only reads it.
"""

import threading


class LamportClock:
    """A single, thread-safe Lamport logical clock."""

    def __init__(self, owner=""):
        """owner: a name for debugging, e.g. "manager" or "worker-1"."""
        self.owner = owner
        self._value = 0
        self._lock = threading.Lock()

    def tick(self) -> int:
        """A local event just happened: advance by 1, return new value."""
        with self._lock:
            self._value += 1
            return self._value

    def send(self) -> int:
        """About to send a message: advance by 1 and take the stamp
        that goes INTO the message's `lamport` field."""
        return self.tick()

    def recv(self, remote: int) -> int:
        """A message arrived carrying stamp `remote`: merge it and
        advance. Returns the new local value."""
        with self._lock:
            self._value = max(self._value, remote) + 1
            return self._value

    @property
    def value(self) -> int:
        """Current value WITHOUT advancing it. Safe to read anywhere."""
        with self._lock:
            return self._value

    def __repr__(self):
        return f"LamportClock({self.owner}={self._value})"