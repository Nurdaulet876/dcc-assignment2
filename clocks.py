import threading


class LamportClock:
    """Lamport logical clock for a single process."""

    def __init__(self):
        self._time = 0
        self._lock = threading.Lock() 

    def tick(self):
        """Local event (SEND, APPLY): increment by 1 and return the new value."""
        with self._lock:
            self._time += 1
            return self._time

    def receive(self, received):
        """Message receipt (RECV): set clock to max(own, received) + 1."""
        with self._lock:
            self._time = max(self._time, received) + 1
            return self._time

    def now(self):
        """Current value without changing it (for debugging)."""
        with self._lock:
            return self._time