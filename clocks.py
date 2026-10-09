import os
import threading


class LamportClock:
    """Lamport logical clock for a single process."""

    def __init__(self):
        self._time = 0
        self._lock = threading.Lock()  # the server handles requests in multiple threads

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


class EventLog:
    """Ticks the clock and writes the log line under ONE lock.
    verbose=False keeps the clock logic but skips all printing/file I/O
    (used by tests and by the benchmark)."""

    def __init__(self, name, clock, verbose=True):
        self.name = name
        self.clock = clock
        self.verbose = verbose
        self._lock = threading.Lock()
        self._file = None
        if verbose:
            os.makedirs("logs", exist_ok=True)
            self._file = open(f"logs/{name}.log", "a", encoding="utf-8")

    def local_event(self, text):
        with self._lock:
            L = self.clock.tick()
            self._write(f"[{self.name}] {text} L={L}")
            return L

    def recv_event(self, text, received):
        with self._lock:
            L = self.clock.receive(received)
            self._write(f"[{self.name}] {text} L={L} (received L={received})")
            return L

    def _write(self, line):
        if not self.verbose:
            return
        print(line, flush=True)
        self._file.write(line + "\n")
        self._file.flush()