import os
import threading


class LamportClock:
    pass


class EventLog:
    """Ticks the clock and writes the log line under ONE lock,
    so the order of L values in the file matches the order of events."""

    def __init__(self, name, clock):
        self.name = name
        self.clock = clock
        self._lock = threading.Lock()
        os.makedirs("logs", exist_ok=True)
        self._file = open(f"logs/{name}.log", "a", encoding="utf-8")

    def local_event(self, text):
        """SEND / APPLY: tick, log, return the new L (put it into the message)."""
        with self._lock:
            L = self.clock.tick()
            self._write(f"[{self.name}] {text} L={L}")
            return L

    def recv_event(self, text, received):
        """RECV: L = max(own, received) + 1, then log."""
        with self._lock:
            L = self.clock.receive(received)
            self._write(f"[{self.name}] {text} L={L} (received L={received})")
            return L

    def _write(self, line):
        print(line, flush=True)
        self._file.write(line + "\n")
        self._file.flush()