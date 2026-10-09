import argparse
import threading
import time
from concurrent import futures

import grpc
import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock, EventLog


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, name="replica-A", delay_ms=0, delay_first=0, verbose=True):
        self._lock = threading.Lock()
        self._values = {}                  # counter_id -> int
        self._seen = {}                    # idempotency_key -> (counter_id, resulting value)
        self._delay_ms = delay_ms
        self._delay_first = delay_first
        self._delay_lock = threading.Lock()
        self._calls = 0
        self.clock = LamportClock()
        self.log = EventLog(name, self.clock, verbose)

    def _maybe_delay(self):
        """Fault injection: simulate a slow replica (outside the main lock)."""
        if not self._delay_ms:
            return
        with self._delay_lock:
            self._calls += 1
            n = self._calls
        if self._delay_first == 0 or n <= self._delay_first:
            time.sleep(self._delay_ms / 1000)

    def Increment(self, request, context):
        self.log.recv_event(
            f"RECV Increment(counter={request.counter_id}, delta={request.delta})",
            request.lamport_time)
        self._maybe_delay()

        with self._lock:
            if request.idempotency_key in self._seen:
                _, value = self._seen[request.idempotency_key]
                duplicate = True
                self.log.local_event(
                    f"DUPLICATE key={request.idempotency_key[:8]} ignored, no APPLY")
            else:
                value = self._values.get(request.counter_id, 0) + request.delta
                self._values[request.counter_id] = value
                self._seen[request.idempotency_key] = (request.counter_id, value)
                duplicate = False
                self.log.local_event(f"APPLY counter={request.counter_id} -> {value}")

        L = self.log.local_event(f"SEND IncrementReply(new_value={value})")
        return counter_pb2.IncrementReply(
            new_value=value, was_duplicate=duplicate, lamport_time=L)

    def Get(self, request, context):
        self.log.recv_event(f"RECV Get(counter={request.counter_id})",
                            request.lamport_time)
        with self._lock:
            found = request.counter_id in self._values
            value = self._values.get(request.counter_id, 0)
        L = self.log.local_event(f"SEND GetReply(value={value}, found={found})")
        return counter_pb2.GetReply(value=value, found=found, lamport_time=L)


def start_server(port=0, delay_ms=0, name="replica-A", delay_first=0, verbose=True):
    """Start a replica and return (server, actual_port). port=0 -> ephemeral port."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(name, delay_ms, delay_first, verbose), server)
    actual_port = server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server, actual_port


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=50051)
    p.add_argument("--name", default="replica-A")
    p.add_argument("--delay-ms", type=int, default=0, help="fault: slow replica")
    p.add_argument("--delay-first", type=int, default=0,
                   help="apply --delay-ms only to the first N requests (0 = all)")
    p.add_argument("--quiet", action="store_true", help="no log output")
    args = p.parse_args()
    srv, port = start_server(args.port, args.delay_ms, args.name,
                             args.delay_first, verbose=not args.quiet)
    if not args.quiet:
        print(f"[{args.name}] listening on {port}", flush=True)
    srv.wait_for_termination()