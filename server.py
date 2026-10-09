import argparse
import threading
import time
from concurrent import futures

import grpc
import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock, EventLog


class CounterServicer(counter_pb2_grpc.CounterServicer):
    def __init__(self, name="replica-A", delay_ms=0):
        self._lock = threading.Lock()
        self._values = {}   # counter_id -> int
        self._seen = {}     # idempotency_key -> (counter_id, resulting value)
        self._delay_ms = delay_ms
        self.clock = LamportClock()
        self.log = EventLog(name, self.clock)

    def Increment(self, request, context):
        self.log.recv_event(
            f"RECV Increment(counter={request.counter_id}, delta={request.delta})",
            request.lamport_time)

        if self._delay_ms:
            time.sleep(self._delay_ms / 1000)

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


def start_server(port=0, delay_ms=0, name="replica-A"):
    """Start the server and return (server, actual_port). port=0 -> ephemeral port."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    counter_pb2_grpc.add_CounterServicer_to_server(
        CounterServicer(name, delay_ms), server)
    actual_port = server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server, actual_port


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=50051)
    p.add_argument("--delay-ms", type=int, default=0)
    p.add_argument("--name", default="replica-A")
    args = p.parse_args()
    srv, port = start_server(args.port, args.delay_ms, args.name)
    print(f"[{args.name}] listening on {port}", flush=True)
    srv.wait_for_termination()