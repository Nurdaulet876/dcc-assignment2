import argparse
import time
import uuid

import grpc
import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock, EventLog

RETRYABLE = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)
BACKOFFS = [0.2, 0.4, 0.8]
TIMEOUT = 2.0


def increment(stub, log, counter_id, delta, key=None):
    key = key or str(uuid.uuid4())
    request = counter_pb2.IncrementRequest(
        counter_id=counter_id, delta=delta, idempotency_key=key)
    for attempt in range(len(BACKOFFS) + 1):
        request.lamport_time = log.local_event(
            f"SEND Increment(counter={counter_id}, delta={delta})")
        try:
            reply = stub.Increment(request, timeout=TIMEOUT)
        except grpc.RpcError as e:
            if e.code() not in RETRYABLE or attempt == len(BACKOFFS):
                raise
            time.sleep(BACKOFFS[attempt])
            continue
        log.recv_event(f"RECV IncrementReply(new_value={reply.new_value})",
                       reply.lamport_time)
        return reply


def get(stub, log, counter_id):
    request = counter_pb2.GetRequest(counter_id=counter_id)
    request.lamport_time = log.local_event(f"SEND Get(counter={counter_id})")
    reply = stub.Get(request, timeout=TIMEOUT)
    log.recv_event(f"RECV GetReply(value={reply.value}, found={reply.found})",
                   reply.lamport_time)
    return reply


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--addr", default="localhost:50051")
    p.add_argument("--name", default="client-1")
    sub = p.add_subparsers(dest="cmd", required=True)
    i = sub.add_parser("incr")
    i.add_argument("counter_id")
    i.add_argument("--by", type=int, default=1)
    i.add_argument("--key", default=None)
    i.add_argument("--times", type=int, default=1)
    g = sub.add_parser("get")
    g.add_argument("counter_id")
    g.add_argument("--times", type=int, default=1)
    args = p.parse_args()

    clock = LamportClock()
    log = EventLog(args.name, clock)

    with grpc.insecure_channel(args.addr) as channel:
        stub = counter_pb2_grpc.CounterStub(channel)
        for _ in range(args.times):
            if args.cmd == "incr":
                r = increment(stub, log, args.counter_id, args.by, args.key)
                print(f"OK value={r.new_value} (duplicate: {'yes' if r.was_duplicate else 'no'})")
            else:
                r = get(stub, log, args.counter_id)
                print(f"value={r.value}" if r.found else "not found")


if __name__ == "__main__":
    main()