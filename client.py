import argparse
import sys
import time
import uuid
from concurrent import futures
from dataclasses import dataclass

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


@dataclass
class QuorumResult:
    committed: bool
    acks: int
    total: int
    value: int
    duplicate: bool


def quorum_increment(stubs, log, counter_id, delta, key=None):
    """Send the SAME idempotency key to all replicas in parallel;
    commit iff a majority acknowledges. Never raises on replica failure."""
    key = key or str(uuid.uuid4())
    majority = len(stubs) // 2 + 1

    def one(stub):
        try:
            return increment(stub, log, counter_id, delta, key)
        except grpc.RpcError:
            return None

    with futures.ThreadPoolExecutor(max_workers=len(stubs)) as pool:
        replies = list(pool.map(one, stubs))

    good = [r for r in replies if r is not None]
    return QuorumResult(
        committed=len(good) >= majority,
        acks=len(good),
        total=len(stubs),
        value=good[0].new_value if good else 0,
        duplicate=bool(good) and all(r.was_duplicate for r in good),
    )


def get_any(stubs, log, counter_id):
    """Read from a single replica (first one that answers). Risk: may be stale."""
    last = None
    for stub in stubs:
        try:
            return get(stub, log, counter_id)
        except grpc.RpcError as e:
            last = e
    raise last


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--addr", default="localhost:50051", help="single replica")
    p.add_argument("--addrs", default=None,
                   help="comma-separated replicas -> quorum mode")
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
    addrs = args.addrs.split(",") if args.addrs else [args.addr]
    channels = [grpc.insecure_channel(a) for a in addrs]
    stubs = [counter_pb2_grpc.CounterStub(c) for c in channels]
    quorum = args.addrs is not None
    exit_code = 0
    try:
        for _ in range(args.times):
            if args.cmd == "incr" and quorum:
                r = quorum_increment(stubs, log, args.counter_id, args.by, args.key)
                if r.committed:
                    print(f"OK committed value={r.value} "
                          f"(replicas acked: {r.acks}/{r.total}, "
                          f"duplicate: {'yes' if r.duplicate else 'no'})")
                else:
                    print(f"FAILED not committed (replicas acked: {r.acks}/{r.total})")
                    exit_code = 1
            elif args.cmd == "incr":
                r = increment(stubs[0], log, args.counter_id, args.by, args.key)
                print(f"OK value={r.new_value} (duplicate: {'yes' if r.was_duplicate else 'no'})")
            else:
                r = get_any(stubs, log, args.counter_id)
                print(f"value={r.value}" if r.found else "not found")
    finally:
        for c in channels:
            c.close()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()