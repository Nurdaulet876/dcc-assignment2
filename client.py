"""Client for the replicated KV store: Get/Put helpers, retries, quorum reads, CLI."""
import argparse
import sys
import time
import uuid

import grpc

import kvstore_pb2 as pb
import kvstore_pb2_grpc as pbg

RETRYABLE = (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
BACKOFFS = [0.1, 0.2, 0.4]
GET_TIMEOUT = 2.0
PUT_TIMEOUT = 6.0

CHANNEL_OPTIONS = [
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 500),
]


class ReplicaUnavailableError(Exception):
    """Replica / leaseholder unreachable, or write quorum lost."""


class StaleReadExceededError(Exception):
    """Follower is further behind the leaseholder than max_staleness_ms."""


def make_channel(addr):
    return grpc.insecure_channel(addr, options=CHANNEL_OPTIONS)


def make_stub(channel):
    return pbg.KeyValueStoreStub(channel)


def _translate(e):
    details = e.details() or ""
    if e.code() == grpc.StatusCode.FAILED_PRECONDITION and details.startswith("STALE_READ_EXCEEDED"):
        return StaleReadExceededError(details)
    if e.code() in RETRYABLE:
        return ReplicaUnavailableError(details or e.code().name)
    return e


def get(stub, key, consistency=pb.LINEARIZABLE, max_staleness_ms=0, timeout=GET_TIMEOUT):
    """One read. Raises ReplicaUnavailableError / StaleReadExceededError."""
    req = pb.GetRequest(key=key, consistency=consistency, max_staleness_ms=max_staleness_ms)
    try:
        return stub.Get(req, timeout=timeout)
    except grpc.RpcError as e:
        raise _translate(e) from e


def put(stub, key, value, strategy=pb.QUORUM_CP, timestamp=0, idempotency_key=None,
        write_quorum=0, raise_on_failure=False, timeout=PUT_TIMEOUT):
    """One write. The SAME idempotency key is used for every retry of a transport error.

    A reply with success=False (quorum lost) is returned as-is unless
    raise_on_failure=True, in which case ReplicaUnavailableError is raised.
    """
    req = pb.PutRequest(key=key, value=value, strategy=strategy, timestamp=timestamp,
                        idempotency_key=idempotency_key or str(uuid.uuid4()),
                        write_quorum=write_quorum)
    for attempt in range(len(BACKOFFS) + 1):
        try:
            reply = stub.Put(req, timeout=timeout)
            break
        except grpc.RpcError as e:
            if e.code() in RETRYABLE and attempt < len(BACKOFFS):
                time.sleep(BACKOFFS[attempt])
                continue
            raise _translate(e) from e
    if not reply.success and raise_on_failure:
        raise ReplicaUnavailableError(reply.error_message)
    return reply


def sync(stub, timeout=3.0):
    """Ask a replica to run one anti-entropy round now."""
    return stub.Sync(pb.SyncRequest(), timeout=timeout)


def quorum_get(stubs, key, r, order=None):
    """Read quorum of size R: read the LOCAL state of R replicas (in `order`),
    return (newest reply by version timestamp, indices of replicas used).
    Raises ReplicaUnavailableError if fewer than R replicas answer."""
    used, best = [], None
    for i in (order if order is not None else range(len(stubs))):
        if len(used) == r:
            break
        try:
            rep = get(stubs[i], key, pb.FOLLOWER_STALE, 0)
        except (ReplicaUnavailableError, StaleReadExceededError):
            continue
        used.append(i)
        if best is None or rep.read_timestamp > best.read_timestamp:
            best = rep
    if len(used) < r:
        raise ReplicaUnavailableError(f"read quorum not reached: {len(used)}/{r} replicas answered")
    return best, used


def main():
    p = argparse.ArgumentParser(description="KV store client")
    p.add_argument("--addr", default="localhost:50051", help="replica to talk to")
    sub = p.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("get")
    g.add_argument("key")
    g.add_argument("--consistency", choices=["lin", "stale"], default="lin")
    g.add_argument("--max-staleness-ms", type=int, default=5000)
    w = sub.add_parser("put")
    w.add_argument("key")
    w.add_argument("value")
    w.add_argument("--strategy", choices=["cp", "ap"], default="cp")
    w.add_argument("--w", type=int, default=0, help="write quorum (0 = majority)")
    w.add_argument("--timestamp", type=int, default=0)
    sub.add_parser("sync")
    args = p.parse_args()

    ch = make_channel(args.addr)
    stub = make_stub(ch)
    code = 0
    try:
        if args.cmd == "get":
            cons = pb.LINEARIZABLE if args.consistency == "lin" else pb.FOLLOWER_STALE
            r = get(stub, args.key, cons, args.max_staleness_ms)
            print(f"value={r.value!r} ts={r.read_timestamp} is_stale={r.is_stale}"
                  if r.found else "not found")
        elif args.cmd == "put":
            strat = pb.QUORUM_CP if args.strategy == "cp" else pb.ALWAYS_AP
            r = put(stub, args.key, args.value, strat, args.timestamp, write_quorum=args.w)
            if r.success:
                print(f"OK acks={r.acks_received}")
            else:
                print(f"FAILED acks={r.acks_received}: {r.error_message}")
                code = 1
        else:
            r = sync(stub)
            print(f"merged={r.entries_merged} peers_reached={r.peers_reached}")
    except (ReplicaUnavailableError, StaleReadExceededError) as e:
        print(f"ERROR {type(e).__name__}: {e}")
        code = 1
    finally:
        ch.close()
    sys.exit(code)


if __name__ == "__main__":
    main()