import argparse
import time
import uuid

import grpc
import counter_pb2
import counter_pb2_grpc

RETRYABLE = (grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.UNAVAILABLE)
BACKOFFS = [0.2, 0.4, 0.8]   # максимум 3 ретрая
TIMEOUT = 2.0


def increment(stub, counter_id, delta, key=None):
    key = key or str(uuid.uuid4())      # ключ создаётся ОДИН раз, до первой попытки
    request = counter_pb2.IncrementRequest(
        counter_id=counter_id, delta=delta, idempotency_key=key)
    for attempt in range(len(BACKOFFS) + 1):
        try:
            return stub.Increment(request, timeout=TIMEOUT)   # тот же request = тот же ключ
        except grpc.RpcError as e:
            if e.code() not in RETRYABLE or attempt == len(BACKOFFS):
                raise
            time.sleep(BACKOFFS[attempt])


def get(stub, counter_id):
    return stub.Get(counter_pb2.GetRequest(counter_id=counter_id), timeout=TIMEOUT)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--addr", default="localhost:50051")
    sub = p.add_subparsers(dest="cmd", required=True)
    i = sub.add_parser("incr")
    i.add_argument("counter_id")
    i.add_argument("--by", type=int, default=1)
    i.add_argument("--key", default=None)
    g = sub.add_parser("get")
    g.add_argument("counter_id")
    args = p.parse_args()

    with grpc.insecure_channel(args.addr) as channel:
        stub = counter_pb2_grpc.CounterStub(channel)
        if args.cmd == "incr":
            r = increment(stub, args.counter_id, args.by, args.key)
            print(f"OK value={r.new_value} (duplicate: {'yes' if r.was_duplicate else 'no'})")
        else:
            r = get(stub, args.counter_id)
            print(f"value={r.value}" if r.found else "not found")


if __name__ == "__main__":
    main()