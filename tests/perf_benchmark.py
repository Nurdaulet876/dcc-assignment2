import argparse
import math
import os
import socket
import statistics
import subprocess
import sys
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import grpc
import counter_pb2_grpc
from clocks import LamportClock, EventLog
from client import increment, quorum_increment

TOTAL = 2000
WARMUP = 20


def free_port():
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


def start_replicas(n):
    procs, addrs = [], []
    for i in range(n):
        port = free_port()
        procs.append(subprocess.Popen(
            [sys.executable, os.path.join(ROOT, "server.py"), "--port", str(port),
             "--name", f"bench-{i}", "--quiet"],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        addrs.append(f"localhost:{port}")
    for a in addrs:
        ch = grpc.insecure_channel(a)
        grpc.channel_ready_future(ch).result(timeout=15)
        ch.close()
    return procs, addrs


def percentile95(latencies):
    s = sorted(latencies)
    idx = math.ceil(0.95 * len(s))
    return s[idx - 1]


def run_once(addrs, quorum, clients):
    per_client = TOTAL // clients
    barrier = threading.Barrier(clients + 1)
    results = [None] * clients

    def worker(i):
        channels = [grpc.insecure_channel(a) for a in addrs]
        stubs = [counter_pb2_grpc.CounterStub(c) for c in channels]
        log = EventLog(f"bench-client-{i}", LamportClock(), verbose=False)
        for _ in range(WARMUP):
            quorum_increment(stubs, log, "bench", 1) if quorum \
                else increment(stubs[0], log, "bench", 1)
        barrier.wait()
        lat = []
        for _ in range(per_client):
            t0 = time.perf_counter()
            if quorum:
                r = quorum_increment(stubs, log, "bench", 1)
                assert r.committed
            else:
                increment(stubs[0], log, "bench", 1)
            lat.append((time.perf_counter() - t0) * 1000.0)  # ms
        results[i] = lat
        for c in channels:
            c.close()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(clients)]
    for t in threads:
        t.start()
    barrier.wait()
    t_start = time.perf_counter()
    for t in threads:
        t.join()
    wall = time.perf_counter() - t_start
    all_lat = [x for lat in results for x in lat]
    return statistics.median(all_lat), percentile95(all_lat), len(all_lat), len(all_lat) / wall


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=1, help="repeat each config for variance")
    args = ap.parse_args()

    configs = [
        ("Single replica, 1 client", 1, False, 1),
        ("Single replica, 16 clients", 1, False, 16),
        ("Quorum (3 replicas), 1 client", 3, True, 1),
        ("Quorum (3 replicas), 16 clients", 3, True, 16),
    ]
    rows = []
    for label, n_replicas, quorum, clients in configs:
        meds, p95s, thr = [], [], []
        for _ in range(args.runs):
            procs, addrs = start_replicas(n_replicas)
            try:
                med, p95, n, tput = run_once(addrs, quorum, clients)
            finally:
                for p in procs:
                    p.kill()
                    p.wait()
            meds.append(med)
            p95s.append(p95)
            thr.append(tput)
        rows.append((label, meds, p95s, n, thr))

    def fmt(vals):
        m = statistics.mean(vals)
        return f"{m:.2f}" + (f" ± {statistics.stdev(vals):.2f}" if len(vals) > 1 else "")

    print("\n| Configuration | Median latency (ms) | p95 latency (ms) | Requests |")
    print("|---|---|---|---|")
    for label, meds, p95s, n, _ in rows:
        print(f"| {label} | {fmt(meds)} | {fmt(p95s)} | {n} |")
    print("\nThroughput (req/s):")
    for label, _, _, _, thr in rows:
        print(f"  {label}: {fmt(thr)}")


if __name__ == "__main__":
    main()