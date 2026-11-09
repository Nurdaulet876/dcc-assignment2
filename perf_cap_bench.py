import argparse
import json
import math
import os
import socket
import statistics
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

import grpc  # noqa: E402
import client as kv  # noqa: E402
import kvstore_pb2 as pb  # noqa: E402

ITERATIONS = 1000
WARMUP = 50
KEY = "bench:key"
MAX_STALENESS_MS = 5000
NAMES = ["A", "B", "C"]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_cluster(rtt_ms=0):
    ports = {n: free_port() for n in NAMES}
    procs = {}
    for n in NAMES:
        peers = ",".join(f"{d}=127.0.0.1:{ports[d]}" for d in NAMES if d != n)
        cmd = [sys.executable, os.path.join(ROOT, "server.py"),
               "--name", n, "--port", str(ports[n]), "--peers", peers,
               "--leaseholder", "A", "--heartbeat-ms", "100",
               "--sync-interval-ms", "0", "--peer-rtt-ms", str(rtt_ms), "--quiet"]
        procs[n] = subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL)
    addrs = {n: f"127.0.0.1:{ports[n]}" for n in NAMES}
    for a in addrs.values():
        ch = grpc.insecure_channel(a)
        grpc.channel_ready_future(ch).result(timeout=20)
        ch.close()
    # preload one key through the leaseholder, let heartbeats reach the followers
    ch = kv.make_channel(addrs["A"])
    r = kv.put(kv.make_stub(ch), KEY, "x" * 16, pb.QUORUM_CP)
    assert r.success and r.acks_received == 3, r
    ch.close()
    time.sleep(0.5)
    return procs, addrs


def stop_cluster(procs):
    for p in procs.values():
        if p.poll() is None:
            p.kill()
        p.wait()


def percentile95(values):
    s = sorted(values)
    return s[math.ceil(0.95 * len(s)) - 1]


def measure(stub, consistency, max_staleness_ms=0):
    """ITERATIONS sequential reads -> median / p95 of successful reads + availability."""
    for _ in range(WARMUP):
        try:
            kv.get(stub, KEY, consistency, max_staleness_ms)
        except (kv.ReplicaUnavailableError, kv.StaleReadExceededError):
            pass
    lat, ok = [], 0
    for _ in range(ITERATIONS):
        t0 = time.perf_counter()
        try:
            r = kv.get(stub, KEY, consistency, max_staleness_ms)
            assert r.found
        except (kv.ReplicaUnavailableError, kv.StaleReadExceededError):
            continue
        lat.append((time.perf_counter() - t0) * 1000.0)
        ok += 1
    return {"median_ms": statistics.median(lat) if lat else None,
            "p95_ms": percentile95(lat) if lat else None,
            "availability_pct": 100.0 * ok / ITERATIONS, "ok": ok}


def both_modes(stub):
    return {"linearizable": measure(stub, pb.LINEARIZABLE),
            "follower_stale": measure(stub, pb.FOLLOWER_STALE, MAX_STALENESS_MS)}


def one_run(wan_rtt_ms, skip_extras):
    out = {}
    procs, addrs = start_cluster(0)
    ch = kv.make_channel(addrs["B"])  # client is co-located with follower B
    stub = kv.make_stub(ch)
    try:
        out["healthy"] = both_modes(stub)
        if not skip_extras:
            procs["A"].kill()  # leaseholder crashes
            procs["A"].wait()
            time.sleep(0.3)
            out["leaseholder_down_0s"] = both_modes(stub)
            time.sleep(6.0)  # lag now exceeds the 5 s bound
            out["leaseholder_down_6s"] = both_modes(stub)
    finally:
        ch.close()
        stop_cluster(procs)
    if not skip_extras and wan_rtt_ms:
        procs, addrs = start_cluster(wan_rtt_ms)
        ch = kv.make_channel(addrs["B"])
        try:
            out["healthy_wan"] = both_modes(kv.make_stub(ch))
        finally:
            ch.close()
            stop_cluster(procs)
    return out


def agg(runs, phase, mode, field):
    vals = [r[phase][mode][field] for r in runs if r[phase][mode][field] is not None]
    if not vals:
        return None, None
    return statistics.mean(vals), (statistics.stdev(vals) if len(vals) > 1 else 0.0)


def fmt(m, s, digits=2):
    if m is None:
        return "n/a"
    return f"{m:.{digits}f}" + (f" ± {s:.{digits}f}" if s else "")


def table(runs, phase, title):
    print(f"\n{title}")
    print("| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | Read Availability (%) |")
    print("|---|---|---|---|---|")
    for mode, label, target in (("linearizable", "Linearizable", "Leaseholder (Remote)"),
                                ("follower_stale", "Follower Stale", "Local Follower")):
        med = agg(runs, phase, mode, "median_ms")
        p95 = agg(runs, phase, mode, "p95_ms")
        av = agg(runs, phase, mode, "availability_pct")
        print(f"| {label} | {target} | {fmt(*med)} | {fmt(*p95)} | {fmt(*av, digits=1)} |")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=1, help="repeat everything, report mean ± stdev")
    ap.add_argument("--wan-rtt-ms", type=int, default=10)
    ap.add_argument("--skip-extras", action="store_true",
                    help="only the table required by the lab (no crash / WAN phases)")
    ap.add_argument("--out", default=os.path.join(ROOT, "results", "bench_results.json"))
    args = ap.parse_args()

    runs = []
    for i in range(args.runs):
        print(f"run {i + 1}/{args.runs} ...", flush=True)
        runs.append(one_run(args.wan_rtt_ms, args.skip_extras))

    table(runs, "healthy", f"Required table: 3 replicas on localhost, {ITERATIONS} reads per scenario "
                           f"({args.runs} run(s), mean ± stdev)")
    if not args.skip_extras:
        table(runs, "healthy_wan", f"Supplement: simulated {args.wan_rtt_ms} ms inter-node RTT")
        table(runs, "leaseholder_down_0s", "Supplement: leaseholder crashed (0.3 s ago)")
        table(runs, "leaseholder_down_6s", "Supplement: leaseholder crashed 6 s ago (> max_staleness_ms)")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump({"iterations": ITERATIONS, "warmup": WARMUP, "runs": runs,
                   "wan_rtt_ms": args.wan_rtt_ms}, f, indent=2)
    print(f"\nraw results -> {args.out}")


if __name__ == "__main__":
    main()