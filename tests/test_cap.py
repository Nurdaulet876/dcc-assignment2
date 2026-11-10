import json
import os
import socket
import sys
import threading
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import client as kv  # noqa: E402
import kvstore_pb2 as pb  # noqa: E402
from server import start_server  # noqa: E402

NAMES = ["A", "B", "C"]
N = 3
OBS = {}  # observations, dumped for the lab report


# --------------------------------------------------------------------------
# Failure-injection wrapper
# --------------------------------------------------------------------------
class LinkProxy:
    """One directed network link (src -> dst) that can be cut and restored."""

    def __init__(self, target_port):
        self.target = ("127.0.0.1", target_port)
        self.blocked = False
        self._conns = set()
        self._lock = threading.Lock()
        self._closed = False
        self._srv = socket.socket()
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", 0))
        self._srv.listen(64)
        self.port = self._srv.getsockname()[1]
        threading.Thread(target=self._accept_loop, daemon=True).start()

    def _accept_loop(self):
        while not self._closed:
            try:
                c, _ = self._srv.accept()
            except OSError:
                return
            if self.blocked:
                c.close()
                continue
            try:
                u = socket.create_connection(self.target, timeout=2)
            except OSError:
                c.close()
                continue
            for s in (c, u):
                s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            with self._lock:
                self._conns.update((c, u))
            threading.Thread(target=self._pump, args=(c, u), daemon=True).start()
            threading.Thread(target=self._pump, args=(u, c), daemon=True).start()

    def _pump(self, src, dst):
        try:
            while True:
                data = src.recv(65536)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        finally:
            for s in (src, dst):
                try:
                    s.close()
                except OSError:
                    pass
            with self._lock:
                self._conns.discard(src)
                self._conns.discard(dst)

    def set_blocked(self, blocked):
        self.blocked = blocked
        if blocked:
            with self._lock:
                conns = list(self._conns)
            for s in conns:
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    s.close()
                except OSError:
                    pass

    def close(self):
        self._closed = True
        self.set_blocked(True)
        try:
            self._srv.close()
        except OSError:
            pass


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Cluster:
    """3 in-process replicas wired through partitionable links."""

    def __init__(self, **server_kwargs):
        self.ports = {n: free_port() for n in NAMES}
        self.links = {(s, d): LinkProxy(self.ports[d])
                      for s in NAMES for d in NAMES if s != d}
        self.servers, self.servicers = {}, {}
        for n in NAMES:
            peers = {d: f"127.0.0.1:{self.links[(n, d)].port}" for d in NAMES if d != n}
            srv, _, svc = start_server(self.ports[n], n, peers, "A", **server_kwargs)
            self.servers[n], self.servicers[n] = srv, svc
        self.channels = {n: kv.make_channel(f"127.0.0.1:{self.ports[n]}") for n in NAMES}
        self.stub = {n: kv.make_stub(c) for n, c in self.channels.items()}
        self.stubs = [self.stub[n] for n in NAMES]

    # -- partition control ------------------------------------------------
    def block(self, a, b, one_way=False):
        """Cut a->b (and b->a unless one_way)."""
        self.links[(a, b)].set_blocked(True)
        if not one_way:
            self.links[(b, a)].set_blocked(True)

    def partition(self, group1, group2):
        for a in group1:
            for b in group2:
                self.block(a, b)

    def heal(self):
        for l in self.links.values():
            l.set_blocked(False)

    def crash(self, name):
        self.servers[name].stop(None)
        self.servicers[name].close()

    def close(self):
        for n in NAMES:
            self.servers[n].stop(None)
            self.servicers[n].close()
        for c in self.channels.values():
            c.close()
        for l in self.links.values():
            l.close()


@pytest.fixture
def cluster():
    c = Cluster()
    yield c
    c.close()


@pytest.fixture(scope="module", autouse=True)
def dump_observations():
    yield
    os.makedirs(os.path.join(ROOT, "../results"), exist_ok=True)
    with open(os.path.join(ROOT, "../results", "test_observations.json"), "w") as f:
        json.dump(OBS, f, indent=2)


def eventually(fn, timeout=8.0, interval=0.1):
    """Poll until fn() is truthy (partition heals take ~0.5 s to be noticed)."""
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            last = fn()
            if last:
                return last
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(interval)
    raise AssertionError(f"condition not reached in {timeout}s (last={last!r})")


def local(c, node, key):
    """Read the node's local state machine (no proxying, no freshness bound)."""
    return kv.get(c.stub[node], key, pb.FOLLOWER_STALE, 0)


# --------------------------------------------------------------------------
# Task 3, Part A -- CP mode (strict quorum)
# --------------------------------------------------------------------------
def test_cp_put_on_majority_side_succeeds(cluster):
    cluster.partition(["A", "B"], ["C"])
    r = kv.put(cluster.stub["A"], "k", "v1", pb.QUORUM_CP)
    assert r.success and r.acks_received == 2
    assert local(cluster, "A", "k").value == "v1"
    assert local(cluster, "B", "k").value == "v1"
    assert not local(cluster, "C", "k").found  # minority replica missed it
    OBS["cp_dc1"] = {"success": r.success, "acks": r.acks_received}


def test_cp_put_sent_to_follower_in_majority_is_forwarded(cluster):
    cluster.partition(["A", "B"], ["C"])
    r = kv.put(cluster.stub["B"], "k", "v1", pb.QUORUM_CP)  # B is not the leaseholder
    assert r.success and r.acks_received == 2
    assert local(cluster, "A", "k").value == "v1"


def test_cp_put_on_minority_side_fails_without_dirty_write(cluster):
    cluster.partition(["A", "B"], ["C"])
    r = kv.put(cluster.stub["C"], "k", "v-minority", pb.QUORUM_CP)
    assert not r.success
    assert "ReplicaUnavailableError" in r.error_message
    with pytest.raises(kv.ReplicaUnavailableError):
        kv.put(cluster.stub["C"], "k", "v-minority", pb.QUORUM_CP, raise_on_failure=True)
    for n in NAMES:  # nothing applied anywhere
        assert not local(cluster, n, "k").found
    OBS["cp_dc2"] = {"success": r.success, "acks": r.acks_received, "error": r.error_message}


def test_cp_write_quorum_w3_fails_and_leaves_no_dirty_write(cluster):
    cluster.block("A", "C")  # only A,B reachable from the leaseholder
    r = kv.put(cluster.stub["A"], "k", "v", pb.QUORUM_CP, write_quorum=3)
    assert not r.success and r.acks_received == 2
    assert not local(cluster, "A", "k").found  # PREPARE promised, never COMMITted
    assert not local(cluster, "B", "k").found


# --------------------------------------------------------------------------
# Task 3, Part B -- AP mode (sloppy quorum / LWW)
# --------------------------------------------------------------------------
def test_ap_put_on_minority_side_is_accepted(cluster):
    cluster.partition(["A", "B"], ["C"])
    r = kv.put(cluster.stub["C"], "k", "v-dc2", pb.ALWAYS_AP)
    assert r.success and r.acks_received == 1
    assert local(cluster, "C", "k").value == "v-dc2"
    assert not local(cluster, "A", "k").found  # diverged from DC-1
    OBS["ap_dc2"] = {"success": r.success, "acks": r.acks_received}


@pytest.mark.parametrize("cp_ts,ap_ts,winner", [
    (1_000, 2_000, "from-dc2-AP"),  # AP write is newer  -> it wins
    (2_000, 1_000, "from-dc1-CP"),  # CP write is newer  -> it wins
])
def test_partition_heal_converges_by_last_write_wins(cluster, cp_ts, ap_ts, winner):
    cluster.partition(["A", "B"], ["C"])
    assert kv.put(cluster.stub["A"], "k", "from-dc1-CP", pb.QUORUM_CP, timestamp=cp_ts).success
    assert kv.put(cluster.stub["C"], "k", "from-dc2-AP", pb.ALWAYS_AP, timestamp=ap_ts).success
    # divergence while partitioned
    assert local(cluster, "A", "k").value == "from-dc1-CP"
    assert local(cluster, "C", "k").value == "from-dc2-AP"

    cluster.heal()

    def converged():
        for n in NAMES:
            kv.sync(cluster.stub[n])  # one anti-entropy round
        return all(local(cluster, n, "k").value == winner for n in NAMES)

    eventually(converged)
    OBS[f"lww_cp{cp_ts}_ap{ap_ts}"] = {
        "before_heal": {"A": "from-dc1-CP", "B": "from-dc1-CP", "C": "from-dc2-AP"},
        "after_heal": {n: local(cluster, n, "k").value for n in NAMES}, "winner": winner}


def test_lww_tie_is_broken_deterministically_by_origin(cluster):
    cluster.partition(["A", "B"], ["C"])
    kv.put(cluster.stub["A"], "k", "from-A", pb.QUORUM_CP, timestamp=5_000)
    kv.put(cluster.stub["C"], "k", "from-C", pb.ALWAYS_AP, timestamp=5_000)
    cluster.heal()

    def converged():
        for n in NAMES:
            kv.sync(cluster.stub[n])
        return {local(cluster, n, "k").value for n in NAMES} == {"from-C"}  # "C" > "A"

    eventually(converged)


def test_background_anti_entropy_heals_without_explicit_sync():
    c = Cluster(sync_interval_ms=200)
    try:
        c.partition(["A", "B"], ["C"])
        assert kv.put(c.stub["C"], "x", "late", pb.ALWAYS_AP).success
        assert not local(c, "A", "x").found
        c.heal()
        eventually(lambda: all(local(c, n, "x").value == "late" for n in NAMES), timeout=10)
    finally:
        c.close()


# --------------------------------------------------------------------------
# Quorum arithmetic: R + W > N  (N = 3, all 9 combinations)
# --------------------------------------------------------------------------
def run_quorum_experiment(c, w, r, key="q"):
    """Write with W acks, read with R replicas picked adversarially.

    1. every replica holds "old" (written with W=N);
    2. links from the leaseholder to the last N-W replicas are cut, so the
       "new" write reaches exactly W replicas {A, B, C}[:W];
    3. the reader asks the R replicas that are LEAST likely to have the write
       (reverse order C, B, A) and keeps the reply with the newest timestamp.
    """
    assert kv.put(c.stub["A"], key, "old", pb.QUORUM_CP, timestamp=1_000, write_quorum=3).acks_received == 3
    for victim in NAMES[w:]:
        c.block("A", victim, one_way=True)
    wr = kv.put(c.stub["A"], key, "new", pb.QUORUM_CP, timestamp=2_000, write_quorum=w)
    assert wr.success and wr.acks_received == w
    has_new = [n for n in NAMES if local(c, n, key).value == "new"]
    order = [2, 1, 0]  # C, B, A  (adversarial)
    reply, used = kv.quorum_get(c.stubs, key, r, order=order)
    return {"W": w, "R": r, "R+W": r + w, "intersects": r + w > N,
            "replicas_with_new": has_new, "read_set": [NAMES[i] for i in used],
            "read_value": reply.value, "fresh": reply.value == "new"}


@pytest.mark.parametrize("w", [1, 2, 3])
@pytest.mark.parametrize("r", [1, 2, 3])
def test_read_write_quorum_intersection(cluster, w, r):
    obs = run_quorum_experiment(cluster, w, r)
    OBS.setdefault("quorum_matrix", []).append(obs)
    # R + W > N  <=>  every read set contains a replica that has the latest write
    assert obs["fresh"] == (r + w > N), obs
    assert set(obs["read_set"]) & set(obs["replicas_with_new"]) or not obs["fresh"]


# --------------------------------------------------------------------------
# PACELC: leaseholder reads vs bounded-staleness follower reads
# --------------------------------------------------------------------------
def test_linearizable_read_through_follower_is_proxied_to_leaseholder(cluster):
    cluster.block("A", "B", one_way=True)  # A cannot replicate to B
    assert kv.put(cluster.stub["A"], "k", "new", pb.QUORUM_CP).acks_received == 2
    stale = kv.get(cluster.stub["B"], "k", pb.FOLLOWER_STALE)
    assert not stale.found  # follower B is behind...
    lin = kv.get(cluster.stub["B"], "k", pb.LINEARIZABLE)
    assert lin.found and lin.value == "new" and not lin.is_stale  # ...but LINEARIZABLE is not
    OBS["linearizable_proxy"] = {"follower_stale_read_found": stale.found, "linearizable_value": lin.value}


def test_linearizable_unavailable_but_follower_read_still_served(cluster):
    assert kv.put(cluster.stub["A"], "k", "v", pb.QUORUM_CP).success
    cluster.block("B", "A", one_way=True)  # B cannot reach the leaseholder
    with pytest.raises(kv.ReplicaUnavailableError):
        kv.get(cluster.stub["B"], "k", pb.LINEARIZABLE)
    r = kv.get(cluster.stub["B"], "k", pb.FOLLOWER_STALE, max_staleness_ms=5000)
    assert r.found and r.value == "v"
    OBS["pacelc_availability"] = {"linearizable": "ReplicaUnavailableError",
                                  "follower_stale": "served"}


def test_follower_read_fails_with_stale_read_exceeded_then_recovers():
    c = Cluster(heartbeat_ms=50)
    try:
        assert kv.put(c.stub["A"], "k", "v", pb.QUORUM_CP).success
        eventually(lambda: kv.get(c.stub["C"], "k", pb.FOLLOWER_STALE, max_staleness_ms=300))
        c.block("A", "C")  # heartbeats to C stop
        time.sleep(0.7)
        with pytest.raises(kv.StaleReadExceededError, match="STALE_READ_EXCEEDED"):
            kv.get(c.stub["C"], "k", pb.FOLLOWER_STALE, max_staleness_ms=300)
        unbounded = kv.get(c.stub["C"], "k", pb.FOLLOWER_STALE, max_staleness_ms=0)
        assert unbounded.found and unbounded.is_stale  # still readable, flagged stale
        c.heal()
        eventually(lambda: kv.get(c.stub["C"], "k", pb.FOLLOWER_STALE, max_staleness_ms=300))
        OBS["stale_read_exceeded"] = {"raised": True, "recovered_after_heal": True}
    finally:
        c.close()


# --------------------------------------------------------------------------
# Misc correctness
# --------------------------------------------------------------------------
def test_idempotent_put_is_not_reapplied(cluster):
    r1 = kv.put(cluster.stub["A"], "k", "first", pb.QUORUM_CP, idempotency_key="id-1")
    r2 = kv.put(cluster.stub["A"], "k", "second", pb.QUORUM_CP, idempotency_key="id-1")
    assert r1.success and r2.success and "duplicate" in r2.error_message
    for n in NAMES:
        assert local(cluster, n, "k").value == "first"


def test_get_missing_key(cluster):
    r = kv.get(cluster.stub["A"], "nope", pb.LINEARIZABLE)
    assert not r.found and r.value == ""


def test_healthy_cluster_replicates_to_all_and_reads_are_linearizable(cluster):
    for i in range(20):
        assert kv.put(cluster.stub["A"], f"key{i}", f"val{i}", pb.QUORUM_CP).acks_received == 3
    for i in range(20):
        for n in NAMES:
            assert local(cluster, n, f"key{i}").value == f"val{i}"
            assert kv.get(cluster.stub[n], f"key{i}", pb.LINEARIZABLE).value == f"val{i}"
