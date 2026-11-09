import argparse
import threading
import time
from concurrent import futures

import grpc
import counter_pb2
import counter_pb2_grpc
from clocks import LamportClock, EventLog

import kvstore_pb2 as pb
import kvstore_pb2_grpc as pbg

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

DEFAULT_PORTS = {"A": 50051, "B": 50052, "C": 50053}
PEER_TIMEOUT = 1.0
HEARTBEAT_TIMEOUT = 0.5
STALE_READ_EXCEEDED = "STALE_READ_EXCEEDED"

# Fast reconnect so that a healed partition is noticed within ~0.5 s.
CHANNEL_OPTIONS = [
    ("grpc.initial_reconnect_backoff_ms", 100),
    ("grpc.min_reconnect_backoff_ms", 100),
    ("grpc.max_reconnect_backoff_ms", 500),
]


def now_us():
    return time.time_ns() // 1000


class KVServicer(pbg.KeyValueStoreServicer):
    def __init__(self, name="A", peers=None, leaseholder="A", heartbeat_ms=100,
                 sync_interval_ms=0, peer_rtt_ms=0, verbose=True):
        self.name = name
        self.leaseholder = leaseholder
        self.is_leaseholder = (name == leaseholder)
        self.verbose = verbose
        self.peers = dict(peers or {})  # peer name -> address
        self.n = len(self.peers) + 1  # N, cluster size
        self._rtt = peer_rtt_ms / 1000.0  # simulated inter-node latency
        self._heartbeat_s = heartbeat_ms / 1000.0
        self._stale_flag_ms = 2 * heartbeat_ms  # lag above this => is_stale=True

        self._channels = {p: grpc.insecure_channel(a, options=CHANNEL_OPTIONS)
                          for p, a in self.peers.items()}
        self._stubs = {p: pbg.KeyValueStoreStub(c) for p, c in self._channels.items()}

        self._lock = threading.Lock()  # protects the fields below
        self._data = {}  # key -> (value, timestamp, origin)
        self._pending = {}  # CP promises (PREPARE, not committed)
        self._seen = {}  # idempotency_key -> acks of first execution
        self._last_ts = 0
        self._write_lock = threading.Lock()  # serialises CP writes at the leaseholder
        self._last_leader_contact = time.monotonic()

        self._stop = threading.Event()
        self._pool = futures.ThreadPoolExecutor(max_workers=max(4, 2 * len(self.peers) + 2))
        if self.is_leaseholder and self.peers and heartbeat_ms > 0:
            threading.Thread(target=self._heartbeat_loop, daemon=True).start()
        if sync_interval_ms > 0 and self.peers:
            threading.Thread(target=self._sync_loop, args=(sync_interval_ms / 1000.0,),
                             daemon=True).start()

    # ------------------------------------------------------------------ utils
    def _log(self, msg):
        if self.verbose:
            print(f"[{self.name}] {msg}", flush=True)

    def close(self):
        self._stop.set()
        self._pool.shutdown(wait=False, cancel_futures=True)
        for c in self._channels.values():
            c.close()

    def _next_ts(self):
        with self._lock:
            self._last_ts = max(now_us(), self._last_ts + 1)
            return self._last_ts

    def _merge(self, key, value, ts, origin):
        """Last-Write-Wins: the version with the larger (timestamp, origin) wins."""
        with self._lock:
            cur = self._data.get(key)
            if cur is None or (ts, origin) > (cur[1], cur[2]):
                self._data[key] = (value, ts, origin)
                return True
            return False

    def _peer(self, peer, method, request, timeout=PEER_TIMEOUT):
        if self._rtt:
            time.sleep(self._rtt)  # simulated WAN round trip
        return getattr(self._stubs[peer], method)(request, timeout=timeout)

    def _fanout(self, peers, method, make_request, timeout=PEER_TIMEOUT):
        """Call `method` on all `peers` in parallel -> {peer: reply or None}."""

        def one(p):
            try:
                return p, self._peer(p, method, make_request(), timeout)
            except grpc.RpcError:
                return p, None

        try:
            return dict(self._pool.map(one, list(peers)))
        except RuntimeError:  # pool already shut down
            return {p: None for p in peers}

    def _rep(self, phase, key="", value="", ts=0, origin="", idem=""):
        return pb.ReplicateRequest(phase=phase, key=key, value=value, timestamp=ts,
                                   origin=origin or self.name, idempotency_key=idem,
                                   sender=self.name)

    def _lag_ms(self):
        if self.is_leaseholder:
            return 0
        return int((time.monotonic() - self._last_leader_contact) * 1000)

    def _note_leader_contact(self):
        self._last_leader_contact = time.monotonic()

    # ------------------------------------------------------------------- Get
    def _local_get(self, key, is_stale):
        with self._lock:
            cur = self._data.get(key)
        if cur is None:
            return pb.GetReply(value="", found=False, read_timestamp=0, is_stale=is_stale)
        return pb.GetReply(value=cur[0], found=True, read_timestamp=cur[1], is_stale=is_stale)

    def Get(self, request, context):
        if request.consistency == pb.LINEARIZABLE:
            if self.is_leaseholder:
                return self._local_get(request.key, is_stale=False)
            # Follower: proxy strictly to the leaseholder.
            try:
                return self._peer(self.leaseholder, "Get", request, timeout=2.0)
            except grpc.RpcError as e:
                context.abort(grpc.StatusCode.UNAVAILABLE,
                              f"ReplicaUnavailableError: leaseholder {self.leaseholder} "
                              f"unreachable from {self.name} ({e.code().name})")

        # FOLLOWER_STALE: serve from the local state machine, bounded by max_staleness_ms.
        lag = self._lag_ms()
        if request.max_staleness_ms and lag > request.max_staleness_ms:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                          f"{STALE_READ_EXCEEDED}: {self.name} last heard from leaseholder "
                          f"{lag} ms ago (max_staleness_ms={request.max_staleness_ms})")
        return self._local_get(request.key, is_stale=lag > self._stale_flag_ms)

    # ------------------------------------------------------------------- Put
    def Put(self, request, context):
        if request.strategy == pb.ALWAYS_AP:
            return self._put_ap(request)
        if not self.is_leaseholder:
            return self._forward_put(request)
        return self._coordinate_cp(request)

    def _forward_put(self, request):
        try:
            return self._peer(self.leaseholder, "Put", request, timeout=4.0)
        except grpc.RpcError as e:
            return pb.PutReply(
                success=False, acks_received=0,
                error_message=f"ReplicaUnavailableError: quorum lost, leaseholder "
                              f"{self.leaseholder} unreachable from {self.name} "
                              f"({e.code().name})")

    def _coordinate_cp(self, req):
        with self._write_lock:
            if req.idempotency_key:
                with self._lock:
                    prior = self._seen.get(req.idempotency_key)
                if prior is not None:
                    return pb.PutReply(success=True, acks_received=prior,
                                       error_message="duplicate idempotency_key: not re-applied")
            need = req.write_quorum or (self.n // 2 + 1)
            if need > self.n:
                return pb.PutReply(success=False, acks_received=0,
                                   error_message=f"invalid write_quorum {need} > N={self.n}")
            ts = req.timestamp or self._next_ts()
            peers = list(self.peers)

            # Phase 1: PREPARE -- collect promises.
            votes = self._fanout(peers, "Replicate", lambda: self._rep(
                pb.PHASE_PREPARE, req.key, req.value, ts, self.name, req.idempotency_key))
            prepared = [p for p, r in votes.items() if r is not None and r.ok]
            if 1 + len(prepared) < need:
                if prepared:
                    self._fanout(prepared, "Replicate", lambda: self._rep(
                        pb.PHASE_ABORT, req.key, req.value, ts, self.name, req.idempotency_key))
                self._log(f"CP put {req.key!r} REJECTED: {1 + len(prepared)}/{self.n} reachable, need {need}")
                return pb.PutReply(
                    success=False, acks_received=1 + len(prepared),
                    error_message=f"ReplicaUnavailableError: quorum lost "
                                  f"({1 + len(prepared)}/{self.n} replicas reachable, need {need})")

            # Phase 2: COMMIT -- apply locally, then on every replica that promised.
            self._merge(req.key, req.value, ts, self.name)
            done = self._fanout(prepared, "Replicate", lambda: self._rep(
                pb.PHASE_COMMIT, req.key, req.value, ts, self.name, req.idempotency_key))
            acks = 1 + sum(1 for r in done.values() if r is not None and r.ok)
            if req.idempotency_key:
                with self._lock:
                    self._seen[req.idempotency_key] = acks
            self._log(f"CP put {req.key!r}={req.value!r} ts={ts} COMMITTED acks={acks}/{self.n}")
            return pb.PutReply(success=True, acks_received=acks, error_message="")

    def _put_ap(self, req):
        if req.idempotency_key:
            with self._lock:
                prior = self._seen.get(req.idempotency_key)
            if prior is not None:
                return pb.PutReply(success=True, acks_received=prior,
                                   error_message="duplicate idempotency_key: not re-applied")
        ts = req.timestamp or self._next_ts()
        self._merge(req.key, req.value, ts, self.name)  # accept immediately
        res = self._fanout(list(self.peers), "Replicate", lambda: self._rep(
            pb.PHASE_APPLY, req.key, req.value, ts, self.name, req.idempotency_key),
                           timeout=0.5)  # best effort
        acks = 1 + sum(1 for r in res.values() if r is not None and r.ok)
        if req.idempotency_key:
            with self._lock:
                self._seen[req.idempotency_key] = acks
        self._log(f"AP put {req.key!r}={req.value!r} ts={ts} accepted acks={acks}/{self.n}")
        return pb.PutReply(success=True, acks_received=acks, error_message="")

    # ------------------------------------------------------- internal RPCs
    def Replicate(self, request, context):
        if request.sender == self.leaseholder and not self.is_leaseholder:
            self._note_leader_contact()
        ph = request.phase
        pend_key = request.idempotency_key or f"{request.key}@{request.timestamp}"
        if ph == pb.PHASE_PREPARE:
            with self._lock:
                self._pending[pend_key] = (request.key, request.value, request.timestamp)
        elif ph == pb.PHASE_COMMIT:
            self._merge(request.key, request.value, request.timestamp, request.origin)
            with self._lock:
                self._pending.pop(pend_key, None)
        elif ph == pb.PHASE_ABORT:
            with self._lock:
                self._pending.pop(pend_key, None)
        elif ph == pb.PHASE_APPLY:
            self._merge(request.key, request.value, request.timestamp, request.origin)
        # PHASE_HEARTBEAT: nothing but the leader-contact bookkeeping above.
        return pb.ReplicateReply(ok=True)

    def Dump(self, request, context):
        with self._lock:
            entries = [pb.Entry(key=k, value=v[0], timestamp=v[1], origin=v[2])
                       for k, v in self._data.items()]
        return pb.DumpReply(entries=entries)

    def sync_now(self):
        """Anti-entropy round: pull every reachable peer's state and merge (LWW)."""
        merged = reached = 0
        for p in self.peers:
            try:
                reply = self._peer(p, "Dump", pb.DumpRequest(), timeout=PEER_TIMEOUT)
            except grpc.RpcError:
                continue
            reached += 1
            if p == self.leaseholder and not self.is_leaseholder:
                self._note_leader_contact()  # we just copied the leaseholder's state
            for e in reply.entries:
                if self._merge(e.key, e.value, e.timestamp, e.origin):
                    merged += 1
        if merged:
            self._log(f"anti-entropy merged {merged} entries")
        return merged, reached

    def Sync(self, request, context):
        merged, reached = self.sync_now()
        return pb.SyncReply(entries_merged=merged, peers_reached=reached)

    # ------------------------------------------------------ background loops
    def _heartbeat_loop(self):
        while not self._stop.wait(self._heartbeat_s):
            self._fanout(list(self.peers), "Replicate",
                         lambda: self._rep(pb.PHASE_HEARTBEAT), timeout=HEARTBEAT_TIMEOUT)

    def _sync_loop(self, interval_s):
        while not self._stop.wait(interval_s):
            try:
                self.sync_now()
            except Exception:  # never kill the loop
                pass


def start_server(port=0, name="A", peers=None, leaseholder="A", heartbeat_ms=100,
                 sync_interval_ms=0, peer_rtt_ms=0, verbose=False):
    """Start a replica; returns (grpc_server, actual_port, servicer). port=0 -> ephemeral."""
    servicer = KVServicer(name, peers, leaseholder, heartbeat_ms, sync_interval_ms,
                          peer_rtt_ms, verbose)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    pbg.add_KeyValueStoreServicer_to_server(servicer, server)
    actual = server.add_insecure_port(f"[::]:{port}")
    server.start()
    return server, actual, servicer


def parse_peers(text):
    out = {}
    for item in filter(None, (text or "").split(",")):
        name, addr = item.split("=", 1)
        out[name.strip()] = addr.strip()
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="KV store replica")
    ap.add_argument("--name", default="A", help="node name (A, B, C)")
    ap.add_argument("--port", type=int, default=None,
                    help="default: 50051/50052/50053 for A/B/C")
    ap.add_argument("--peers", default=None,
                    help="B=host:port,C=host:port (default: the other default nodes on localhost)")
    ap.add_argument("--leaseholder", default="A", help="name of the leaseholder node")
    ap.add_argument("--heartbeat-ms", type=int, default=100)
    ap.add_argument("--sync-interval-ms", type=int, default=1000,
                    help="background anti-entropy period (0 = off)")
    ap.add_argument("--peer-rtt-ms", type=int, default=0,
                    help="simulated inter-node latency added to every peer RPC")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    port = args.port or DEFAULT_PORTS[args.name]
    peers = parse_peers(args.peers) if args.peers is not None else {
        n: f"localhost:{p}" for n, p in DEFAULT_PORTS.items() if n != args.name}
    srv, port, _ = start_server(port, args.name, peers, args.leaseholder,
                                args.heartbeat_ms, args.sync_interval_ms,
                                args.peer_rtt_ms, verbose=not args.quiet)
    if not args.quiet:
        role = "LEASEHOLDER" if args.name == args.leaseholder else "follower"
        print(f"[{args.name}] {role} listening on {port}, peers={peers}", flush=True)
    srv.wait_for_termination()