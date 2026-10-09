import os
import socket
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import grpc
import pytest

import counter_pb2_grpc
from clocks import LamportClock, EventLog
from server import start_server


class Replica:
    """An in-process replica plus a ready-to-use client stub."""

    def __init__(self, name="replica-T", delay_ms=0, delay_first=0):
        self.name = name
        self.server, self.port = start_server(
            0, delay_ms, name, delay_first, verbose=False)
        self.channel = grpc.insecure_channel(f"localhost:{self.port}")
        self.stub = counter_pb2_grpc.CounterStub(self.channel)
        self.alive = True

    def stop(self):
        """Simulate a crash: abort in-flight RPCs and close the port."""
        if self.alive:
            self.server.stop(None)
            self.alive = False

    def close(self):
        self.stop()
        self.channel.close()


@pytest.fixture
def log():
    """Quiet client-side event log (no console/file output)."""
    return EventLog("test-client", LamportClock(), verbose=False)


@pytest.fixture
def replica_factory():
    created = []

    def make(**kwargs):
        r = Replica(**kwargs)
        created.append(r)
        return r

    yield make
    for r in created:
        r.close()


@pytest.fixture
def replica(replica_factory):
    return replica_factory(name="replica-T")


@pytest.fixture
def replicas3(replica_factory):
    return [replica_factory(name=f"replica-{n}") for n in "ABC"]


def free_port():
    with socket.socket() as s:
        s.bind(("localhost", 0))
        return s.getsockname()[1]


@pytest.fixture
def spawn_replica():
    """Start a replica as a REAL OS process (route (a): proc.kill())."""
    procs, channels = [], []

    def spawn(name, delay_ms=0):
        port = free_port()
        cmd = [sys.executable, os.path.join(ROOT, "server.py"),
               "--port", str(port), "--name", name, "--quiet"]
        if delay_ms:
            cmd += ["--delay-ms", str(delay_ms)]
        proc = subprocess.Popen(cmd, cwd=ROOT,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        procs.append(proc)
        ch = grpc.insecure_channel(f"localhost:{port}")
        channels.append(ch)
        grpc.channel_ready_future(ch).result(timeout=15)  # wait until it accepts RPCs
        return proc, counter_pb2_grpc.CounterStub(ch)

    yield spawn
    for ch in channels:
        ch.close()
    for p in procs:
        if p.poll() is None:
            p.kill()
        p.wait()