import threading
import time

from client import get, quorum_increment


def _write_in_thread(stubs, log, box, counter="x", delta=1, key=None):
    """Run a quorum write in a thread; capture result or exception."""
    def run():
        try:
            box["res"] = quorum_increment(stubs, log, counter, delta, key)
        except Exception as e:
            box["exc"] = e
    t = threading.Thread(target=run)
    t.start()
    return t


def test_crash_mid_request_still_commits(replica_factory, log):
    a = replica_factory(name="A")
    b = replica_factory(name="B", delay_ms=1500)
    c = replica_factory(name="C")
    box = {}
    t = _write_in_thread([a.stub, b.stub, c.stub], log, box)
    time.sleep(0.3)
    b.stop()
    t.join(timeout=20)
    assert "exc" not in box
    res = box["res"]
    assert res.committed and res.acks == 2
    assert get(a.stub, log, "x").value == 1
    assert get(c.stub, log, "x").value == 1


def test_crash_with_real_process_kill(spawn_replica, log):
    pa, sa = spawn_replica("A")
    pb, sb = spawn_replica("B", delay_ms=1500)
    pc, sc = spawn_replica("C")
    box = {}
    t = _write_in_thread([sa, sb, sc], log, box)
    time.sleep(0.3)
    pb.kill()
    pb.wait()
    t.join(timeout=20)
    assert "exc" not in box
    assert box["res"].committed and box["res"].acks == 2
    assert get(sa, log, "x").value == 1
    assert get(sc, log, "x").value == 1


def test_duplicate_request_moves_value_once(replicas3, log):
    stubs = [r.stub for r in replicas3]
    r1 = quorum_increment(stubs, log, "x", 1, key="dup-key")
    r2 = quorum_increment(stubs, log, "x", 1, key="dup-key")
    assert r1.committed and not r1.duplicate
    assert r2.committed and r2.duplicate
    for r in replicas3:
        assert get(r.stub, log, "x").value == 1


def test_timeout_then_retry_moves_value_once(replica_factory, log):
    a = replica_factory(name="A", delay_ms=3000, delay_first=1)
    b = replica_factory(name="B")
    c = replica_factory(name="C")
    res = quorum_increment([a.stub, b.stub, c.stub], log, "x", 1)
    assert res.committed and res.acks == 3
    time.sleep(1.5)
    for r in (a, b, c):
        assert get(r.stub, log, "x").value == 1