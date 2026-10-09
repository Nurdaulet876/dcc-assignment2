import threading
import time

from clocks import LamportClock, EventLog
from client import increment, get, quorum_increment


def test_increment_applies_delta(replica, log):
    r = increment(replica.stub, log, "x", 5)
    assert r.new_value == 5 and not r.was_duplicate
    assert get(replica.stub, log, "x").value == 5
    assert get(replica.stub, log, "y").found is False


def test_duplicate_key_not_reapplied(replica, log):
    r1 = increment(replica.stub, log, "x", 5, key="k-1")
    r2 = increment(replica.stub, log, "x", 5, key="k-1")
    assert r1.new_value == 5 and not r1.was_duplicate
    assert r2.new_value == 5 and r2.was_duplicate
    assert get(replica.stub, log, "x").value == 5


def test_concurrent_increments_exact(replica):
    errors = []

    def worker(name):
        wlog = EventLog(name, LamportClock(), verbose=False)
        try:
            for _ in range(1000):
                increment(replica.stub, wlog, "hits", 1)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(f"c{i}",)) for i in (1, 2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    check = EventLog("check", LamportClock(), verbose=False)
    assert not errors
    assert get(replica.stub, check, "hits").value == 2000


def test_get_missing_counter(replica, log):
    r = get(replica.stub, log, "does-not-exist")
    assert r.found is False and r.value == 0


def test_retry_after_timeout_is_safe(replica_factory, log):
    slow = replica_factory(name="slow", delay_ms=3000, delay_first=1)
    r = increment(slow.stub, log, "x", 1)
    assert r.new_value == 1
    time.sleep(1.5)
    assert get(slow.stub, log, "x").value == 1


def test_majority_commit_two_acks(replicas3, log):
    a, b, c = replicas3
    b.stop()
    res = quorum_increment([r.stub for r in replicas3], log, "x", 1)
    assert res.committed and res.acks == 2 and res.total == 3
    assert get(a.stub, log, "x").value == 1
    assert get(c.stub, log, "x").value == 1


def test_no_commit_below_majority(replicas3, log):
    a, b, c = replicas3
    b.stop()
    c.stop()
    res = quorum_increment([r.stub for r in replicas3], log, "x", 1)

    assert not res.committed and res.acks == 1
    assert get(a.stub, log, "x").value == 1


def test_replicas_converge(replicas3, log):
    stubs = [r.stub for r in replicas3]
    expected = {"x": 0, "y": 0}
    for i in range(20):
        cid = "x" if i % 2 == 0 else "y"
        res = quorum_increment(stubs, log, cid, i + 1)
        assert res.committed and res.acks == 3
        expected[cid] += i + 1
    for r in replicas3:
        for cid, total in expected.items():
            assert get(r.stub, log, cid).value == total