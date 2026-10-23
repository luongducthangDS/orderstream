"""Sink correctness without Kafka: idempotency, ordering, deletes, schema evolution, DLQ."""
import json

import pytest

from orderstream.sink import Sink


def event(op, lsn, row, table="orders", ts=1):
    return json.dumps({"op": op, "before": row if op == "d" else None, "after": None if op == "d" else row,
                       "source": {"table": table, "lsn": lsn, "ts_ms": ts}})


def order(id=1, status="created", amount=100, **extra):
    return {"id": id, "customer_id": 7, "status": status, "amount_vnd": amount, "updated_at": "2026-01-01T00:00:00Z", **extra}


@pytest.fixture
def sink(tmp_path):
    return Sink(tmp_path / "t.duckdb")


def state(sink):
    return sink.con.execute("SELECT id, status, amount_vnd, _deleted FROM orders ORDER BY id").fetchall()


def test_insert_then_update(sink):
    sink.apply(event("c", 10, order()))
    sink.apply(event("u", 20, order(status="paid")))
    assert state(sink) == [(1, "paid", 100, False)]


def test_replay_is_idempotent(sink):
    events = [event("c", 10, order()), event("u", 20, order(status="paid")), event("u", 30, order(status="shipped"))]
    for e in events:
        sink.apply(e)
    before = state(sink)
    assert [sink.apply(e) for e in events] == ["skipped"] * 3
    assert state(sink) == before


def test_late_old_event_does_not_overwrite_newer_state(sink):
    sink.apply(event("u", 30, order(status="shipped")))
    assert sink.apply(event("u", 20, order(status="paid"))) == "skipped"
    assert state(sink) == [(1, "shipped", 100, False)]


def test_delete_is_a_soft_delete(sink):
    sink.apply(event("c", 10, order()))
    sink.apply(event("d", 20, order()))
    assert state(sink) == [(1, "created", 100, True)]


def test_new_source_column_is_added_not_fatal(sink):
    sink.apply(event("c", 10, order(coupon="SALE10")))
    assert sink.con.execute("SELECT coupon FROM orders").fetchone() == ("SALE10",)


def test_malformed_event_goes_to_dlq(sink):
    assert sink.apply("{not json") == "dlq"
    assert sink.apply(json.dumps({"op": "c"})) == "dlq"
    assert sink.con.execute("SELECT count(*) FROM dlq").fetchone()[0] == 2


def test_status_history_has_one_row_per_change(sink):
    for lsn, s in [(10, "created"), (20, "paid"), (20, "paid"), (30, "shipped")]:
        sink.apply(event("u", lsn, order(status=s)))
    rows = sink.con.execute("SELECT status FROM order_status_history ORDER BY lsn").fetchall()
    assert [r[0] for r in rows] == ["created", "paid", "shipped"]
