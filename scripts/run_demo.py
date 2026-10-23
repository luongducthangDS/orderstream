"""End-to-end run against the docker-compose stack. Writes reports/results.json.

Phases: register connector -> bulk load + lifecycle updates + deletes -> consume (throughput)
-> reconcile -> replay from offset 0 (idempotency) -> crash mid-batch and resume -> schema change
-> live traffic at a fixed rate (end-to-end latency) -> final reconcile.
"""
import json
import random
import statistics
import threading
import time
from pathlib import Path

import psycopg
import requests

from orderstream.consumer import consume
from orderstream.sink import Sink

ROOT = Path(__file__).resolve().parents[1]
PG = "postgresql://postgres:postgres@localhost:55432/shop"
CONNECT = "http://localhost:8083/connectors"
rng = random.Random(42)
NEXT = {"created": ["paid", "cancelled"], "paid": ["shipped", "cancelled"], "shipped": ["delivered"]}


def register():
    for _ in range(60):
        try:
            requests.get(CONNECT, timeout=3).raise_for_status()
            break
        except requests.RequestException:
            time.sleep(2)
    cfg = json.loads((ROOT / "connect/postgres-connector.json").read_text())
    if requests.get(f"{CONNECT}/{cfg['name']}").status_code == 404:
        requests.post(CONNECT, json=cfg).raise_for_status()
    for _ in range(60):
        st = requests.get(f"{CONNECT}/{cfg['name']}/status").json()
        if st.get("tasks") and st["tasks"][0]["state"] == "RUNNING":
            return
        time.sleep(2)
    raise RuntimeError(st)


def bulk(con, n_customers=2_000, n_orders=30_000, n_updates=40_000, n_deletes=500):
    cities = ["Hanoi", "HCMC", "Da Nang", "Hai Phong", "Can Tho"]
    with con.cursor() as cur:
        cur.executemany("INSERT INTO customers (name, city) VALUES (%s, %s)",
                        [(f"cust{i}", rng.choice(cities)) for i in range(n_customers)])
        cur.executemany("INSERT INTO orders (customer_id, status, amount_vnd) VALUES (%s, 'created', %s)",
                        [(rng.randint(1, n_customers), rng.randint(20, 5_000) * 1_000) for _ in range(n_orders)])
        con.commit()
        done = 0
        while done < n_updates:              # random order lifecycle moves, many orders change several times
            oid = rng.randint(1, n_orders)
            cur.execute("SELECT status FROM orders WHERE id = %s", [oid])
            r = cur.fetchone()
            if r and r[0] in NEXT:
                cur.execute("UPDATE orders SET status = %s, updated_at = now() WHERE id = %s",
                            [rng.choice(NEXT[r[0]]), oid])
                done += 1
                if done % 1000 == 0:
                    con.commit()
        cur.execute("UPDATE customers SET city = 'HCMC', updated_at = now() WHERE id %% 10 = 0")
        cur.execute("DELETE FROM orders WHERE id IN (SELECT id FROM orders WHERE status = 'cancelled' "
                    "ORDER BY id LIMIT %s)", [n_deletes])
        con.commit()


def reconcile(con, sink) -> dict:
    """Source vs sink: row counts, money totals, status mix, and a row-by-row comparison."""
    with con.cursor() as cur:
        cur.execute("SELECT id, customer_id, status, amount_vnd FROM orders ORDER BY id")
        src = cur.fetchall()
        cur.execute("SELECT id, city FROM customers ORDER BY id")
        src_c = cur.fetchall()
    dst = sink.con.execute("SELECT id, customer_id, status, amount_vnd FROM orders WHERE NOT _deleted "
                           "ORDER BY id").fetchall()
    dst_c = sink.con.execute("SELECT id, city FROM customers WHERE NOT _deleted ORDER BY id").fetchall()
    mismatched = len(set(src) ^ set(map(tuple, dst)))
    return {"orders_source": len(src), "orders_sink": len(dst),
            "amount_source": sum(r[3] for r in src), "amount_sink": sum(r[3] for r in dst),
            "customers_source": len(src_c), "customers_sink": len(dst_c),
            "mismatched_rows": mismatched + len(set(src_c) ^ set(map(tuple, dst_c))),
            "deleted_in_sink": sink.con.execute("SELECT count(*) FROM orders WHERE _deleted").fetchone()[0]}


def snapshot(sink):
    return sink.con.execute("SELECT * FROM orders ORDER BY id").fetchall()


def pct(xs, q):
    xs = sorted(xs)
    return round(xs[min(len(xs) - 1, int(q * len(xs)))], 1)


def main():
    out = {}
    register()
    db = ROOT / "reports/orderstream.duckdb"
    db.unlink(missing_ok=True)
    sink = Sink(db)
    with psycopg.connect(PG) as con:
        t = time.time()
        bulk(con)
        out["generate_s"] = round(time.time() - t, 1)
        with con.cursor() as cur:
            cur.execute("SELECT count(*) FROM orders"); out["orders_after_generate"] = cur.fetchone()[0]

        # 1) catch up from the snapshot + change stream
        t = time.time()
        s = consume(sink, "sink-main")
        dt = time.time() - t - 8.0                       # minus the idle wait at the end
        events = s["applied"] + s["skipped"] + s["dlq"]
        out["catch_up"] = {"events": events, "applied": s["applied"], "skipped": s["skipped"], "dlq": s["dlq"],
                           "seconds": round(dt, 1), "events_per_s": round(events / dt)}
        out["reconcile_after_catch_up"] = reconcile(con, sink)

        # 2) replay everything from offset 0 with a new consumer group: nothing may change
        before = snapshot(sink)
        s = consume(sink, "sink-replay")
        out["replay"] = {"events": s["applied"] + s["skipped"], "applied": s["applied"],
                         "state_identical": snapshot(sink) == before}

        # 3) crash: new writes, consumer dies mid-batch without committing, then restarts
        with con.cursor() as cur:
            cur.execute("UPDATE orders SET status = 'delivered', updated_at = now() "
                        "WHERE status = 'shipped'")
            n_crash = cur.rowcount
            con.commit()
        time.sleep(5)
        first = consume(sink, "sink-main", max_msgs=n_crash // 2 + 123)   # dies mid-batch
        second = consume(sink, "sink-main")
        out["crash_resume"] = {"source_changes": n_crash, "processed_before_crash": first["applied"] + first["skipped"],
                               "redelivered_and_skipped": second["skipped"], "applied_after_restart": second["applied"],
                               "reconcile": reconcile(con, sink)}

        # 4) schema change at the source while the pipeline runs
        with con.cursor() as cur:
            cur.execute("ALTER TABLE orders ADD COLUMN coupon TEXT")
            cur.execute("UPDATE orders SET coupon = 'SALE10', updated_at = now() WHERE id %% 100 = 1")
            n_coupon = cur.rowcount
            con.commit()
        time.sleep(5)
        consume(sink, "sink-main")
        out["schema_change"] = {"rows_updated_with_coupon": n_coupon,
                                "sink_rows_with_coupon": sink.con.execute(
                                    "SELECT count(*) FROM orders WHERE coupon = 'SALE10'").fetchone()[0]}

        # 5) live traffic: 50 new orders/s for 60 s while the consumer runs -> commit-to-sink latency
        stop = threading.Event()

        def writer():
            with psycopg.connect(PG, autocommit=True) as c2:
                while not stop.is_set():
                    c2.execute("INSERT INTO orders (customer_id, status, amount_vnd) VALUES (%s, 'created', %s)",
                               [rng.randint(1, 2000), 100_000])
                    time.sleep(0.02)
        th = threading.Thread(target=writer); th.start()
        timer = threading.Timer(60, stop.set); timer.start()
        s = consume(sink, "sink-main", idle_s=10, commit_every=50)
        th.join()
        lat = s["lat_ms"]
        out["live"] = {"events": len(lat), "latency_ms_p50": pct(lat, .5), "latency_ms_p95": pct(lat, .95),
                       "latency_ms_p99": pct(lat, .99), "latency_ms_max": round(max(lat), 1)}
        out["final_reconcile"] = reconcile(con, sink)
        out["status_history_rows"] = sink.con.execute("SELECT count(*) FROM order_status_history").fetchone()[0]
    (ROOT / "reports/results.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
