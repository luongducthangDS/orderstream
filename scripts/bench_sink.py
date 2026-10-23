"""Sink write cost without Kafka: one event per DuckDB transaction vs 2,000 per transaction.
Same synthetic Debezium events for both. Writes reports/bench_sink.json."""
import json
import random
import tempfile
import time
from pathlib import Path

from orderstream.sink import Sink

ROOT = Path(__file__).resolve().parents[1]
rng = random.Random(0)


def events(n):
    out = []
    for lsn in range(1, n + 1):
        oid = rng.randint(1, n // 3)
        row = {"id": oid, "customer_id": 1, "status": rng.choice(["created", "paid", "shipped"]),
               "amount_vnd": 100_000, "updated_at": "2026-01-01T00:00:00Z"}
        out.append(json.dumps({"op": "u", "before": None, "after": row,
                               "source": {"table": "orders", "lsn": lsn, "ts_ms": 0}}))
    return out


def run(evs, batch):
    with tempfile.TemporaryDirectory() as d:
        sink = Sink(Path(d) / "b.duckdb")
        t = time.perf_counter()
        for i in range(0, len(evs), batch):
            sink.con.begin()
            sink.apply_batch(evs[i:i + batch])
            sink.con.commit()
        dt = time.perf_counter() - t
        sink.con.close()
    return {"events": len(evs), "batch_size": batch, "seconds": round(dt, 2), "events_per_s": round(len(evs) / dt)}


if __name__ == "__main__":
    out = {"row_by_row": run(events(3_000), 1), "micro_batch": run(events(60_000), 2_000)}
    (ROOT / "reports/bench_sink.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))
