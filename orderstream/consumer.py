"""Kafka -> DuckDB consumer. At-least-once delivery (offsets committed only after the sink has applied the
batch) + an idempotent sink = exactly-once *effect*."""
import json
import time

from confluent_kafka import Consumer

TOPICS = ["shop.public.customers", "shop.public.orders"]


def consume(sink, group: str, idle_s: float = 8.0, max_msgs: int | None = None, commit_every: int = 500,
            bootstrap: str = "localhost:19092") -> dict:
    """Consume until the topics are idle for idle_s. With max_msgs, stop early WITHOUT committing the
    last partial batch: that is how a crash looks to Kafka."""
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe(TOPICS)
    stats = {"applied": 0, "skipped": 0, "dlq": 0, "lat_ms": []}
    seen, pending, last = 0, 0, time.time()
    try:
        while time.time() - last < idle_s:
            msg = c.poll(0.5)
            if msg is None or msg.error():
                continue
            last = time.time()
            if msg.value() is None:
                continue
            result = sink.apply(msg.value().decode())
            stats[result] += 1
            if result == "applied":
                ts = json.loads(msg.value())["source"]["ts_ms"]   # source commit time
                stats["lat_ms"].append(time.time() * 1000 - ts)
            seen += 1
            pending += 1
            if max_msgs and seen >= max_msgs:
                return stats                                 # "crash": no commit for the pending batch
            if pending >= commit_every:
                c.commit(asynchronous=False)
                pending = 0
        if pending:
            c.commit(asynchronous=False)
        return stats
    finally:
        c.close()
