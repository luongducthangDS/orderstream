"""Kafka -> DuckDB consumer. At-least-once delivery (offsets committed only after the sink transaction has
committed) + an idempotent sink = exactly-once *effect*."""
import json
import time

from confluent_kafka import Consumer

TOPICS = ["shop.public.customers", "shop.public.orders"]


def consume(sink, group: str, idle_s: float = 8.0, max_msgs: int | None = None, batch_size: int = 2_000,
            max_wait_s: float = 0.5, bootstrap: str = "localhost:19092") -> dict:
    """Consume until the topics are idle for idle_s. A batch is flushed at batch_size events or after
    max_wait_s. With max_msgs, stop after flushing the sink but BEFORE committing offsets: the worst
    possible crash point, which forces redelivery."""
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": group, "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe(TOPICS)
    stats = {"applied": 0, "skipped": 0, "dlq": 0, "events": 0, "lat_ms": []}
    buf, last_msg, first_in_buf = [], time.time(), None

    def flush(commit_offsets=True):
        nonlocal buf, first_in_buf
        if not buf:
            return
        sink.con.begin()
        r = sink.apply_batch(buf)
        sink.con.commit()
        now = time.time() * 1000      # latency = source commit -> row visible in the sink
        stats["lat_ms"].extend(now - json.loads(m)["source"]["ts_ms"] for m in buf)
        for k in ("applied", "skipped", "dlq"):
            stats[k] += r[k]
        stats["events"] += len(buf)
        if commit_offsets:
            c.commit(asynchronous=False)  # positions cover exactly the messages in buf
        buf, first_in_buf = [], None

    try:
        while time.time() - last_msg < idle_s:
            if buf and time.time() - first_in_buf > max_wait_s:
                flush()
            msg = c.poll(0.1)
            if msg is None or msg.error():
                continue
            last_msg = time.time()
            if msg.value() is None:
                continue
            buf.append(msg.value().decode())
            first_in_buf = first_in_buf or time.time()
            if max_msgs and stats["events"] + len(buf) >= max_msgs:
                flush(commit_offsets=False)      # "crash": sink committed, offsets not
                return stats
            if len(buf) >= batch_size:
                flush()
        flush()
        return stats
    finally:
        c.close()
