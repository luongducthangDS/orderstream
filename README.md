# OrderStream: change data capture from Postgres to DuckDB

Every insert, update and delete in a shop's Postgres database is streamed into an analytics copy in DuckDB, and the
copy is **provably identical** to the source after normal runs, replays, a crash at the worst moment, and a schema
change.

```
Postgres 16 (wal_level=logical, pgoutput)
   │  every committed change, incl. deletes and intermediate updates
   ▼
Debezium 2.7 (Kafka Connect) ──► Redpanda 24.2 (Kafka API)   topics: shop.public.orders, shop.public.customers
                                      │
                                      ▼
                         Python consumer (micro-batches) ──► DuckDB: orders, customers,
                                                             order_status_history, dlq
```

## Results (`reports/results.json`, one run on a laptop, Docker Desktop)

| Check | Result |
|---|---|
| Workload | 2,000 customers, 30,000 orders, ~40,000 status changes, 200 customer updates, 500 deletes → **72,699 change events** |
| Catch-up throughput | **12,322 events/s** (72,699 events in 5.9 s) |
| Reconciliation | source vs sink: 29,500 = 29,500 orders, 74,195,669,000 = 74,195,669,000 VND, **0 mismatched rows** (row-by-row) |
| Replay from offset 0 | 72,699 events re-read, **0 applied, state identical** |
| Crash between sink commit and offset commit | 1,126 events redelivered, **1,126 skipped**, 880 new applied, still 0 mismatched rows |
| Schema change at source (`ALTER TABLE orders ADD COLUMN coupon`) | column added in the sink automatically; 294 / 294 rows carry the value |
| Live traffic (~40 inserts/s for 60 s) | source commit → row visible in DuckDB: **p50 0.98 s, p95 1.42 s, p99 1.53 s** |
| Final reconciliation (after live traffic) | 31,885 = 31,885 orders, 0 mismatched rows |

## How correctness is guaranteed

- **At-least-once delivery + idempotent sink.** Offsets are committed only after the DuckDB transaction commits.
  A crash in between means redelivery, never loss. The demo deliberately crashes at exactly that point.
- **LSN guard.** Each row stores the Postgres LSN of the change that produced it. A change is written only if its
  LSN is newer, so duplicates, replays and late older events cannot overwrite newer state.
- **Newest version per key per batch.** A batch keeps only the latest change per id (the 942 "skipped" events in
  catch-up are orders that changed several times inside one batch); the status history keeps all of them.
- **Full row images** (`REPLICA IDENTITY FULL`), so updates and deletes never need a lookup at the source.
- **Soft deletes** (`_deleted = true`) keep deleted orders available for audit.
- **Bad events go to a `dlq` table** instead of stopping the stream.

## Decisions and what I measured before making them

- **Micro-batches instead of row-by-row.** The first version applied one event per DuckDB statement and kept up
  with only ~70 events/s (observed consumer lag while another CPU-heavy job was running on the same laptop).
  Batching 2,000 events per transaction with one set-based `INSERT OR REPLACE ... BY NAME` gave 12,322 events/s.
  The cost is latency: a batch is flushed at 2,000 events or 0.5 s.
- **Where the 1 s latency comes from:** Debezium's default 0.5 s poll interval plus the 0.5 s flush window. Lower
  both if fresher data is worth more CPU. Latency is measured as Postgres commit time (from the event) to DuckDB
  commit on the host, so it also includes any clock difference between the Docker VM and the host.
- **Redpanda instead of Apache Kafka** for a single-node local stack: same Kafka API, one container, no ZooKeeper
  or KRaft tuning.
- **CDC instead of polling `updated_at`:** polling misses deletes and changes that happen between two polls.

## Limits (honest list)

- One partition per topic, so no parallel consumers; ordering per key would need `id` as the partition key.
- DuckDB allows one writer process. For several writers the sink would move to Postgres or a lakehouse format.
- New columns are added; renamed or retyped columns are not handled.

## Run it

```bash
pip install -r requirements.txt
pytest -q                         # 9 sink tests: idempotency, ordering, deletes, schema change, DLQ, batching
docker compose up -d --wait       # Postgres :55432, Redpanda :19092, Kafka Connect :8083
python scripts/run_demo.py        # ~5 min -> reports/results.json
docker compose down -v
```

## Interview questions this project prepares for

1. Why is "exactly-once" here an effect and not a delivery guarantee?
2. What breaks if two partitions carry changes for the same order?
3. How would you backfill a new table without stopping the stream?
4. Why can the LSN be trusted as an order, but `updated_at` cannot?
