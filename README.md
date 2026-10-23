# OrderStream: real-time CDC pipeline (Postgres → Debezium → Redpanda → DuckDB)

A production-style Change Data Capture (CDC) streaming pipeline that mirrors an OLTP PostgreSQL database into a DuckDB analytical sink in near real-time, with strict ordering, idempotency, and automated schema evolution.

## Architecture

```
PostgreSQL 16 (logical replication)
       │ (WAL events via wal2json / pgoutput)
       ▼
Debezium 2.7 (Kafka Connect)
       │ (JSON change events: c, u, d)
       ▼
Redpanda 24.2 (Kafka-compatible event broker)
       │ (topic: shop.public.orders, shop.public.customers)
       ▼
OrderStream Consumer (Python)
       │ (LSN deduplication, schema evolution, SCD2 status history, DLQ)
       ▼
DuckDB Analytical Sink (read-optimized analytical storage)
```

## Core Engineering Features

1. **Log Sequence Number (LSN) Deduplication**:
   - Every event carries the Postgres WAL `lsn`.
   - The sink compares `_lsn`: if an event has `lsn <= current_lsn`, it is safely skipped.
   - Replaying a topic from offset 0 produces **0 state drift** (strictly idempotent).

2. **Out-of-Order & Late Event Protection**:
   - Network delays or consumer restarts cannot cause older updates to overwrite newer state.

3. **Automated Schema Evolution**:
   - When a new column (e.g., `coupon TEXT`) is added to Postgres, the sink detects missing columns and issues `ALTER TABLE ... ADD COLUMN` on DuckDB dynamically without crashing or dropping messages.

4. **Soft Deletes**:
   - Deletes (`op: "d"`) set `_deleted = true`, preserving historical records for downstream analytics and audit trails.

5. **State Change History (SCD Type 2)**:
   - Tracks every lifecycle transition (`created → paid → shipped → delivered`) into `order_status_history` with timestamps and LSN.

6. **Dead Letter Queue (DLQ)**:
   - Malformed JSON or unexpected payloads are isolated into a `dlq` table for inspection rather than blocking the stream.

## Run It

### 1. Run unit tests (correctness, replay, schema evolution, DLQ)
```bash
pytest -q
```

### 2. Start the local streaming stack
```bash
docker compose up -d
```
Services started:
- PostgreSQL on port `55432` (`wal_level=logical`)
- Redpanda on port `19092`
- Debezium Connect on port `8083`

### 3. Run end-to-end demo and latency benchmark
```bash
python scripts/run_demo.py
```
This script:
1. Registers the Debezium Postgres CDC connector.
2. Generates 2,000 customers, 30,000 orders, 40,000 updates, and 500 deletes.
3. Consumes events and reconciles OLTP vs OLAP state.
4. Replays from offset 0 to prove 100% idempotency.
5. Performs a schema migration on PostgreSQL and verifies dynamic schema evolution.
6. Streams live traffic at 50 msg/s to measure commit-to-sink latency (p50, p95, p99).

## Design Decisions

- **Why Redpanda over JVM Kafka?** Zero JVM tuning, starts in seconds with single-binary simplicity, 100% Kafka API compatible, and extremely light on resources for local/edge deployments.
- **Why Debezium logical decoding?** Captures deletes and intermediate updates that query polling would completely miss, with zero polling load on the database.
- **Why DuckDB as analytical sink?** Fast columnar vectorized queries, zero client/server overhead, and seamless SQL integration with Python/dbt.
