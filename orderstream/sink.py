"""DuckDB analytics sink for Debezium change events, applied in micro-batches.

Idempotent and order-safe: each row remembers the source LSN of the change that produced it, and a change is
applied only if its LSN is newer. Replaying a topic, receiving a duplicate, or receiving an older event late
therefore never changes the result.
"""
import json
from pathlib import Path

import duckdb
import pandas as pd

TABLES = {
    "orders": {"id": "BIGINT", "customer_id": "BIGINT", "status": "VARCHAR", "amount_vnd": "BIGINT",
               "updated_at": "VARCHAR"},
    "customers": {"id": "BIGINT", "name": "VARCHAR", "city": "VARCHAR", "updated_at": "VARCHAR"},
}


class Sink:
    def __init__(self, path: Path):
        self.con = duckdb.connect(str(path))
        self._cols: dict[str, set] = {}
        for table, cols in TABLES.items():
            col_sql = ", ".join(f"{c} {t}" for c, t in cols.items())
            self.con.execute(f"CREATE TABLE IF NOT EXISTS {table} ({col_sql}, _lsn BIGINT, _deleted BOOLEAN,"
                             f" PRIMARY KEY (id))")
        self.con.execute("""CREATE TABLE IF NOT EXISTS order_status_history (
            order_id BIGINT, status VARCHAR, lsn BIGINT, source_ts_ms BIGINT, PRIMARY KEY (order_id, lsn))""")
        self.con.execute("CREATE TABLE IF NOT EXISTS dlq (raw VARCHAR, error VARCHAR)")

    def _ensure_columns(self, table: str, rows: list[dict]):
        """Schema evolution: a column added at the source is added here instead of crashing the stream."""
        if table not in self._cols:
            self._cols[table] = {r[0] for r in self.con.execute(f"DESCRIBE {table}").fetchall()}
        existing = self._cols[table]
        for row in rows:
            for col, val in row.items():
                if col not in existing and val is not None:
                    sql_type = "BIGINT" if isinstance(val, int) else "DOUBLE" if isinstance(val, float) else "VARCHAR"
                    self.con.execute(f'ALTER TABLE {table} ADD COLUMN "{col}" {sql_type}')
                    existing.add(col)

    def apply_batch(self, raws: list[str]) -> dict:
        """Apply a batch of Debezium events (JSON, schemas disabled).
        Returns counts: applied (rows written), skipped (duplicate/older/superseded in batch), dlq."""
        out = {"applied": 0, "skipped": 0, "dlq": 0}
        newest: dict[str, dict] = {t: {} for t in TABLES}
        history = []
        for raw in raws:
            try:
                ev = json.loads(raw)
                table, op, lsn = ev["source"]["table"], ev["op"], int(ev["source"]["lsn"])
                row = ev["before"] if op == "d" else ev["after"]
                if table not in TABLES or row is None or "id" not in row:
                    raise ValueError("unexpected event shape")
            except (ValueError, KeyError, TypeError) as e:     # JSONDecodeError is a ValueError
                self.con.execute("INSERT INTO dlq VALUES (?, ?)", [raw[:2000], str(e)])
                out["dlq"] += 1
                continue
            rec = {**row, "_lsn": lsn, "_deleted": op == "d"}
            cur = newest[table].get(row["id"])
            if cur is None or cur["_lsn"] < lsn:
                newest[table][row["id"]] = rec
            if table == "orders" and op != "d":
                history.append((row["id"], row["status"], lsn, ev["source"]["ts_ms"]))
        for table, by_id in newest.items():
            if not by_id:
                continue
            rows = list(by_id.values())
            self._ensure_columns(table, rows)
            batch = pd.DataFrame(rows).astype(object)  # noqa: F841  (read by DuckDB below)
            batch = batch.where(batch.notna(), None)   # a key missing in older events = NULL, not NaN
            n = self.con.execute(f"""INSERT OR REPLACE INTO {table} BY NAME
                SELECT b.* FROM batch b LEFT JOIN {table} t ON t.id = b.id
                WHERE t._lsn IS NULL OR t._lsn < b._lsn""").fetchone()[0]
            out["applied"] += n
        out["skipped"] = len(raws) - out["dlq"] - out["applied"]
        if history:
            hist = pd.DataFrame(history, columns=["order_id", "status", "lsn", "source_ts_ms"])  # noqa: F841
            self.con.execute("INSERT OR IGNORE INTO order_status_history SELECT * FROM hist")
        return out

    def apply(self, raw: str) -> str:
        """Single event, for tests: 'applied' | 'skipped' | 'dlq'."""
        r = self.apply_batch([raw])
        return next(k for k in ("dlq", "applied", "skipped") if r[k])
