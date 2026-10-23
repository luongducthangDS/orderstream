"""DuckDB analytics sink for Debezium change events.

Idempotent and order-safe: each row remembers the source LSN of the change that produced it, and a change is
applied only if its LSN is newer. Replaying a topic, receiving a duplicate, or receiving an older event late
therefore never changes the result.
"""
import json
from pathlib import Path

import duckdb

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
        self.con.execute("""CREATE TABLE IF NOT EXISTS dlq (raw VARCHAR, error VARCHAR)""")

    def _ensure_columns(self, table: str, row: dict):
        """Schema evolution: a column added at the source is added here instead of crashing the stream."""
        if table not in self._cols:
            self._cols[table] = {r[0] for r in self.con.execute(f"DESCRIBE {table}").fetchall()}
        existing = self._cols[table]
        for col, val in row.items():
            if col not in existing:
                existing.add(col)
                sql_type = "BIGINT" if isinstance(val, int) else "DOUBLE" if isinstance(val, float) else "VARCHAR"
                self.con.execute(f'ALTER TABLE {table} ADD COLUMN "{col}" {sql_type}')

    def apply(self, raw: str) -> str:
        """Apply one Debezium event (JSON, schemas disabled). Returns 'applied' | 'skipped' | 'dlq'."""
        try:
            ev = json.loads(raw)
            table, op, lsn = ev["source"]["table"], ev["op"], int(ev["source"]["lsn"])
            row = ev["before"] if op == "d" else ev["after"]
            if table not in TABLES or row is None or "id" not in row:
                raise ValueError("unexpected event shape")
        except (ValueError, KeyError, TypeError, json.JSONDecodeError) as e:
            self.con.execute("INSERT INTO dlq VALUES (?, ?)", [raw[:2000], str(e)])
            return "dlq"
        cur = self.con.execute(f"SELECT _lsn FROM {table} WHERE id = ?", [row["id"]]).fetchone()
        if cur is not None and cur[0] >= lsn:
            return "skipped"                     # duplicate or late event: already have a newer version
        self._ensure_columns(table, row)
        cols = list(row) + ["_lsn", "_deleted"]
        vals = list(row.values()) + [lsn, op == "d"]
        quoted = ", ".join(f'"{c}"' for c in cols)
        self.con.execute(f"INSERT OR REPLACE INTO {table} ({quoted}) VALUES ({', '.join('?' * len(cols))})", vals)
        if table == "orders" and op != "d":
            self.con.execute("INSERT OR IGNORE INTO order_status_history VALUES (?, ?, ?, ?)",
                             [row["id"], row["status"], lsn, ev["source"]["ts_ms"]])
        return "applied"
