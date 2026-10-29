"""Transactional, idempotent CSV -> SQLite star-schema ETL."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import re
import sqlite3
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .generator import CUSTOMER_HEADER, ITEM_HEADER, ORDER_HEADER, PRODUCT_HEADER

SCHEMAS = {
    "customers.csv": CUSTOMER_HEADER,
    "products.csv": PRODUCT_HEADER,
    "orders.csv": ORDER_HEADER,
    "order_items.csv": ITEM_HEADER,
}
LOGGER = logging.getLogger("warehouse")


class PipelineError(Exception):
    """A batch-level failure that should roll back the warehouse load."""


class InvalidRecord(ValueError):
    """An individual source row that should be quarantined."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def stable_key(namespace: str, identifier: str) -> int:
    """Deterministic positive 63-bit SQLite integer; namespace avoids cross-entity reuse."""
    digest = hashlib.sha256(f"{namespace}:{identifier}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & ((1 << 63) - 1)


def validate_int(raw: str, column: str, minimum: int = 0) -> int:
    if not re.fullmatch(r"-?\d+", raw):
        raise InvalidRecord(f"invalid_integer:{column}")
    value = int(raw)
    if value < minimum:
        raise InvalidRecord(f"out_of_range:{column} (minimum {minimum})")
    return value


def normalize_timestamp(raw: str) -> str:
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidRecord("invalid_order_timestamp") from exc
    if parsed.utcoffset() is None:
        raise InvalidRecord("timestamp_requires_timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="seconds")


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_database(conn: sqlite3.Connection) -> None:
    schema_path = Path(__file__).resolve().parents[2] / "sql" / "schema.sql"
    conn.executescript(schema_path.read_text(encoding="utf-8"))
    conn.commit()


class Loader:
    def __init__(self, conn: sqlite3.Connection, source_dir: Path, run_id: str, fail_after_facts: int | None):
        self.conn = conn
        self.source_dir = source_dir
        self.run_id = run_id
        self.fail_after_facts = fail_after_facts
        self.metrics = {"input_rows": 0, "output_rows": 0, "rejected_rows": 0, "skipped_rows": 0}
        self.customers: dict[str, int] = {}
        self.products: dict[str, int] = {}
        self.orders: dict[str, tuple[str, str, str]] = {}

    def rows(self, filename: str):
        path = self.source_dir / filename
        with path.open("r", newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle, strict=True)
            header = reader.fieldnames
            expected = SCHEMAS[filename]
            if header != expected:
                raise PipelineError(f"schema_mismatch:{filename}: expected={expected}, actual={header}")
            for record in reader:
                self.metrics["input_rows"] += 1
                # Extra fields appear under the None key in DictReader.
                raw = {str(key) if key is not None else "__extra_columns__": value
                       for key, value in record.items()}
                yield filename, reader.line_num, raw

    def clean(self, raw: dict) -> dict[str, str]:
        if "__extra_columns__" in raw or any(value is None for value in raw.values()):
            raise InvalidRecord("malformed_row:column_count_mismatch")
        values = {key: value.strip() for key, value in raw.items()}
        for key, value in values.items():
            if not value:
                raise InvalidRecord(f"missing_required_field:{key}")
        return values

    def reject(self, filename: str, row_number: int, raw: dict, source_id: str | None, reason: str):
        self.conn.execute(
            """INSERT INTO rejected_records
               (run_id, source_file, source_row_number, source_id, raw_json, reject_reason, rejected_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (self.run_id, filename, row_number, source_id,
             json.dumps(raw, ensure_ascii=False, sort_keys=True), reason, utc_now()),
        )
        self.metrics["rejected_rows"] += 1
        LOGGER.warning("REJECT %s:%s %s %s", filename, row_number, source_id or "", reason)

    def load_dimension(self, filename: str, table: str, id_col: str, key_col: str, attributes: list[str]):
        seen: dict[str, tuple] = {}
        for source_file, row_number, raw in self.rows(filename):
            identifier = raw.get(id_col) or None
            try:
                row = self.clean(raw)
                identifier = row[id_col]
                signature = (identifier, *(row[name] for name in attributes))
                if identifier in seen:
                    if seen[identifier] == signature:
                        self.metrics["skipped_rows"] += 1
                    else:
                        raise InvalidRecord(f"conflicting_duplicate:{id_col}")
                    continue
                key = stable_key(table, identifier)
                existing = self.conn.execute(
                    f"SELECT {id_col}, {', '.join(attributes)} FROM {table} WHERE {id_col} = ?",
                    (identifier,),
                ).fetchone()
                if existing is not None and tuple(existing) != signature:
                    # Quarantine the changed snapshot, but use the existing published
                    # dimension key for facts. Avoid unnecessary cascading rejects.
                    if table == "dim_customer":
                        self.customers[identifier] = key
                    else:
                        self.products[identifier] = key
                    raise InvalidRecord(f"changed_reference_snapshot:{id_col}")
                if existing is None:
                    columns = [key_col, id_col, *attributes]
                    placeholders = ", ".join("?" for _ in columns)
                    self.conn.execute(
                        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
                        (key, *signature),
                    )
                else:
                    self.metrics["skipped_rows"] += 1
                seen[identifier] = signature
                if table == "dim_customer":
                    self.customers[identifier] = key
                else:
                    self.products[identifier] = key
            except InvalidRecord as exc:
                self.reject(source_file, row_number, raw, identifier, str(exc))

    def load_orders(self):
        for filename, row_number, raw in self.rows("orders.csv"):
            identifier = raw.get("order_id") or None
            try:
                row = self.clean(raw)
                identifier = row["order_id"]
                timestamp = normalize_timestamp(row["order_timestamp"])
                status = row["order_status"].lower()
                if status not in {"completed", "cancelled", "pending"}:
                    raise InvalidRecord("invalid_order_status")
                if row["customer_id"] not in self.customers:
                    raise InvalidRecord("invalid_reference:customer_id")
                if status != "completed" and self.conn.execute(
                    "SELECT 1 FROM fact_sales_order_line WHERE order_id = ? LIMIT 1", (identifier,)
                ).fetchone():
                    raise InvalidRecord("previously_loaded_order_changed_status:requires_backfill")
                signature = (row["customer_id"], timestamp, status)
                if identifier in self.orders:
                    if self.orders[identifier] == signature:
                        self.metrics["skipped_rows"] += 1
                    else:
                        raise InvalidRecord("conflicting_duplicate:order_id")
                    continue
                self.orders[identifier] = signature
            except InvalidRecord as exc:
                self.reject(filename, row_number, raw, identifier, str(exc))

    def load_items(self):
        seen: dict[tuple[str, int], tuple] = {}
        for filename, row_number, raw in self.rows("order_items.csv"):
            identifier = f"{raw.get('order_id', '')}:{raw.get('line_no', '')}"
            try:
                row = self.clean(raw)
                line_no = validate_int(row["line_no"], "line_no", 1)
                quantity = validate_int(row["quantity"], "quantity", 1)
                price = validate_int(row["unit_price_cents"], "unit_price_cents", 0)
                discount = validate_int(row["discount_cents"], "discount_cents", 0)
                gross = quantity * price
                if discount > gross:
                    raise InvalidRecord("discount_exceeds_gross")
                natural_key = (row["order_id"], line_no)
                signature = (row["product_id"], quantity, price, discount)
                if natural_key in seen:
                    if seen[natural_key] == signature:
                        self.metrics["skipped_rows"] += 1
                    else:
                        raise InvalidRecord("conflicting_duplicate:order_id_line_no")
                    continue
                if row["order_id"] not in self.orders:
                    raise InvalidRecord("invalid_reference:order_id")
                if row["product_id"] not in self.products:
                    raise InvalidRecord("invalid_reference:product_id")
                # Check duplicates BEFORE cancelled/pending status, so repeated events are audited.
                seen[natural_key] = signature
                customer_id, timestamp, status = self.orders[row["order_id"]]
                if status != "completed":
                    self.metrics["skipped_rows"] += 1
                    continue
                customer_key = self.customers[customer_id]
                product_key = self.products[row["product_id"]]
                date_value = timestamp[:10]
                date_obj = datetime.fromisoformat(date_value).date()
                date_key = int(date_obj.strftime("%Y%m%d"))
                net = gross - discount
                existing = self.conn.execute(
                    """SELECT customer_key, product_key, date_key, order_timestamp, quantity,
                       unit_price_cents, gross_amount_cents, discount_cents, net_amount_cents
                       FROM fact_sales_order_line WHERE order_id = ? AND line_no = ?""",
                    natural_key,
                ).fetchone()
                facts = (customer_key, product_key, date_key, timestamp, quantity, price, gross, discount, net)
                if existing is not None:
                    if tuple(existing) == facts:
                        self.metrics["skipped_rows"] += 1
                    else:
                        raise InvalidRecord("conflicting_existing_fact:order_id_line_no")
                    continue
                self.conn.execute(
                    "INSERT OR IGNORE INTO dim_date (date_key, full_date, year, month, day) VALUES (?,?,?,?,?)",
                    (date_key, date_value, date_obj.year, date_obj.month, date_obj.day),
                )
                self.conn.execute(
                    """INSERT INTO fact_sales_order_line
                       (sales_line_key, order_id, line_no, customer_key, product_key, date_key,
                        order_timestamp, quantity, unit_price_cents, gross_amount_cents,
                        discount_cents, net_amount_cents, source_file, source_row_number, ingest_run_id)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (stable_key("fact_sales_order_line", f"{row['order_id']}:{line_no}"),
                     row["order_id"], line_no, *facts, filename, row_number, self.run_id),
                )
                self.metrics["output_rows"] += 1
                if self.fail_after_facts is not None and self.metrics["output_rows"] >= self.fail_after_facts:
                    raise PipelineError("injected_failure_after_fact_insertion")
            except InvalidRecord as exc:
                self.reject(filename, row_number, raw, identifier, str(exc))

    def load_all(self):
        self.load_dimension("customers.csv", "dim_customer", "customer_id", "customer_key", ["customer_name", "city"])
        self.load_dimension("products.csv", "dim_product", "product_id", "product_key", ["product_name", "category"])
        self.load_orders()
        self.load_items()


def run_pipeline(source_dir: Path, db_path: Path, fail_after_facts: int | None = None) -> dict:
    """Execute an entire load. Failed runs roll back targets and retain run audit status."""
    if fail_after_facts is not None and fail_after_facts < 1:
        raise ValueError("fail_after_facts must be at least 1")
    run_id = str(uuid.uuid4())
    conn = connect(Path(db_path))
    try:
        init_database(conn)
        conn.execute("INSERT INTO etl_runs (run_id, started_at, status) VALUES (?, ?, 'RUNNING')",
                     (run_id, utc_now()))
        conn.commit()  # RUNNING record is retained even if the load rolls back.
        loader = Loader(conn, Path(source_dir), run_id, fail_after_facts)
        try:
            conn.execute("BEGIN IMMEDIATE")
            loader.load_all()
            metrics = loader.metrics
            conn.execute(
                """UPDATE etl_runs SET ended_at = ?, status = 'SUCCESS', input_rows = ?,
                   output_rows = ?, rejected_rows = ?, skipped_rows = ? WHERE run_id = ?""",
                (utc_now(), metrics["input_rows"], metrics["output_rows"],
                 metrics["rejected_rows"], metrics["skipped_rows"], run_id),
            )
            conn.commit()
            summary = {"run_id": run_id, "status": "SUCCESS", **metrics}
            LOGGER.info("SUCCESS %s", summary)
            return summary
        except Exception as exc:
            conn.rollback()
            conn.execute(
                """UPDATE etl_runs SET ended_at = ?, status = 'FAILED', error_message = ?,
                   input_rows = ?, output_rows = 0, rejected_rows = 0, skipped_rows = 0
                   WHERE run_id = ?""",
                (utc_now(), f"{type(exc).__name__}: {exc}", loader.metrics["input_rows"], run_id),
            )
            conn.commit()
            LOGGER.error("FAILED run_id=%s reason=%s", run_id, exc)
            raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run transactional CSV-to-SQLite warehouse ETL")
    parser.add_argument("--source", type=Path, default=Path("data/sample_input"))
    parser.add_argument("--db", type=Path, default=Path("output/warehouse.db"))
    parser.add_argument("--fail-after-facts", type=int, default=None, help="Inject a rollback test; omit for normal runs")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s", stream=sys.stdout)
    try:
        summary = run_pipeline(args.source, args.db, args.fail_after_facts)
        print(json.dumps(summary, indent=2))
    except Exception as exc:
        print(f"PIPELINE FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
