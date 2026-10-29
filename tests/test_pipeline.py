from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import pytest

from src.warehouse.generator import generate_dataset
from src.warehouse.pipeline import PipelineError, run_pipeline, stable_key
from tests.conftest import append, read_rows, rewrite


def dbquery(db: Path, sql: str, params=()):
    with sqlite3.connect(db) as con:
        con.execute("PRAGMA foreign_keys=ON")
        return con.execute(sql, params).fetchall()


def count(db: Path, table: str) -> int:
    return dbquery(db, f"SELECT COUNT(*) FROM {table}")[0][0]


def expected_completed_items(source: Path) -> int:
    statuses = {r["order_id"]: r["order_status"] for r in read_rows(source / "orders.csv")}
    return sum(statuses[r["order_id"]] == "completed" for r in read_rows(source / "order_items.csv"))


def test_happy_path_and_financial_reconciliation(sample):
    source, db = sample
    summary = run_pipeline(source, db)
    assert summary["status"] == "SUCCESS"
    assert count(db, "dim_customer") == len(read_rows(source / "customers.csv"))
    assert count(db, "dim_product") == len(read_rows(source / "products.csv"))
    assert count(db, "fact_sales_order_line") == expected_completed_items(source)
    assert summary["output_rows"] == count(db, "fact_sales_order_line")
    assert summary["rejected_rows"] == 0
    amounts = dbquery(db, "SELECT quantity, unit_price_cents, gross_amount_cents, discount_cents, net_amount_cents FROM fact_sales_order_line")
    assert all(q * unit == gross and gross - discount == net for q, unit, gross, discount, net in amounts)
    assert dbquery(db, "PRAGMA foreign_key_check") == []
    completed = {r["order_id"] for r in read_rows(source / "orders.csv") if r["order_status"] == "completed"}
    expected_total = sum(int(r["quantity"]) * int(r["unit_price_cents"]) - int(r["discount_cents"])
                         for r in read_rows(source / "order_items.csv") if r["order_id"] in completed)
    assert dbquery(db, "SELECT SUM(net_amount_cents) FROM fact_sales_order_line")[0][0] == expected_total


def test_empty_batch(tmp_path):
    source, db = tmp_path / "empty", tmp_path / "empty.db"
    assert generate_dataset(source, lines=0) == {"customers": 0, "products": 0, "orders": 0, "order_items": 0}
    result = run_pipeline(source, db)
    assert result["input_rows"] == result["output_rows"] == result["rejected_rows"] == 0
    assert count(db, "fact_sales_order_line") == 0
    assert count(db, "etl_runs") == 1


@pytest.mark.parametrize("bad_row", [
    "O0000001,31,P0001,2,100\n",          # missing last column
    "O0000001,31,P0001,2,100,0,EXTRA\n",  # extra last column
])
def test_malformed_row_quarantined(sample, bad_row):
    source, db = sample
    with (source / "order_items.csv").open("a", encoding="utf-8") as f:
        f.write(bad_row)
    result = run_pipeline(source, db)
    assert result["rejected_rows"] == 1
    reason = dbquery(db, "SELECT reject_reason FROM rejected_records")[0][0]
    assert "malformed_row" in reason


def test_missing_field_and_raw_lineage(sample):
    source, db = sample
    customers = read_rows(source / "customers.csv")
    customers[0]["customer_name"] = ""
    rewrite(source / "customers.csv", customers)
    result = run_pipeline(source, db)
    assert result["rejected_rows"] >= 1
    row = dbquery(db, "SELECT run_id, source_file, source_row_number, source_id, raw_json, reject_reason FROM rejected_records WHERE source_file='customers.csv'")[0]
    assert row[0] == result["run_id"]
    assert row[1] == "customers.csv" and row[2] == 2
    assert row[3] == "C0001" and json.loads(row[4])["customer_name"] == ""
    assert "missing_required_field" in row[5]


def test_exact_duplicates_skipped(sample):
    source, db = sample
    append(source / "customers.csv", read_rows(source / "customers.csv")[0])
    append(source / "order_items.csv", read_rows(source / "order_items.csv")[0])
    result = run_pipeline(source, db)
    assert result["skipped_rows"] >= 2
    assert result["rejected_rows"] == 0
    assert count(db, "fact_sales_order_line") == expected_completed_items(source) - 1


def test_conflicting_duplicate_order_line_rejected(sample):
    source, db = sample
    new_line = read_rows(source / "order_items.csv")[0].copy()
    new_line["quantity"] = str(int(new_line["quantity"]) + 1)
    append(source / "order_items.csv", new_line)
    result = run_pipeline(source, db)
    assert result["rejected_rows"] == 1
    assert "conflicting_duplicate" in dbquery(db, "SELECT reject_reason FROM rejected_records")[0][0]


@pytest.mark.parametrize("new_header", ["customer_id,name,city", "customer_id,customer_name,city,unexpected_column"])
def test_unexpected_schema_change_fails_atomically(sample, new_header):
    source, db = sample
    rows = (source / "customers.csv").read_text(encoding="utf-8").splitlines()
    (source / "customers.csv").write_text(new_header + "\n" + "\n".join(rows[1:]) + "\n", encoding="utf-8")
    with pytest.raises(PipelineError, match="schema_mismatch"):
        run_pipeline(source, db)
    assert count(db, "fact_sales_order_line") == 0
    assert count(db, "dim_customer") == 0
    assert dbquery(db, "SELECT status, error_message FROM etl_runs")[0][0] == "FAILED"


def test_invalid_customer_reference_rejects_order_and_item(sample):
    source, db = sample
    append(source / "orders.csv", {"order_id": "O_BAD", "customer_id": "C_UNKNOWN", "order_timestamp": "2026-01-05T12:00:00+00:00", "order_status": "completed"})
    append(source / "order_items.csv", {"order_id": "O_BAD", "line_no": 1, "product_id": "P0001", "quantity": 2, "unit_price_cents": 100, "discount_cents": 0})
    result = run_pipeline(source, db)
    reasons = [r[0] for r in dbquery(db, "SELECT reject_reason FROM rejected_records")]
    assert result["rejected_rows"] == 2
    assert "invalid_reference:customer_id" in reasons
    assert "invalid_reference:order_id" in reasons


def test_invalid_product_reference(sample):
    source, db = sample
    append(source / "order_items.csv", {"order_id": "O0000001", "line_no": 12, "product_id": "P_UNKNOWN", "quantity": 2, "unit_price_cents": 100, "discount_cents": 0})
    result = run_pipeline(source, db)
    assert result["rejected_rows"] == 1
    assert dbquery(db, "SELECT reject_reason FROM rejected_records")[0][0] == "invalid_reference:product_id"


@pytest.mark.parametrize("quantity,price,discount,expected", [
    ("0", "100", "0", "out_of_range:quantity"),
    ("-3", "100", "0", "out_of_range:quantity"),
    ("2", "100", "201", "discount_exceeds_gross"),
    ("2", "-1", "0", "out_of_range:unit_price_cents"),
])
def test_domain_invalid_quantities_and_discounts(sample, quantity, price, discount, expected):
    source, db = sample
    append(source / "order_items.csv", {"order_id": "O0000001", "line_no": 12, "product_id": "P0001", "quantity": quantity, "unit_price_cents": price, "discount_cents": discount})
    result = run_pipeline(source, db)
    assert result["rejected_rows"] == 1
    assert expected in dbquery(db, "SELECT reject_reason FROM rejected_records")[0][0]


def test_zero_price_zero_discount_is_valid_boundary(sample):
    source, db = sample
    append(source / "order_items.csv", {"order_id": "O0000001", "line_no": 12, "product_id": "P0001", "quantity": 1, "unit_price_cents": 0, "discount_cents": 0})
    result = run_pipeline(source, db)
    assert result["rejected_rows"] == 0
    assert dbquery(db, "SELECT net_amount_cents FROM fact_sales_order_line WHERE line_no=12") == [(0,)]


def test_cancelled_pending_excluded_not_rejected(sample):
    source, db = sample
    rows = read_rows(source / "orders.csv")
    rows[0]["order_status"] = "cancelled"
    rows[1]["order_status"] = "pending"
    rewrite(source / "orders.csv", rows)
    result = run_pipeline(source, db)
    assert result["rejected_rows"] == 0
    assert count(db, "fact_sales_order_line") == expected_completed_items(source)
    assert not dbquery(db, "SELECT 1 FROM fact_sales_order_line WHERE order_id IN ('O0000001','O0000002')")


def test_late_arriving_order_on_second_run(sample):
    source, db = sample
    run_pipeline(source, db)
    original = count(db, "fact_sales_order_line")
    append(source / "orders.csv", {"order_id": "O_LATE", "customer_id": "C0001", "order_timestamp": "2020-03-02T10:30:00+00:00", "order_status": "completed"})
    append(source / "order_items.csv", {"order_id": "O_LATE", "line_no": 1, "product_id": "P0001", "quantity": 2, "unit_price_cents": 1000, "discount_cents": 500})
    result = run_pipeline(source, db)
    assert result["output_rows"] == 1
    assert count(db, "fact_sales_order_line") == original + 1
    assert dbquery(db, "SELECT date_key FROM fact_sales_order_line WHERE order_id='O_LATE'") == [(20200302,)]


def test_idempotent_rerun_does_not_duplicate_facts_or_dimensions(sample):
    source, db = sample
    first = run_pipeline(source, db)
    counts = [count(db, t) for t in ("dim_customer", "dim_product", "dim_date", "fact_sales_order_line")]
    second = run_pipeline(source, db)
    assert first["output_rows"] > 0
    assert second["output_rows"] == 0
    assert second["rejected_rows"] == 0
    assert counts == [count(db, t) for t in ("dim_customer", "dim_product", "dim_date", "fact_sales_order_line")]
    assert count(db, "etl_runs") == 2


def test_failure_rollback_and_successful_retry(sample):
    source, db = sample
    with pytest.raises(PipelineError, match="injected_failure"):
        run_pipeline(source, db, fail_after_facts=2)
    assert count(db, "fact_sales_order_line") == 0
    assert count(db, "dim_customer") == 0
    assert count(db, "dim_product") == 0
    assert dbquery(db, "SELECT status, output_rows FROM etl_runs")[0] == ("FAILED", 0)
    result = run_pipeline(source, db)
    assert result["output_rows"] == expected_completed_items(source)
    assert dbquery(db, "SELECT status FROM etl_runs ORDER BY started_at, rowid") == [("FAILED",), ("SUCCESS",)]


def test_changed_existing_fact_rejected_not_overwritten(sample):
    source, db = sample
    run_pipeline(source, db)
    before = dbquery(db, "SELECT net_amount_cents FROM fact_sales_order_line WHERE order_id='O0000001' AND line_no=1")[0][0]
    rows = read_rows(source / "order_items.csv")
    rows[0]["unit_price_cents"] = str(int(rows[0]["unit_price_cents"]) + 100)
    rewrite(source / "order_items.csv", rows)
    summary = run_pipeline(source, db)
    assert summary["rejected_rows"] == 1
    assert dbquery(db, "SELECT net_amount_cents FROM fact_sales_order_line WHERE order_id='O0000001' AND line_no=1")[0][0] == before
    assert "conflicting_existing_fact" in dbquery(db, "SELECT reject_reason FROM rejected_records")[0][0]


def test_changed_reference_snapshot_is_rejected(sample):
    source, db = sample
    run_pipeline(source, db)
    customers = read_rows(source / "customers.csv")
    customers[0]["city"] = "Changed City"
    rewrite(source / "customers.csv", customers)
    summary = run_pipeline(source, db)
    assert summary["rejected_rows"] == 1
    assert dbquery(db, "SELECT city FROM dim_customer WHERE customer_id='C0001'") == [("Indore",)]
    assert "changed_reference_snapshot" in dbquery(db, "SELECT reject_reason FROM rejected_records")[0][0]


def test_invalid_timestamp_requires_timezone(sample):
    source, db = sample
    orders = read_rows(source / "orders.csv")
    orders[0]["order_timestamp"] = "2026-01-05T10:00:00"
    rewrite(source / "orders.csv", orders)
    summary = run_pipeline(source, db)
    assert summary["rejected_rows"] >= 1
    assert "timestamp_requires_timezone" in [r[0] for r in dbquery(db, "SELECT reject_reason FROM rejected_records")]


def test_loaded_order_change_to_cancelled_is_flagged(sample):
    source, db = sample
    run_pipeline(source, db)
    orders = read_rows(source / "orders.csv")
    orders[0]["order_status"] = "cancelled"
    rewrite(source / "orders.csv", orders)
    summary = run_pipeline(source, db)
    assert summary["rejected_rows"] >= 1
    assert "previously_loaded_order_changed_status:requires_backfill" in [r[0] for r in dbquery(db, "SELECT reject_reason FROM rejected_records")]
    assert count(db, "fact_sales_order_line") == expected_completed_items(source) + 3


def test_stable_surrogate_keys():
    assert stable_key("dim_product", "P0001") == stable_key("dim_product", "P0001")
    assert stable_key("dim_customer", "P0001") != stable_key("dim_product", "P0001")
    assert 0 <= stable_key("dim_product", "P0001") < 2**63
