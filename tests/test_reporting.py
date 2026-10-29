"""Phase 5 reporting checks; all databases and source mutations use tmp_path."""
from __future__ import annotations

import sqlite3
from pathlib import Path

from src.warehouse.analytics import run_analytics
from src.warehouse.benchmark import benchmark
from src.warehouse.pipeline import run_pipeline
from src.warehouse.reconcile import reconcile
from tests.conftest import append, read_rows, rewrite


def test_analytics_uses_real_star_schema(sample):
    source, db = sample
    run_pipeline(source, db)
    output = run_analytics(db, Path("sql/analytics.sql"))
    for label in ("revenue_by_category", "monthly_revenue_and_completed_orders",
                  "top_10_customers_by_revenue", "top_10_products_by_quantity_and_revenue",
                  "daily_sales_trend"):
        assert f"== {label} (" in output
    with sqlite3.connect(db) as conn:
        total = conn.execute("SELECT SUM(net_amount_cents) FROM fact_sales_order_line").fetchone()[0]
    category_section = output.split("== revenue_by_category (", 1)[1].split("== monthly_revenue", 1)[0]
    amounts = [int(line.split(" | ")[-1]) for line in category_section.splitlines()
               if line.startswith(("Home |", "Electronics |", "Books |"))]
    assert sum(amounts) == total


def test_reconciliation_and_disposable_idempotency_replay(sample):
    source, db = sample
    run_pipeline(source, db)
    with sqlite3.connect(db) as conn:
        before_runs = conn.execute("SELECT COUNT(*) FROM etl_runs").fetchone()[0]
    report = reconcile(source, db, replay=True)
    assert report["summary"]["failed"] == 0
    assert report["idempotency_copy_replay"]["same_business_snapshot"]
    with sqlite3.connect(db) as conn:
        after_runs = conn.execute("SELECT COUNT(*) FROM etl_runs").fetchone()[0]
    assert before_runs == after_runs  # real db must remain untouched


def test_reconciliation_counts_invalid_duplicate_and_cancelled_once(sample):
    source, db = sample
    orders = read_rows(source / "orders.csv")
    orders[0]["order_status"] = "cancelled"
    rewrite(source / "orders.csv", orders)
    items = read_rows(source / "order_items.csv")
    append(source / "order_items.csv", items[0])  # identical duplicate
    bad = items[0].copy()
    bad["line_no"] = "99"
    bad["quantity"] = "0"
    append(source / "order_items.csv", bad)
    run_pipeline(source, db)
    report = reconcile(source, db)
    assert report["summary"]["failed"] == 0, report["validations"]
    assert report["source_classification"]["rejected"] == 1
    assert report["source_classification"]["exact_duplicate"] == 1
    assert report["source_classification"]["noncompleted_line"] == 3
    assert report["source_classification"]["completed_line"] == 27


def test_reconciliation_fails_on_actual_corrupted_financial_data(sample):
    source, db = sample
    run_pipeline(source, db)
    with sqlite3.connect(db) as conn:
        conn.execute("UPDATE fact_sales_order_line SET net_amount_cents = net_amount_cents + 1 "
                     "WHERE order_id = 'O0000001' AND line_no = 1")
    report = reconcile(source, db)
    results = {row["validation_name"]: row["status"] for row in report["validations"]}
    assert results["source_vs_fact_net_amount_cents"] == "FAIL"
    assert results["invalid_financial_equations"] == "FAIL"


def test_benchmark_uses_separate_database(sample):
    source, _ = sample
    db = source.parent / "temporary_benchmark.db"
    result = benchmark(source, db, "isolated")
    assert result["source_rows_processed"] == 46
    assert result["final_fact_rows"] == 30
    assert result["duration_seconds"] >= 0
    assert result["foreign_key_violations"] == 0
