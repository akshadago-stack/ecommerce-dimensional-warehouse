"""Independent source-to-warehouse reconciliation for full snapshots and identical reruns.

A source row is classified exactly once as accepted_reference, completed_line,
noncompleted_line, exact_duplicate, or rejected. Conflicting duplicates are a
subset of rejected, never counted a second time. Existing warehouse rows reused
on reruns are not new facts; source classifications still describe the file.
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import tempfile
from collections import Counter
from contextlib import closing
from pathlib import Path

from .pipeline import SCHEMAS, InvalidRecord, normalize_timestamp, run_pipeline, validate_int


def source_records(source: Path, filename: str):
    with (source / filename).open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle, strict=True)
        if reader.fieldnames != SCHEMAS[filename]:
            raise ValueError(f"schema_mismatch:{filename}: {reader.fieldnames}")
        for raw in reader:
            # Align physical row numbering with the existing ETL.
            yield reader.line_num, raw


def clean(raw: dict) -> dict[str, str]:
    if None in raw or any(value is None for value in raw.values()):
        raise InvalidRecord("malformed_row:column_count_mismatch")
    values = {key: value.strip() for key, value in raw.items()}
    for key, value in values.items():
        if not value:
            raise InvalidRecord(f"missing_required_field:{key}")
    return values


def classify_source(source: Path) -> dict:
    """Calculate expectations without reading facts or run-audit counters."""
    category = Counter()
    per_file = Counter()
    expected_rejects = []
    customers, products, orders = {}, {}, {}
    expected_facts = {}
    dup_count = 0

    def rejected(filename, line_no, exc):
        category["rejected"] += 1
        expected_rejects.append((filename, line_no, str(exc)))

    for filename, id_col, attrs, target in [
        ("customers.csv", "customer_id", ("customer_name", "city"), customers),
        ("products.csv", "product_id", ("product_name", "category"), products),
    ]:
        for row_no, raw in source_records(source, filename):
            per_file[filename] += 1
            try:
                row = clean(raw)
                key = row[id_col]
                value = tuple(row[attr] for attr in attrs)
                if key in target:
                    dup_count += 1
                    if target[key] != value:
                        raise InvalidRecord(f"conflicting_duplicate:{id_col}")
                    category["exact_duplicate"] += 1
                    continue
                target[key] = value
                category["accepted_reference"] += 1
            except InvalidRecord as exc:
                rejected(filename, row_no, exc)

    for row_no, raw in source_records(source, "orders.csv"):
        filename = "orders.csv"
        per_file[filename] += 1
        try:
            row = clean(raw)
            timestamp = normalize_timestamp(row["order_timestamp"])
            status = row["order_status"].lower()
            if status not in {"completed", "cancelled", "pending"}:
                raise InvalidRecord("invalid_order_status")
            if row["customer_id"] not in customers:
                raise InvalidRecord("invalid_reference:customer_id")
            signature = (row["customer_id"], timestamp, status)
            key = row["order_id"]
            if key in orders:
                dup_count += 1
                if orders[key] != signature:
                    raise InvalidRecord("conflicting_duplicate:order_id")
                category["exact_duplicate"] += 1
                continue
            orders[key] = signature
            category["accepted_reference"] += 1
        except InvalidRecord as exc:
            rejected(filename, row_no, exc)

    seen_items = {}
    for row_no, raw in source_records(source, "order_items.csv"):
        filename = "order_items.csv"
        per_file[filename] += 1
        try:
            row = clean(raw)
            line = validate_int(row["line_no"], "line_no", 1)
            qty = validate_int(row["quantity"], "quantity", 1)
            unit = validate_int(row["unit_price_cents"], "unit_price_cents", 0)
            discount = validate_int(row["discount_cents"], "discount_cents", 0)
            gross = qty * unit
            if discount > gross:
                raise InvalidRecord("discount_exceeds_gross")
            key = (row["order_id"], line)
            signature = (row["product_id"], qty, unit, discount)
            if key in seen_items:
                dup_count += 1
                if seen_items[key] != signature:
                    raise InvalidRecord("conflicting_duplicate:order_id_line_no")
                category["exact_duplicate"] += 1
                continue
            if row["order_id"] not in orders:
                raise InvalidRecord("invalid_reference:order_id")
            if row["product_id"] not in products:
                raise InvalidRecord("invalid_reference:product_id")
            seen_items[key] = signature
            customer, ts, status = orders[row["order_id"]]
            if status != "completed":
                category["noncompleted_line"] += 1
                continue
            expected_facts[key] = {
                "customer_id": customer, "product_id": row["product_id"],
                "date_key": int(ts[:10].replace("-", "")), "order_timestamp": ts,
                "quantity": qty, "unit_price_cents": unit,
                "gross_amount_cents": gross, "discount_cents": discount,
                "net_amount_cents": gross - discount,
            }
            category["completed_line"] += 1
        except InvalidRecord as exc:
            rejected(filename, row_no, exc)

    return {
        "source_rows": sum(per_file.values()), "rows_by_file": dict(per_file),
        "classification": {key: category[key] for key in (
            "accepted_reference", "completed_line", "noncompleted_line",
            "exact_duplicate", "rejected")},
        "duplicate_source_rows": dup_count,
        "conflicting_duplicate_rows": sum("conflicting_duplicate" in reason for _, _, reason in expected_rejects),
        "expected_rejects": expected_rejects, "facts": expected_facts,
        "noncompleted_order_ids": [key for key, (_, _, status) in orders.items() if status != "completed"],
    }


def check(name: str, expected, actual) -> dict:
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        difference = actual - expected
    else:
        difference = 0 if expected == actual else 1
    return {"validation_name": name, "expected_value": expected,
            "actual_value": actual, "difference": difference,
            "status": "PASS" if difference == 0 else "FAIL"}


def business_snapshot(conn: sqlite3.Connection) -> dict:
    result = {}
    for name in ("dim_customer", "dim_product", "dim_date", "fact_sales_order_line"):
        result[name] = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    result["gross_cents"], result["discount_cents"], result["net_cents"] = conn.execute(
        "SELECT COALESCE(SUM(gross_amount_cents),0), COALESCE(SUM(discount_cents),0), "
        "COALESCE(SUM(net_amount_cents),0) FROM fact_sales_order_line").fetchone()
    return result


def verify_replay(source: Path, existing_db: Path) -> dict:
    """Replay into a disposable SQLite backup, NEVER the supplied warehouse."""
    with tempfile.TemporaryDirectory(prefix="warehouse_reconcile_replay_") as folder:
        db_copy = Path(folder) / "replay.db"
        with closing(sqlite3.connect(existing_db)) as original, closing(sqlite3.connect(db_copy)) as target:
            original.backup(target)
        with closing(sqlite3.connect(db_copy)) as conn:
            before = business_snapshot(conn)
        summary = run_pipeline(source, db_copy)
        with closing(sqlite3.connect(db_copy)) as conn:
            after = business_snapshot(conn)
        return {"before": before, "after": after, "run": summary,
                "same_business_snapshot": before == after and summary["output_rows"] == 0}


def reconcile(source: Path, db: Path, run_id: str | None = None,
              replay: bool = False) -> dict:
    if not db.is_file():
        raise FileNotFoundError(f"Database does not exist: {db}")
    expected = classify_source(source)
    facts = expected["facts"]
    checks = []
    with closing(sqlite3.connect(f"file:{db.resolve().as_posix()}?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        if run_id:
            run = conn.execute("SELECT * FROM etl_runs WHERE run_id=?", (run_id,)).fetchone()
        else:
            run = conn.execute("SELECT * FROM etl_runs ORDER BY rowid DESC LIMIT 1").fetchone()
        if run is None:
            raise ValueError("No ETL run found")
        run = dict(run)
        checks.append(check("selected_run_success", "SUCCESS", run["status"]))
        checks.append(check("source_input_row_count", expected["source_rows"], run["input_rows"]))
        checks.append(check("source_rejected_row_count", expected["classification"]["rejected"], run["rejected_rows"]))
        checks.append(check("source_row_classification_balance", expected["source_rows"],
                            sum(expected["classification"].values())))
        expected_rejects = {(f, n) for f, n, _ in expected["expected_rejects"]}
        reject_rows = conn.execute("SELECT source_file, source_row_number, reject_reason "
                                   "FROM rejected_records WHERE run_id=?", (run["run_id"],)).fetchall()
        actual_rejects = {(r[0], r[1]) for r in reject_rows}
        checks.append(check("rejected_source_row_identity_match", sorted(expected_rejects), sorted(actual_rejects)))
        checks.append(check("rejection_reason_match", sorted(expected["expected_rejects"]),
                            sorted((r[0], r[1], r[2]) for r in reject_rows)))
        checks.append(check("conflicting_duplicate_rejections",
                            expected["conflicting_duplicate_rows"],
                            sum("conflicting_duplicate" in r[2] for r in reject_rows)))
        checks.append(check("audit_new_facts_vs_ingest_lineage", run["output_rows"],
                            conn.execute("SELECT COUNT(*) FROM fact_sales_order_line WHERE ingest_run_id=?",
                                         (run["run_id"],)).fetchone()[0]))
        # Rejected/duplicate/noncompleted are mutually exclusive in source classification.
        # On a rerun, reused facts and dimensions ALSO increment audit skipped_rows,
        # so skip audit need not equal source's noncompleted + exact duplicate count.
        minimum_skips = (expected["classification"]["noncompleted_line"] +
                         expected["classification"]["exact_duplicate"])
        checks.append(check("audit_skips_at_least_source_exclusions", True,
                            run["skipped_rows"] >= minimum_skips))

        actual_rows = conn.execute("""SELECT f.order_id, f.line_no, c.customer_id, p.product_id,
                            f.date_key, f.order_timestamp, f.quantity, f.unit_price_cents,
                            f.gross_amount_cents, f.discount_cents, f.net_amount_cents
                            FROM fact_sales_order_line f
                            JOIN dim_customer c ON c.customer_key=f.customer_key
                            JOIN dim_product p ON p.product_key=f.product_key""").fetchall()
        actual = {(r[0], r[1]): dict(zip(("customer_id", "product_id", "date_key",
                     "order_timestamp", "quantity", "unit_price_cents", "gross_amount_cents",
                     "discount_cents", "net_amount_cents"), r[2:])) for r in actual_rows}
        checks.append(check("completed_order_fact_count", len(facts), len(actual)))
        checks.append(check("source_fact_keys_missing_from_target", 0,
                            len(facts.keys() - actual.keys())))
        checks.append(check("unexpected_target_fact_keys", 0,
                            len(actual.keys() - facts.keys())))
        mismatched = sum(facts[k] != actual.get(k) for k in facts)
        checks.append(check("source_to_fact_row_values_mismatched", 0, mismatched))
        for name, column in (("quantity", "quantity"), ("gross_amount_cents", "gross_amount_cents"),
                             ("discount_cents", "discount_cents"), ("net_amount_cents", "net_amount_cents")):
            checks.append(check(f"source_vs_fact_{name}", sum(row[column] for row in facts.values()),
                                conn.execute(f"SELECT COALESCE(SUM({column}),0) "
                                             "FROM fact_sales_order_line").fetchone()[0]))
        checks.append(check("fact_foreign_key_violations", 0,
                            len(conn.execute("PRAGMA foreign_key_check").fetchall())))
        checks.append(check("duplicate_fact_grain_keys", 0, conn.execute(
            "SELECT COUNT(*) FROM (SELECT order_id,line_no FROM fact_sales_order_line "
            "GROUP BY order_id,line_no HAVING COUNT(*)>1)").fetchone()[0]))
        checks.append(check("invalid_financial_equations", 0, conn.execute(
            "SELECT COUNT(*) FROM fact_sales_order_line WHERE "
            "gross_amount_cents != quantity*unit_price_cents "
            "OR net_amount_cents != gross_amount_cents-discount_cents "
            "OR discount_cents>gross_amount_cents").fetchone()[0]))
        if expected["noncompleted_order_ids"]:
            placeholders = ",".join("?" for _ in expected["noncompleted_order_ids"])
            excluded = conn.execute(f"SELECT COUNT(*) FROM fact_sales_order_line "
                                    f"WHERE order_id IN ({placeholders})",
                                    expected["noncompleted_order_ids"]).fetchone()[0]
        else:
            excluded = 0
        checks.append(check("cancelled_pending_order_fact_count", 0, excluded))

    replay_detail = None
    if replay:
        replay_detail = verify_replay(source, db)
        checks.append(check("idempotent_replay_business_snapshot", True,
                            replay_detail["same_business_snapshot"]))

    return {
        "source_directory": str(source), "warehouse_database": str(db),
        "selected_run_id": run["run_id"], "source_rows_by_file": expected["rows_by_file"],
        "source_classification": expected["classification"],
        "duplicate_source_rows_including_conflicts": expected["duplicate_source_rows"],
        "conflicting_duplicate_rows_counted_as_rejected": expected["conflicting_duplicate_rows"],
        "run_audit": {k: run[k] for k in ("status", "input_rows", "output_rows",
                       "rejected_rows", "skipped_rows", "error_message")},
        "counting_rule": ("Each physical source row is classified once: accepted_reference "
                          "(customers/products/order headers), completed_line, noncompleted_line, "
                          "exact_duplicate, or rejected. Conflicting duplicates are included in "
                          "rejected, not counted twice. ETL skipped_rows additionally includes "
                          "already-loaded dimension/fact records on reruns; output_rows counts "
                          "new fact rows, not accepted source rows."),
        "scope": ("Reconciles a complete source snapshot against a warehouse containing only "
                  "that snapshot (or an identical rerun). Historical mixes and conflicting "
                  "source updates require separate controlled reconciliation."),
        "validations": checks,
        "summary": {"passed": sum(c["status"] == "PASS" for c in checks),
                    "failed": sum(c["status"] == "FAIL" for c in checks)},
        "idempotency_copy_replay": replay_detail,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("data/sample_input"))
    parser.add_argument("--db", type=Path, default=Path("output/warehouse.db"))
    parser.add_argument("--run-id", help="Audit run to compare (default: latest)")
    parser.add_argument("--verify-rerun", action="store_true", help="Replay into a disposable database copy")
    parser.add_argument("--out", type=Path, default=Path("evidence/reconciliation_results.json"))
    args = parser.parse_args()
    result = reconcile(args.source, args.db, args.run_id, args.verify_rerun)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for row in result["validations"]:
        print(f"{row['status']:<4} {row['validation_name']}: expected={row['expected_value']} actual={row['actual_value']}")
    print(f"{result['summary']} -> {args.out}")
    raise SystemExit(1 if result["summary"]["failed"] else 0)


if __name__ == "__main__":
    main()
