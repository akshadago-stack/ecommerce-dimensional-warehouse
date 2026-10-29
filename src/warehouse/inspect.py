"""Inspect and export the finished Phase 3/4 warehouse without a SQLite CLI."""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path

TABLES = ("dim_customer", "dim_product", "dim_date", "fact_sales_order_line", "etl_runs", "rejected_records")


def inspect_db(db: Path, export_dir: Path | None = None) -> dict:
    if not db.is_file():
        raise FileNotFoundError(f"Database not found: {db} (run the pipeline first)")
    with sqlite3.connect(db) as con:
        con.execute("PRAGMA foreign_keys=ON")
        summary = {table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in TABLES}
        summary["total_net_revenue_cents"] = con.execute(
            "SELECT COALESCE(SUM(net_amount_cents),0) FROM fact_sales_order_line"
        ).fetchone()[0]
        summary["foreign_key_violations"] = len(con.execute("PRAGMA foreign_key_check").fetchall())
        summary["recent_run_statuses"] = [row[0] for row in con.execute(
            "SELECT status FROM etl_runs ORDER BY rowid DESC LIMIT 5"
        ).fetchall()]
        if export_dir:
            export_dir.mkdir(parents=True, exist_ok=True)
            for table in TABLES:
                cursor = con.execute(f"SELECT * FROM {table} ORDER BY rowid")
                with (export_dir / f"{table}.csv").open("w", newline="", encoding="utf-8") as fh:
                    writer = csv.writer(fh)
                    writer.writerow([column[0] for column in cursor.description])
                    writer.writerows(cursor)
    return summary


def main():
    parser = argparse.ArgumentParser(description="Inspect counts, FK integrity and export warehouse tables")
    parser.add_argument("--db", type=Path, default=Path("output/warehouse.db"))
    parser.add_argument("--export-dir", type=Path)
    args = parser.parse_args()
    print(json.dumps(inspect_db(args.db, args.export_dir), indent=2))


if __name__ == "__main__":
    main()
