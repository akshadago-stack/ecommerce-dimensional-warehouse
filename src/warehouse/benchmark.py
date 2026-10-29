"""Measure fresh isolated ETL loads; never touch the project's warehouse database."""
from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from contextlib import closing
from pathlib import Path

from .generator import generate_dataset
from .pipeline import run_pipeline


def benchmark(source: Path, db: Path, label: str) -> dict:
    """Time ONLY the existing ETL; exclude data generation and JSON writing."""
    start = time.perf_counter()
    run = run_pipeline(source, db)
    duration = time.perf_counter() - start
    # A sqlite3 connection context only manages transactions; it does not close the file.
    with closing(sqlite3.connect(db)) as conn:
        fact_count = conn.execute("SELECT COUNT(*) FROM fact_sales_order_line").fetchone()[0]
        fk_violations = len(conn.execute("PRAGMA foreign_key_check").fetchall())
    size = db.stat().st_size
    return {
        "dataset": label, "source_directory": str(source),
        "source_rows_processed": run["input_rows"],
        "accepted_source_rows_not_rejected_or_skipped": (
            run["input_rows"] - run["rejected_rows"] - run["skipped_rows"]),
        "rejected_source_rows": run["rejected_rows"],
        "skipped_source_rows": run["skipped_rows"],
        "new_completed_order_facts": run["output_rows"],
        "duration_seconds": round(duration, 6),
        "throughput_source_rows_per_second": round(run["input_rows"] / duration, 2) if duration else None,
        "final_fact_rows": fact_count,
        "sqlite_database_bytes": size,
        "foreign_key_violations": fk_violations,
        "status": run["status"],
    }


def run_benchmarks(small: Path, lines: int, seed: int) -> dict:
    if lines < 100000:
        raise ValueError("Phase 5 large benchmark must contain at least 100,000 order lines")
    with tempfile.TemporaryDirectory(prefix="ecommerce_benchmark_") as folder:
        temp = Path(folder)
        large_source = temp / "large_input"
        source_counts = generate_dataset(large_source, lines=lines, seed=seed)
        small_result = benchmark(small, temp / "small.db", "starter")
        large_result = benchmark(large_source, temp / "large.db", "large")
    return {
        "methodology": ("Python time.perf_counter measures ETL only (including SQLite database "
                        "initialization and commit); CSV generation and report writing excluded. "
                        "Both input loads use fresh, separate SQLite databases in a temporary "
                        "directory. Results are machine/environment-specific. No performance "
                        "optimization or before/after comparison was performed."),
        "seed": seed, "requested_large_order_lines": lines,
        "large_source_counts": source_counts,
        "results": [small_result, large_result],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--small-source", type=Path, default=Path("data/sample_input"))
    parser.add_argument("--lines", type=int, default=100000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", type=Path, default=Path("evidence/benchmark_results.json"))
    args = parser.parse_args()
    result = run_benchmarks(args.small_source, args.lines, args.seed)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for item in result["results"]:
        print(f"{item['dataset']}: source={item['source_rows_processed']} "
              f"accepted={item['accepted_source_rows_not_rejected_or_skipped']} "
              f"rejected={item['rejected_source_rows']} facts={item['final_fact_rows']} "
              f"duration={item['duration_seconds']:.6f}s "
              f"throughput={item['throughput_source_rows_per_second']} records/s "
              f"database_bytes={item['sqlite_database_bytes']}")
    print(f"Report saved: {args.out}")


if __name__ == "__main__":
    main()
