# E-commerce Dimensional Data Warehouse — Phase 5 Complete

A reproducible, fully local **Python 3 + SQLite dimensional warehouse** built from four transactional CSV sources. Designed for a 72-hour Data Engineering take-home at the 0–3-year experience level. Includes deterministic data generation, validation/quarantine, transaction-safe ETL, stable surrogate keys, five analytical SQL queries, source-to-target reconciliation, isolated performance benchmarking, and pytest tests.

**Tools:** Python 3.10+, built-in `csv`, `sqlite3`, `hashlib`, `datetime`, `time`, and other standard-library modules; `pytest` for tests. No Spark, Airflow, Docker, cloud, or frontend.

## 1. Business problem and objective

Transactional order data is useful for operations but inconvenient for consistent sales analytics. This project converts customers, products, orders, and order lines into a small **star schema** so an analyst can measure completed-order revenue by customer, product, category, and date. Invalid rows remain traceable in a quarantine table; run logs record successes and failures. All financial arithmetic uses **integer cents**, not floating-point money.

## 2. Architecture and project files

```text
customers.csv ─┐
products.csv ──┤  Python CSV reader / schema & row validation
orders.csv ────┤       ├─ invalid rows → rejected_records
order_items.csv┘       └─ valid rows → SQLite transaction
                                  ├─ dim_customer
                                  ├─ dim_product
                                  ├─ dim_date
                                  └─ fact_sales_order_line
                              each execution → etl_runs
warehouse.db → named analytical SQL / reconciliation / evidence
```

| Path | Purpose |
|---|---|
| `src/warehouse/generator.py` | Seeded clean CSV generator (`--lines` controls order-line count) |
| `src/warehouse/pipeline.py` | Existing validator/ETL, quarantine, idempotency, transaction and run audit |
| `src/warehouse/inspect.py` | Counts, foreign-key check and CSV exports |
| `src/warehouse/analytics.py` | Executes named SQL statements as a read-only report |
| `src/warehouse/reconcile.py` | Independently classifies source records and compares source/target metrics; JSON output |
| `src/warehouse/benchmark.py` | Times fresh ETL runs with `time.perf_counter()` on temporary databases |
| `sql/schema.sql` | Original six-table schema, unchanged in Phase 5 |
| `sql/sample_queries.sql` | Existing Phase 4 illustrative examples, retained unchanged |
| `sql/analytics.sql` | Five required final analytical queries |
| `tests/test_pipeline.py` | Original Phase 4 ETL tests, unchanged |
| `tests/test_reporting.py` | Five Phase 5 reporting/reconciliation tests |
| `tests/test_replay_connection_cleanup.py` | Two regression tests for closing replay SQLite connections before temporary cleanup |
| `data/sample_input/` | Supplied deterministic 30-line starter source data |
| `data/test_fixtures/` | Intentionally invalid inputs, separate from clean data |
| `output/warehouse.db` | Supplied sample SQLite database (two successful runs) |
| `evidence/` | Actual test, analytics, reconciliation, benchmark and replay evidence |
| `DESIGN_NOTES.md` | Grain, trade-offs, recovery, limitations, AI assistance |

## 3. Data dictionary and relationships

### Input contract (all fields required, headers match exactly)

| CSV source | Columns / SQLite-compatible types | Business rule |
|---|---|---|
| `customers.csv` | `customer_id` TEXT, `customer_name` TEXT, `city` TEXT | Customer ID unique; all nonblank |
| `products.csv` | `product_id` TEXT, `product_name` TEXT, `category` TEXT | Product ID unique; all nonblank |
| `orders.csv` | `order_id` TEXT, `customer_id` TEXT, `order_timestamp` TEXT, `order_status` TEXT | Unique order ID; valid customer; timezone-aware ISO 8601; completed/cancelled/pending |
| `order_items.csv` | `order_id` TEXT, `line_no` INT, `product_id` TEXT, `quantity` INT, `unit_price_cents` INT, `discount_cents` INT | Composite key `(order_id,line_no)`; valid order/product; line ≥1; quantity ≥1; price ≥0; discount between 0 and gross |

### Warehouse star schema (actual Phase 2 schema)

| Table | Key(s) and foreign keys | Attributes / measures |
|---|---|---|
| `dim_customer` | `customer_key` INTEGER PK; `customer_id` UNIQUE | customer_name, city |
| `dim_product` | `product_key` INTEGER PK; `product_id` UNIQUE | product_name, category |
| `dim_date` | `date_key` INTEGER PK (YYYYMMDD); `full_date` UNIQUE | year, month, day |
| `fact_sales_order_line` | `sales_line_key` INTEGER PK; `(order_id,line_no)` UNIQUE; FK `customer_key`, `product_key`, `date_key` | order_timestamp, quantity, unit_price_cents, gross_amount_cents, discount_cents, net_amount_cents, source_file, source_row_number, ingest_run_id |
| `etl_runs` | `run_id` TEXT PK | started_at, ended_at, status, input_rows, output_rows, rejected_rows, skipped_rows, error_message |
| `rejected_records` | `reject_id` PK, `run_id` FK | source_file, source_row_number, source_id, raw_json, reject_reason, rejected_at |

**Fact grain:** one line from a *completed* order. `sales_line_key` is a deterministic SHA-256-derived 63-bit positive integer from the namespaced `(order_id,line_no)` natural key. Customer and product surrogate keys are similarly generated from their entity-specific IDs. Dates use YYYYMMDD. One dimension row may join to many facts.

**Measures:** `gross_amount_cents = quantity * unit_price_cents`; `net_amount_cents = gross_amount_cents - discount_cents`. `discount_cents` means *total discount for the line*, not per-unit discount. Revenue is computed from **completed** orders only. All stored amounts use integer cents; dividing by 100 is only for display.

## 4. Existing ETL behavior / validation / recovery

1. `pipeline.py` opens all four CSVs, checks exact ordered headers, and processes dimensions → orders → order items. It checks nonblank fields, types, formats, natural-key duplicates, domain rules and referential relationships.
2. Bad individual rows are quarantined in `rejected_records` with **run ID, filename, physical CSV line, source ID, original JSON-formatted fields and reason**. Invalid downstream references can result in further rejected records (each is a separate source row).
3. Exact duplicates within the same source batch are **skipped**; conflicting duplicates are **rejected**. Changed previously published dimension/fact snapshots are rejected for controlled review rather than silently rewritten.
4. Cancelled/pending order lines are **skipped**, not rejected. If an already-published completed order later becomes cancelled/pending, the correction is flagged as `requires_backfill`; the previous fact is not silently deleted.
5. Deterministic keys and unique constraints prevent new duplicate business rows on identical reruns. A repeat run has a new audit `run_id`, but `output_rows=0` when facts are already present.
6. The batch runs inside a SQLite transaction. A fatal error rolls back target changes and per-run rejects, while the run's FAILED audit entry is retained. A corrected retry can then succeed.
7. Older valid orders can be ingested later; their **event time**, normalized to UTC, determines `date_key`, not ingestion time. Source files still need all four headers and the necessary references.
8. Unexpected column headers are a **fatal schema mismatch**, not a row-level rejection.

### Operational counting definitions

- **`input_rows`**: all physical parsed data records from all four CSVs (not headers).
- **`output_rows`**: *new completed-order fact rows inserted during this run*; not all valid source rows and not dimension count.
- **`rejected_rows`**: source rows quarantined in this run; repeated rejects in separate runs have separate audit entries.
- **`skipped_rows`**: identical duplicates, already-existing dimension/fact records, and non-completed lines. Order headers that validate are not counted as new fact outputs.
- **Reconciliation's separate source classification**: every source row belongs to exactly one of `accepted_reference` (customer/product/order header), `completed_line`, `noncompleted_line`, `exact_duplicate`, or `rejected`. Conflicting duplicates belong to `rejected`, not an additional category. Rerun audit skips may be larger than file-level exclusions, because previously loaded facts/dimensions are also skipped.

## 5. Windows installation and data/ETL commands

Open `ecommerce-warehouse` in VS Code. Run commands in its **root**, not its parent directory:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt

# Generate deterministic clean CSV data in a NEW directory if needed.
.\.venv\Scripts\python.exe -m src.warehouse.generator --out data\new_sample --lines 30 --seed 42

# Fresh database for an independently regenerated sample:
.\.venv\Scripts\python.exe -m src.warehouse.pipeline --source data\new_sample --db output\new_sample.db

# Inspect the supplied sample database without changing it:
.\.venv\Scripts\python.exe -m src.warehouse.inspect --db output\warehouse.db --export-dir output\sample_output
```

The supplied `data/sample_input` and `output/warehouse.db` were used for the Phase 5 analytical and reconciliation evidence. The existing sample database already contains two successfully logged runs; **do not delete it** when replicating the evidence.

## 6. Automated tests

```powershell
.\.venv\Scripts\python.exe -m pytest -v
# Capture your own Windows results; keep the supplied execution evidence:
.\.venv\Scripts\python.exe -m pytest -v *> evidence\test_results_windows.txt
$LASTEXITCODE
Get-Content evidence\test_results_windows.txt -Tail 12
```

A pytest exit code of 0 means the test suite passed. All tests use temporary test databases and do not modify `output/warehouse.db`. Original Phase 5 evidence: **30 passed in 1.15s**. After the Windows replay-handle fix, **32 tests passed in 0.94s** using Python 3.13.5 on Linux; Windows verification is still required. The original Phase 4/5 test evidence remains preserved, and the combined-suite result is in `evidence/final_bundle_test_results.txt`.

## 7. Five SQL analytical queries

The query file is `sql/analytics.sql`. All aggregates return **integer cents** for monetary totals:

```powershell
.\.venv\Scripts\python.exe -m src.warehouse.analytics --db output\warehouse.db --sql sql\analytics.sql --out evidence\analytical_query_results_windows.txt
```

The report includes: revenue by category; monthly revenue and `COUNT(DISTINCT order_id)`; top 10 customers by net revenue; top 10 products by units and net revenue; and daily completed-order sales. The supplied sample database has 30 completed-order fact rows, 10 distinct completed orders, and **1,076,712 net cents** total; the detailed category, monthly, customer, product and date results are in `evidence/analytical_query_results.txt`.

## 8. Source-to-target reconciliation

```powershell
.\.venv\Scripts\python.exe -m src.warehouse.reconcile --source data\sample_input --db output\warehouse.db --verify-rerun --out evidence\reconciliation_results_windows.json
$LASTEXITCODE
```

The JSON contains `validation_name`, `expected_value`, `actual_value`, `difference`, `status` (`PASS`/`FAIL`) for every check; summary and counting notes are included. A nonzero exit code means one or more reconciliations failed. Checks cover source-file counts and classification, run-audit and quarantine correspondence, completed fact keys/count, quantities, exact financial totals, FK integrity, duplicate grain keys, per-fact equations, excluded noncompleted orders, and an identical rerun.

`--verify-rerun` creates a **SQLite backup in a disposable temporary directory** and replays the input there. It does *not* rerun against or change the supplied warehouse. Use `--run-id <ID>` to select a specific audit run; by default the latest run is inspected.

**Scope:** this source-to-target reconciliation expects a full source snapshot whose business facts exactly match the database, or an identical rerun. It flags unexpected extra historical facts or corrections; it is not a multi-batch historical inventory/CDC reconciler. For the uploaded sample, actual local output: **22 PASS, 0 FAIL** (`evidence/reconciliation_results.json`).

## 9. Fresh 100,000-line benchmark

```powershell
.\.venv\Scripts\python.exe -m src.warehouse.benchmark --small-source data\sample_input --lines 100000 --seed 42 --out evidence\benchmark_results_windows.json
```

The benchmark generates **exactly 100,000 source order lines** with the Phase 2 generator in a temporary directory. It measures the small and large ETL loads into separate **fresh temporary SQLite files**, using `time.perf_counter()`. Data generation and report writing are excluded from ETL runtime. The temporary databases are automatically deleted; your `output/warehouse.db` is never touched.

Actual execution on Python 3.13.5 in this environment (not a universal performance claim):

| Metric | Small (supplied sample) | Large (seed 42) |
|---|---:|---:|
| Source records across four files | 46 | 133,614 |
| Source order lines | 30 | 100,000 |
| Accepted rows (`input - rejected - skipped`) | 46 | 123,642 |
| Rejected rows | 0 | 0 |
| Skipped rows | 0 | 9,972 |
| New completed-order facts | 30 | 90,028 |
| ETL duration (`perf_counter`, seconds) | 0.040283 | 4.976901 |
| Throughput (source records/sec) | 1,141.93 | 26,846.83 |
| SQLite file size (bytes) | 77,824 | 23,932,928 |
| Foreign-key violations | 0 | 0 |

The larger dataset has 200 customers, 80 products, and 33,334 orders. The 9,972 excluded source lines belong to pending or cancelled orders, so **100,000 source lines are not the same as 100,000 sales facts**.

**Optimization candidate (not implemented or measured):** batch repeated INSERTs using `executemany` or staging tables, and reduce per-row `SELECT` lookups when processing the large input. The existing ETL looks up each completed fact by `(order_id,line_no)` before insert. A before/after performance improvement is **not claimed**, because no controlled code-change experiment was performed. Existing dimension-join indexes were retained.

## 10. Actual evidence and final scope

- `evidence/test_results.txt`: full Phase 5 pytest run, 30 tests.
- `evidence/phase4_test_results_original.txt`: untouched copy of supplied Phase 4 result.
- `evidence/reconciliation_results.json`: sample source-to-target checks, all PASS.
- `evidence/benchmark_results.json`: measurements described above.
- `evidence/analytical_query_results.txt`: actual five SQL result sets.
- `evidence/pipeline_idempotency_phase5.txt`: isolated first load, identical replay, failed transaction and retry.
- Original `sample_first_run.txt`, `sample_second_run.txt`, `sample_inspection.json` and other Phase 4 files are preserved.

**Limitations and future improvements:** no automatic historical status corrections, no SCD Type 2 or change-data-capture/watermarks, no distributed/concurrent ETL, no returns/tax/shipping/currency conversion, no production scheduler. `orders` are validated in memory rather than stored as historical source-state records. Source headers are strict; problematic CSV syntax may fail the batch. Reconciliation is snapshot-scoped. Large-scale throughput could benefit from SQL staging and batch operations; no performance gain was measured.

**AI assistance disclosure:** AI was used to help propose and generate project code, SQL, test cases, reconciliation/benchmark scripts, and documentation, and to run checks in a sandbox environment. These outputs are not represented as independently handwritten. The candidate should review the logic, reproduce the tests on their own Windows machine, confirm results, and explain the design and limitations. The included evidence files are from actual local executions, not screenshots or estimated performance.

See `DESIGN_NOTES.md` for the details of key choices, recovery, assumptions and limitations. Phase 5 is the final scope for this submission; no further platform or UI features are required.


## Final Windows replay-database handle fix

The final project includes a targeted change to `src/warehouse/reconcile.py`: SQLite connections used for backup, before/after snapshots and read-only reconciliation are wrapped in `contextlib.closing()`. `with sqlite3.connect(...)` alone controls transaction commit/rollback but **does not close** the connection, which can cause `[WinError 32]` when `replay.db` is removed on Windows. The ETL pipeline and schema are unchanged. Two regression tests in `tests/test_replay_connection_cleanup.py` check cleanup during normal replay and on an exception.

```powershell
.\.venv\Scripts\python.exe -m src.warehouse.reconcile --source data\sample_input --db output\warehouse.db --verify-rerun --out evidence\reconciliation_results_windows.json
.\.venv\Scripts\python.exe -m pytest -v
```

The final assembled project was verified on Python 3.13.5 (Linux) with **32/32 passing tests** and **22/22 passing reconciliation checks**, and the supplied warehouse database was not modified. See `evidence/final_bundle_test_results.txt` and `evidence/final_bundle_reconciliation_cli.txt`; these are not Windows-specific proof.
