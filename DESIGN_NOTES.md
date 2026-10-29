# Design Notes — E-commerce Dimensional Data Warehouse (Phases 1–5)

## Business goal and chosen architecture

Transform synthetic transactional CSV exports into a dependable small local analytical warehouse. Read `customers`, `products`, `orders`, `order_items` in Python; validate; quarantine unacceptable source rows; load SQLite dimensions and completed-order facts; audit each run. This is a minimal, explainable Python + SQLite solution, not a production distributed platform.

## Source contract and business rules

- `customers(customer_id TEXT PK, customer_name TEXT, city TEXT)`; customer fields required/nonblank.
- `products(product_id TEXT PK, product_name TEXT, category TEXT)`; product fields required/nonblank.
- `orders(order_id TEXT PK, customer_id TEXT FK, order_timestamp TEXT, order_status TEXT)`; valid status `completed`, `pending`, or `cancelled`; timestamp must include a timezone and is converted to UTC.
- `order_items(order_id TEXT, line_no INT, product_id TEXT, quantity INT, unit_price_cents INT, discount_cents INT)`; composite `(order_id,line_no)` natural key; order/product FKs; line and quantity positive; price and discount nonnegative; discount cannot exceed quantity × unit price.
- Exact header sequence is required; an unrecognized header is a **batch-fatal schema error**, whereas a malformed individual row or invalid FK is rejected if the parser can preserve row boundaries.

## Target schema, grain, keys, measures

- Star dimensions: `dim_customer(customer_key PK, customer_id UNIQUE, customer_name, city)`, `dim_product(product_key PK, product_id UNIQUE, product_name, category)`, `dim_date(date_key PK, full_date, year, month, day)`.
- Fact: `fact_sales_order_line(sales_line_key PK, order_id, line_no, customer_key FK, product_key FK, date_key FK, order_timestamp, quantity, unit_price_cents, gross_amount_cents, discount_cents, net_amount_cents, source_file, source_row_number, ingest_run_id)`; **UNIQUE `(order_id,line_no)`**.
- Grain = one line **belonging to a completed order**. A multi-line order creates multiple fact rows, but `COUNT(DISTINCT order_id)` counts it once in order reports.
- SHA-256-derived positive 63-bit integer surrogate keys from entity-specific namespaces avoid arrival-order-dependent autoincrement. Date key is event date `YYYYMMDD` in UTC. Hash collision risk is negligible but not impossible; uniqueness/PK constraints protect against silent replacement.
- Currency stays in integer cents. `gross = quantity × unit_price_cents`; `net = gross − discount_cents`; discount is per line, not per unit. Only integer sums are used for financial reconciliation.
- Operational tables: `etl_runs(run_id PK, started_at, ended_at, status, input_rows, output_rows, rejected_rows, skipped_rows, error_message)` and `rejected_records(reject_id PK, run_id FK, source_file, source_row_number, source_id, raw_json, reject_reason, rejected_at)`.

## Duplicate handling, validation and lifecycle

1. Load dimensions, orders, order lines in dependency order. Reject missing/blank fields, invalid integers/status/timestamps, unknown references and discounts greater than gross.
2. First valid source natural key wins within a batch. Exact duplicate rows are skipped; conflicting duplicate keys are rejected with an explicit reason.
3. On repeat runs, already stored identical dimension and fact records are skipped. Existing *changed* snapshots are quarantined, not overwritten: this prototype does not implement a full change-data-capture or SCD Type 2 pipeline.
4. `cancelled` and `pending` lines are skipped, not included in sales or treated as errors. Previously published completed-order lines subsequently marked cancelled/pending are **flagged for explicit repair** (`requires_backfill`), not automatically removed. This protects financial history from silent reversal.
5. Late-arriving valid completed orders load using their UTC business date, even if earlier than already stored facts. Their customer/product references must be available in the batch (the pipeline does not persist separate source-order staging).
6. `BEGIN IMMEDIATE` encloses warehouse writes and successful run counts/rejects; any fatal exception rolls them back. A RUNNING audit entry is committed before loading, and FAILED status is written after rollback. A new run ID is used for retry.
7. Successful replay preserves dimension and fact counts; the `etl_runs` and per-run reject histories grow intentionally. No silent upsert of changed facts occurs.

## Count semantics and reconciliation design

- `input_rows` = source data rows read across 4 CSVs. `output_rows` = *inserted completed-order fact rows for this run*. `rejected_rows` = rows quarantined this run. `skipped_rows` = exact duplicate rows, reused dimension/fact rows, and pending/cancelled lines.
- Source-only reconciliation classifies each input record **exactly once** into: accepted reference row (customer/product/order), accepted completed line, noncompleted line, exact duplicate, or rejected row. Conflicting duplicates are a **subset of rejects**, not counted again as accepted or skipped. The source classification is separate from run audit: replay skips reused records, which changes `skipped_rows` but not source semantics.
- `reconcile.py` derives expected facts and quantities/money from CSVs, compares fact natural keys and values, checks quarantine row identities/reasons and audit counts, and verifies FK/grain/equation/cancellation rules. The JSON report contains the requested expected/actual/difference/PASS-or-FAIL fields.
- Optional idempotency verification uses SQLite's `backup()` to a disposable database, runs the existing pipeline there and compares before/after business counts and financial totals. It never modifies the supplied database.
- **Scope limitation:** source-to-target comparison assumes the warehouse represents the provided complete source snapshot or its identical rerun. It deliberately flags extra/mismatched historical data rather than pretending to reconcile arbitrary partially overlapping batches, status reversals, or CDC events.

## Analytics and performance

- `sql/analytics.sql` includes five named queries: category revenue, monthly orders/revenue, customer top 10, product top 10 by units/revenue, daily trends. Reporting keeps monetary values in integer cents.
- `benchmark.py` generates a fresh seeded 100,000-line source dataset, runs the unchanged ETL on two fresh temporary SQLite databases, and measures execution with `time.perf_counter()`. Source generation/report writing are excluded. No time is reused from earlier pipeline runs.
- Actual local measurements (Python 3.13.5, execution environment): starter 46 source rows, 30 facts, 0 rejected, 0.040283 seconds, 1,141.93 rows/second, 77,824 database bytes; large 133,614 source rows including 100,000 order items, 90,028 completed facts, 9,972 skipped, 0 rejected, 4.976901 seconds, 26,846.83 rows/second, 23,932,928 bytes. Full machine-readable evidence in `evidence/benchmark_results.json`.
- **Potential optimization:** batch operations (`executemany`/staging) or fewer repeated per-order-line existence lookups. No implementation or measured speedup was made; a proper before/after test requires same inputs, clean DBs and comparable hardware. Existing FK lookup indexes were kept unchanged.

## Verification evidence

- Original Phase 4 tests preserved unchanged; additional Phase 5 tests check analytical output, reconciliation, a quarantine/cancelled/duplicate case, detection of financial corruption and isolated benchmarking. Actual full-suite result: **30 passed in 1.15 seconds**; see `evidence/test_results.txt`.
- Sample source-to-target reconciliation: **22 PASS, 0 FAIL** with disposable idempotency replay (`evidence/reconciliation_results.json`). Sample fact total 30 rows; quantity 99, gross 1,171,320 cents, discount 94,608 cents, net 1,076,712 cents.
- Isolated pipeline first run inserted 30 facts; identical rerun inserted 0, retained 30 facts and added a second audit run. Injected failure rolled back all target rows; retry inserted 30 facts once. See `evidence/pipeline_idempotency_phase5.txt`.
- Sample analytical outputs are in `evidence/analytical_query_results.txt`, full benchmark in `evidence/benchmark_results.json`; original evidence remains in the folder.

## Limitations, future work, and trade-offs

- SQLite is easy to run and transactional but not suited to high-concurrency distributed ingestion.
- Dimensions behave like immutable descriptive snapshots: changed attributes are quarantined, not managed with SCD Type 2. Late source correction cannot silently rewrite already published fact financial totals.
- Historical cancellations after completed sale need a governed correction/backfill process; there is no refunds/returns ledger.
- All monetary amounts assume one currency; taxes, shipping, exchange rates and returns are excluded.
- CSV contracts are strict and no versioned schema registry or CDC/watermarks are implemented; no source-file checksum/manifest is stored for cross-batch overlap.
- Source orders are held in memory for reference checks; 100k lines are supported, but larger scales may warrant staging and set-based SQL inserts.
- Accepted facts preserve source filename and row number, but no immutable raw-source archive is built; rejected rows preserve source values as JSON.
- Manual scheduling is intentional: no Airflow, cloud, services, dashboard or UI.
- The reconciliation process validates a full snapshot or identical replay, not arbitrary multi-file incremental history.

## AI assistance (disclosure)

AI assisted in generating and reviewing the Python implementation, SQL queries, test cases, reconciliation and benchmarking code, documentation and execution commands. The included automated test runs, SQL output, recon and performance measurements were actually executed in a sandbox. This does **not** imply the candidate independently authored every source line or has verified the results on their Windows machine. The candidate should reproduce the commands, verify data, debug any environmental differences and be ready to defend all design decisions.


### Post-Phase-5 Windows SQLite replay cleanup

A targeted resource-management fix now wraps reconciliation backup, snapshot and read-only SQLite connections in `contextlib.closing()`. This leaves the original ETL, schema and idempotency semantics untouched; it ensures replay connections are closed before `TemporaryDirectory` removes `replay.db`. Two regression tests verify normal and exception paths. The combined project passed **32 tests** and the replay reconciliation **22 checks** using Python 3.13.5 on Linux. The original Phase 5 30-test evidence remains preserved as a historical execution result; Windows-specific execution must still be verified by the candidate.
