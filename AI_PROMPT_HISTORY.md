# AI Prompt History

**Project:** E-commerce Dimensional Data Warehouse (Phases 1–5)

## AI Tools Used

- **ChatGPT** — used for architecture/design, data generation, ETL implementation, testing, reconciliation, benchmarking, documentation, debugging, and Windows execution guidance.
- **GitHub Copilot** 


## Phase 1 - Architecture and Design

Tool: ChatGPT

### Prompt 1 

````text
Act as a senior Data Engineer mentoring a candidate with 0–3 years of experience. Help me complete a 72-hour take-home assignment, step by step.

PROJECT: Build a small dimensional data warehouse from transactional e-commerce CSV data.

STACK: Python, Python standard library, SQLite, SQL, pytest, VS Code on Windows. No Spark, Airflow, Docker, cloud services, frontend or unnecessary libraries.

SOURCE ENTITIES:
- customers: customer_id, customer_name, city
- products: product_id, product_name, category
- orders: order_id, customer_id, order_timestamp, order_status
- order_items: order_id, line_no, product_id, quantity, unit_price_cents, discount_cents

TARGET:
- dim_customer
- dim_product
- dim_date
- fact_sales_order_line
- etl_runs and rejected_records for operational traceability

FACT GRAIN: One row per order line belonging to a completed order.

REQUIRED:
1. Define PKs, FKs, data types, nullability and business rules.
2. Use stable deterministic surrogate keys.
3. Store currency in integer cents.
4. Validate input schemas and business rules.
5. Handle duplicates, missing fields, malformed rows, invalid references and unexpected schema changes.
6. Preserve rejected source data with file name, row number and reason.
7. Make reruns idempotent and loads transactional.
8. Record run_id, timestamps, status, input/output/reject counts and error details.
9. Support empty batches, partial failures and retries.
10. Accept late-arriving but otherwise valid order data.
11. Generate reproducible clean and invalid sample datasets with a fixed seed, plus at least 100,000 order lines for benchmarking.
12. Provide pytest tests, SQL analytics, reconciliation and actual execution evidence.
13. Document known limitations honestly.

STRICT INSTRUCTIONS:
- Work one phase at a time.
- Do not build the entire project at once.
- Do not add features beyond those requested.
- Prefer clear beginner-readable functions over abstractions.
- Do not invent test results, performance metrics or screenshots.
- Explain the purpose of every proposed file.
- Do not change an approved design without explaining why.
- At the end of each phase, stop and ask me to proceed.
- When providing executable code, give exact file names, complete file contents and commands to run on Windows.
- If anything fails, help diagnose that exact failure rather than rewriting unrelated parts.

PHASE 1 ONLY:
A. Explain the project architecture in plain English.
B. Define source and target schema/data dictionary with keys and relationships.
C. Explain fact grain, surrogate keys, measures and business-rule assumptions.
D. Propose a simple folder structure.
E. Give 3 sample analytical queries the warehouse should answer.
F. List any important design trade-offs.

Do not generate Python implementation yet. Stop after Phase 1.
````

## Phase 2 - Data Generation

Tool: ChatGPT

### Prompt 2 

````text
approve Phase 1

Implement Phase 2 only. Generate the minimal project folder structure, requirements.txt, SQLite schema SQL, and a Python CSV data generator for customers, products, orders, and order_items. Use a fixed seed and configurable row count, output directory and file paths. The generator must create a clean small sample and support at least 100,000 order lines. Create invalid records separately as test fixtures, not mixed into the default clean data. Provide exact Windows commands to install dependencies and generate the datasets. Do not implement the ETL pipeline or tests yet. Stop after giving me verification steps.
````

## Phase 3 - ETL Implementation

Tool: ChatGPT

### Prompt 3 

````text
Continue with Phase 3 of my e-commerce dimensional warehouse project.

Use the existing Phase 2 project structure, generated source CSV files, schema, and approved design. Do not create a new project.

YOUR TASK:
1. Implement the Python ETL pipeline for customers.csv, products.csv, orders.csv and order_items.csv.
2. Read and validate source data, including schema, required fields, data types, primary keys and foreign keys.
3. Handle duplicate records, missing values, malformed records, unexpected schema changes, invalid references, and invalid quantities or discounts.
4. Store invalid records in a quarantine/rejected_records table with their original values, source_file, row number, run_id and rejection reason.
5. Transform the validated transactional data into:
   - dim_customer
   - dim_product
   - dim_date
   - fact_sales_order_line
6. Define and preserve the fact grain: one row per order line belonging to a completed order.
7. Use deterministic surrogate keys and calculate financial measures using integer cents.
8. Make the pipeline idempotent. Running the same files twice must not create duplicate fact or dimension records.
9. Use SQLite transactions for safe loading and rollback if a failure occurs.
10. Add logging with run_id, start time, end time, status, source count, inserted count, rejected count and failure reason.
11. Handle valid late-arriving and out-of-order records.
12. Preserve source lineage and support reruns and retries.

STRICT RULES:
- Implement Phase 3 only.
- Do not modify the Phase 2 generator or approved schema without first explaining why.
- Do not add a frontend, cloud services, Spark, Airflow, Docker or unrelated features.
- Reuse existing files whenever possible.
- Keep the code modular and understandable for a beginner.
- Provide exact file names, complete code and Windows PowerShell commands.
- Explain what each file does in simple language.
- Do not generate pytest tests, performance benchmarks or final documentation yet.
- Never claim that the pipeline works unless it has actually been executed and verified.

AFTER WRITING THE CODE:
1. Tell me exactly how to run the pipeline in VS Code.
2. Give me SQL or Python commands to inspect the loaded tables.
3. Tell me what output I should expect.
4. Tell me how to check whether records were rejected.
5. Show me how to run the pipeline a second time to verify idempotency.

Stop after Phase 3. Wait for me to run and verify the code before proceeding.
````

### Phase 3 verification follow-up 

````text
Phase 3 verification completed successfully. All eight checks passed. Please review my results before we proceed to Phase 4.
````

## Phase 4 - Automated Testing

Tool: ChatGPT

### Prompt 4 

````text
Continue with Phase 4 of my existing e-commerce dimensional warehouse project.

Phase 2 (data generation) and Phase 3 (ETL pipeline) have been implemented.

YOUR TASK:
Implement automated testing and edge-case verification using pytest.

FIRST:
Inspect the existing project structure, actual schema, pipeline implementation, function names, CLI commands, and business rules. Do not assume files or APIs exist without checking them.

WRITE TESTS FOR:
1. Happy path: valid source files load into dimensions and fact tables with correct data.
2. Empty input: pipeline handles an empty batch safely.
3. Malformed input: invalid CSV rows are handled according to the documented rejection policy.
4. Missing required fields and null values.
5. Duplicate source records: exact duplicates and conflicting duplicate keys.
6. Unexpected schema changes, including missing or extra CSV columns.
7. Invalid customer_id and product_id foreign key references.
8. Domain edge cases: zero/negative quantity and discount greater than gross amount.
9. Cancelled orders: they do not contribute completed-order sales facts.
10. Late-arriving and out-of-order transactions: valid records are not lost.
11. Idempotency: running identical input twice does not increase dimension or fact counts.
12. Partial failure and retry: target data is rolled back and a successful retry does not duplicate data.
13. Financial reconciliation: quantity, gross amount, discount and net revenue are correct.
14. Referential integrity: loaded fact rows reference valid dimension records.
15. Reject traceability: rejected data preserves the original source, row identifier and reason.

STRICT RULES:
- Implement Phase 4 only.
- Reuse existing project code and configuration.
- Do not rewrite the complete ETL pipeline.
- Do not change the approved schema or business rules without explaining why.
- Use pytest temporary directories and separate test databases. Never modify or delete the project's real sample/output database during tests.
- Use small deterministic synthetic test fixtures.
- Give complete code only for files being added or changed.
- Do not silently weaken tests to make them pass.
- If a test discovers a real bug, explain the root cause and provide the smallest necessary fix.
- Do not invent successful test results.
- Do not implement performance benchmarking or final documentation yet.

AFTER IMPLEMENTATION:
1. Give exact Windows PowerShell commands to run the tests.
2. Explain the purpose of each test file.
3. Show how to interpret passed and failed tests.
4. Explain how to save actual test execution evidence.
5. Stop after Phase 4 and wait for my verification.
````

## Phase 5 - Reconciliation and Benchmarking

Tool: ChatGPT

### Prompt 5 
````text
Continue with **Phase 5 of my existing E-commerce Dimensional Data Warehouse project**.

Phases 1–4 have already been implemented, including:

- Reproducible CSV data generation
- SQLite star schema
- Python ETL pipeline
- Data validation and rejected-record handling
- Idempotency and run logging
- Automated pytest tests

**FIRST — Inspect the existing project**

Before writing any code, inspect the actual folder structure, README, SQLite schema, existing Python functions, CLI commands, tests, and available output files.

Reuse the existing implementation. Do not assume file names, table names, database paths, or commands.

## TASK 1 — Analytical SQL Queries

Implement at least five SQL queries against the existing dimensional model:

1. Total sales revenue by product category.
2. Monthly revenue and number of distinct completed orders.
3. Top 10 customers by total revenue.
4. Top 10 products by quantity sold and revenue.
5. Daily sales trends.

Use actual existing column names.

Save queries in the existing `sql/` directory. Provide actual query results from the sample database.

## TASK 2 — Data Reconciliation

Create reconciliation queries or a Python validation script that verifies:

1. Source records versus accepted, rejected, skipped, and duplicate records, with clearly defined counting rules.
2. Expected completed-order line count versus loaded sales fact count.
3. Source gross amount versus fact-table gross amount.
4. Source discount versus fact-table discount.
5. Source net revenue versus fact-table net revenue.
6. Fact-table foreign keys reference valid dimensions.
7. No duplicate fact grain keys exist.
8. Gross amount minus discount equals net amount.
9. Cancelled orders are excluded from completed-order revenue.
10. Rerunning identical files does not increase business-record counts.

Generate a machine-readable reconciliation report showing:

- Validation name
- Expected value
- Actual value
- Difference
- PASS/FAIL status

Account for documented exclusions and avoid double-counting duplicate or rejected records.

Do not manufacture values. Calculate results from the actual dataset.

## TASK 3 — Performance Benchmarking

Benchmark the existing ETL pipeline using:

- The small starter dataset.
- A deterministic larger dataset containing at least 100,000 order lines.

Measure:

1. Number of source records processed.
2. Number of accepted and rejected records.
3. Execution duration in seconds.
4. Processing throughput in records/second.
5. Final fact-table record count.
6. Database size, where practical.

Use Python's `time.perf_counter()` for elapsed time measurement.

Run the benchmark with a fresh isolated database so previous pipeline runs do not distort the results.

Identify one reasonable performance optimization, such as batch inserts, appropriate indexing, or reducing repeated database lookups.

Explain why the optimization is relevant. If feasible, compare before-and-after measurements under identical conditions.

Do not claim a performance improvement without measured evidence.

## TASK 4 — Execution Evidence

Generate or update the existing `evidence/` folder with actual execution results:

- `test_results.txt`
- `reconciliation_results.txt` or `.json`
- `benchmark_results.txt` or `.json`
- `analytical_query_results.txt`
- Pipeline execution and idempotency verification results

Preserve genuine Phase 4 evidence.

If you cannot execute a command in your environment, provide the exact Windows PowerShell command for me to run and clearly mark the evidence as pending.

Never fabricate test results, benchmark timings, database outputs, or successful execution logs.

## TASK 5 — Final Documentation

Update the existing `README.md` and `DESIGN_NOTES.md` to explain:

1. Business problem and project objective.
2. Source-to-target architecture.
3. Source entities and data dictionary.
4. Star schema and fact grain.
5. Surrogate keys and relationships.
6. ETL workflow.
7. Data validation and rejection policy.
8. Duplicate handling and idempotency.
9. Failure recovery, retries, and backfill behavior.
10. Running the project locally on Windows.
11. Running the automated tests.
12. Running analytical and reconciliation queries.
13. Actual benchmark methodology and results.
14. Known limitations and future improvements.
15. The role of AI assistance, accurately described.

Make the documentation understandable to a Data Engineer with 0–3 years of experience.

## STRICT IMPLEMENTATION RULES

- Implement **Phase 5 only**.
- Work within the existing project folder.
- Do not rebuild Phases 1–4.
- Do not change source schemas, fact grain, business rules, or surrogate-key logic without identifying a concrete defect and explaining the change.
- Do not rewrite working pipeline code unnecessarily.
- Do not weaken existing tests.
- If a defect is found, propose the smallest fix and rerun all relevant tests.
- Do not add Streamlit, dashboards, Spark, Airflow, Docker, AWS, APIs, or unrelated services.
- Do not add unnecessary libraries.
- Use SQLite, SQL, Python, and existing project dependencies.
- Keep code modular and understandable.
- Use deterministic test data and isolated benchmark databases.
- Do not invent files, results, successful test runs, or performance numbers.

## AFTER IMPLEMENTATION

Provide:

1. List of files created or modified.
2. Exact Windows PowerShell commands to execute the reconciliation.
3. Exact command to execute analytical queries.
4. Exact command to benchmark 100,000+ records.
5. Exact command to rerun the complete pytest suite.
6. Actual measured output, if execution was performed.
7. A final checklist showing which original assignment acceptance criteria are satisfied and which remain unverified.

**Stop after Phase 5. Do not add new features or begin another phase.**
````

## Debugging and Fixes

Tool: ChatGPT

### Prompt 6 — Diagnose and fix `WinError 32` when `--verify-rerun` cleans up `replay.db` 
````text
I am running my existing Phase 5 E-commerce Dimensional Warehouse project on Windows using Python 3.13.

The reconciliation script fails when I execute it with `--verify-rerun`.

**ACTUAL ERROR:**

`PermissionError: [WinError 32] The process cannot access the file because it is being used by another process.`

The affected file is a temporary SQLite database named `replay.db`.

The traceback points to `src/warehouse/reconcile.py`, inside `verify_replay()`, around line 271, and to temporary directory cleanup through `shutil.rmtree()`.

**YOUR TASK:**

1. Inspect the actual existing `src/warehouse/reconcile.py` implementation.
2. Identify which SQLite connection or other resource is keeping `replay.db` open.
3. Inspect any other functions called during replay verification that may leave database connections open.
4. Ensure all SQLite connections, cursors and other file handles are closed before temporary directory cleanup.
5. Use `try/finally` or `contextlib.closing()` where appropriate. Remember that `with sqlite3.connect(...)` alone does not close a connection.
6. Preserve the existing idempotency verification logic.
7. Ensure the fix works on Windows.
8. Add or update a regression test for this issue where practical.

**STRICT RULES:**

- Fix only this specific issue and any directly related resource leak.
- Do not rewrite the ETL pipeline.
- Do not modify the star schema.
- Do not change business rules, financial calculations or existing reconciliation logic.
- Do not delete the production warehouse database.
- Do not disable `--verify-rerun`.
- Do not hide the exception or simply ignore failed temporary-directory cleanup.
- Do not introduce arbitrary sleep calls or unnecessary dependencies.
- Do not claim that the bug is fixed without actual execution evidence.

**AFTER THE FIX:**

Provide the exact files changed, explain the root cause, and give Windows PowerShell commands to rerun reconciliation with `--verify-rerun` and execute the automated tests.

Do not begin Phase 6 or make unrelated changes.
````

### Prompt 7 — Request a full project bundle with final changes 
````text
can you generate the full folder till phase 5 with the final changes
````

### Prompt 8 — Request to run the Windows benchmark 

````text
lets run the 100,000-line benchmark on Windows
````



## AI Assistance and Verification Disclosure

ChatGPT assisted with implementation ideas, code generation, debugging, test construction, SQL/reporting, documentation, and Windows commands. The user executed the project and shared screenshots confirming 34 passing pytest cases and the successful 100,000-order-line benchmark on Windows. Do not describe those user-supplied screenshots as AI-generated or claim use of GitHub Copilot without confirmation.
