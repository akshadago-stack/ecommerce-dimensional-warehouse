PRAGMA foreign_keys = ON;

-- SHA-256-derived 63-bit deterministic surrogate keys are assigned by Python.
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INTEGER PRIMARY KEY,
    customer_id TEXT NOT NULL UNIQUE,
    customer_name TEXT NOT NULL CHECK (length(trim(customer_name)) > 0),
    city TEXT NOT NULL CHECK (length(trim(city)) > 0)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key INTEGER PRIMARY KEY,
    product_id TEXT NOT NULL UNIQUE,
    product_name TEXT NOT NULL CHECK (length(trim(product_name)) > 0),
    category TEXT NOT NULL CHECK (length(trim(category)) > 0)
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date TEXT NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL CHECK (month BETWEEN 1 AND 12),
    day INTEGER NOT NULL CHECK (day BETWEEN 1 AND 31)
);

-- Grain: a single line_no on a completed order_id.
CREATE TABLE IF NOT EXISTS fact_sales_order_line (
    sales_line_key INTEGER PRIMARY KEY,
    order_id TEXT NOT NULL,
    line_no INTEGER NOT NULL CHECK (line_no > 0),
    customer_key INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    product_key INTEGER NOT NULL REFERENCES dim_product(product_key),
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    order_timestamp TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents INTEGER NOT NULL CHECK (unit_price_cents >= 0),
    gross_amount_cents INTEGER NOT NULL CHECK (gross_amount_cents >= 0),
    discount_cents INTEGER NOT NULL CHECK (discount_cents >= 0),
    net_amount_cents INTEGER NOT NULL CHECK (net_amount_cents >= 0),
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    ingest_run_id TEXT NOT NULL,
    UNIQUE (order_id, line_no)
);
CREATE INDEX IF NOT EXISTS idx_fact_date_key ON fact_sales_order_line(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_product_key ON fact_sales_order_line(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_customer_key ON fact_sales_order_line(customer_key);

CREATE TABLE IF NOT EXISTS etl_runs (
    run_id TEXT PRIMARY KEY,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    input_rows INTEGER NOT NULL DEFAULT 0,
    output_rows INTEGER NOT NULL DEFAULT 0,
    rejected_rows INTEGER NOT NULL DEFAULT 0,
    skipped_rows INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

-- Rejections belong to an execution, so rerun history is intentionally retained.
CREATE TABLE IF NOT EXISTS rejected_records (
    reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES etl_runs(run_id),
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    source_id TEXT,
    raw_json TEXT NOT NULL,
    reject_reason TEXT NOT NULL,
    rejected_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rejected_run_id ON rejected_records(run_id);
