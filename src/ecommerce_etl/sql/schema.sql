-- Warehouse schema for the e-commerce order pipeline.
-- Applied idempotently at the start of every load.

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id text PRIMARY KEY,
    country text NOT NULL CHECK (char_length(country) = 2),
    first_order_date date NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id text PRIMARY KEY,
    product_name text NOT NULL,
    category text NOT NULL,
    last_unit_price numeric(12, 2) NOT NULL CHECK (last_unit_price >= 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fact_order_items (
    order_id text NOT NULL,
    customer_id text NOT NULL REFERENCES dim_customers (customer_id),
    product_id text NOT NULL REFERENCES dim_products (product_id),
    order_date date NOT NULL,
    order_month date NOT NULL,
    quantity integer NOT NULL CHECK (quantity > 0),
    unit_price numeric(12, 2) NOT NULL CHECK (unit_price >= 0),
    discount numeric(5, 4) NOT NULL DEFAULT 0 CHECK (discount >= 0 AND discount <= 1),
    line_total numeric(12, 2) NOT NULL CHECK (line_total >= 0),
    status text NOT NULL CHECK (
        status IN ('pending', 'shipped', 'delivered', 'cancelled', 'returned')
    ),
    country text NOT NULL CHECK (char_length(country) = 2),
    source_file text NOT NULL,
    ingest_run_id text NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, product_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_order_date ON fact_order_items (order_date);
CREATE INDEX IF NOT EXISTS idx_fact_customer ON fact_order_items (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_run ON fact_order_items (ingest_run_id);

CREATE TABLE IF NOT EXISTS rejected_order_items (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ingest_run_id text NOT NULL,
    source_file text NOT NULL,
    source_row_number integer NOT NULL,
    raw_row jsonb NOT NULL,
    reject_reason text NOT NULL,
    rejected_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (ingest_run_id, source_row_number)
);

CREATE INDEX IF NOT EXISTS idx_rejected_run ON rejected_order_items (ingest_run_id);

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id text PRIMARY KEY,
    source_file text NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    status text NOT NULL CHECK (status IN ('running', 'loaded', 'success', 'failed')),
    rows_read integer NOT NULL DEFAULT 0,
    rows_valid integer NOT NULL DEFAULT 0,
    rows_rejected integer NOT NULL DEFAULT 0,
    rows_upserted integer NOT NULL DEFAULT 0,
    revenue_loaded numeric(14, 2) NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_run_log_started ON etl_run_log (started_at DESC);

COMMENT ON TABLE fact_order_items IS 'One row per order line, upserted on (order_id, product_id).';
COMMENT ON TABLE rejected_order_items IS 'Quarantine: rows that failed validation, with reasons.';
COMMENT ON TABLE etl_run_log IS 'Operational metrics for every pipeline run.';
