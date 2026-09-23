\connect dss150p;

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS curated.sales_order_lines (
  order_id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  product_id TEXT NOT NULL,
  order_timestamp TIMESTAMPTZ NOT NULL,
  customer_city TEXT,
  customer_tier TEXT,
  product_name TEXT,
  category TEXT,
  brand TEXT,
  quantity INTEGER NOT NULL,
  unit_price NUMERIC(14,2) NOT NULL,
  discount_pct NUMERIC(6,4) NOT NULL,
  gross_amount NUMERIC(16,2) NOT NULL,
  discount_amount NUMERIC(16,2) NOT NULL,
  net_amount NUMERIC(16,2) NOT NULL,
  status TEXT NOT NULL,
  source_updated_at TIMESTAMPTZ NOT NULL,
  pipeline_run_id TEXT NOT NULL,
  processed_at_utc TIMESTAMPTZ NOT NULL,
  record_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
  pipeline_run_id TEXT PRIMARY KEY,
  started_at_utc TIMESTAMPTZ NOT NULL,
  completed_at_utc TIMESTAMPTZ,
  status TEXT NOT NULL,
  rows_staging INTEGER,
  rows_curated INTEGER,
  rows_quarantined INTEGER,
  message TEXT
);

CREATE TABLE IF NOT EXISTS audit.partition_loads (
  partition_key TEXT PRIMARY KEY,
  loaded_at_utc TIMESTAMPTZ NOT NULL,
  row_count INTEGER NOT NULL,
  pipeline_run_id TEXT NOT NULL
);
