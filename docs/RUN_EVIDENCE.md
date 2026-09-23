# Run Evidence

Values below were captured by running this repository in a verification
sandbox: Ubuntu 24.04, Python 3.11.15, a local PostgreSQL 16.13 server. Raw
output is in `docs/evidence/`. Items marked **▶ CAPTURE** must be produced on
your own machine, because they need Docker Desktop or the Airflow UI. Paste
your output or screenshot names in their place.

## Week 4: Goal 1 environment

- **Python version:** 3.11 (Docker image `python:3.11-slim`; Airflow image `apache/airflow:2.10.5-python3.11`). Pinned packages are in `requirements.txt`: pandas 2.2.3, pyarrow 18.1.0, psycopg2-binary 2.9.10, PyYAML 6.0.2, python-dotenv 1.0.1, numpy 2.1.3, pytest 8.3.4.
  ▶ CAPTURE `python --version` and `pip list | grep -Ei "pandas|pyarrow|psycopg2|yaml|dotenv"` from your venv, plus `python -m src.cli validate-env` (see `docs/evidence/integrated_run_sandbox.txt` for the expected shape).
- **Git status/log evidence:** branches `goal1-reproducible-environment`, `goal2-etl-pipeline`, `goal3-storage-benchmark`, `goal4-airflow-orchestration` and `docs-submission`, each merged into `main` with `--no-ff`, so the checkpoints are visible in `git log --oneline --graph --all`. `.env` has never been tracked.
  ▶ CAPTURE `git log --oneline --decorate --graph -15` and `git status --short`.
- **Docker image/container evidence:** ▶ CAPTURE `docker compose build pipeline`, `docker compose ps` (postgres **healthy**), `docker compose run --rm pipeline python -m src.cli validate-env` (`"postgres": "ok (schemas: audit,curated,staging)"`), and `\dn` / `\dt curated.*` through `docker exec`. The compose files were validated with `docker compose config`. The images could not be pulled in the sandbox.
- **External configuration evidence:** `config/settings.yml` holds non-secret defaults. `.env` (git-ignored) holds host, port, DB, user and password. `src/config.py` is the only reader, and `load_dotenv(override=False)` lets Compose set `POSTGRES_HOST=postgres` in containers while `.env` keeps `localhost` for the host. The Task C `git grep` for the placeholder string (excluding `.env.example`) prints nothing. Full explanation: `docs/ANSWERS.md` Part C.

## Week 5: Goal 2 ETL

(Source: `docs/evidence/goal2_layer_counts_sandbox.txt`, `docs/evidence/integrated_run_sandbox.txt`.)

- **Raw row counts:** customers.csv 3,003 · products.json 601 · orders.csv 50,005, in `data/raw/run_id=<run id>/` with a SHA-256 per file in `_manifest.json`, identical to `data/source/SHA256SUMS`.
- **Staging row counts:** customers 3,000 (3 older duplicates removed; 4 rows kept with `email_missing = true`) · products 599 (1 duplicate removed: P0300 → "Rev2" version kept) · orders 49,998 (5 duplicates removed, latest `updated_at` kept, e.g. O0000100 CANCELLED → DELIVERED).
- **Curated row counts:** 49,897 `sales_order_lines`; 49,897 + 101 quarantined = 49,998 staged orders (reconciled by `validate`). Net amount total: 9,273,680,736.60.
- **Quarantine row counts:** 104 in total.
  - `staging_products.csv`: 1, P0078 `negative_unit_price` (−199.00)
  - `staging_orders.csv`: 2, O0000112 `invalid_quantity` (0) and O0004445 `invalid_status` (UNKNOWN)
  - `curated_orders.csv`: 101, of which 99 are `product_quarantined_in_staging` (the orders for P0078), 1 is `orphan_customer_id` (O0002223 → C99999) and 1 is `orphan_product_id` (O0003334 → P9999)
- **First load affected rows:** `inserted 49,897, updated 0, unchanged 0`.
- **Second rerun affected rows / evidence of idempotency:** `inserted 0, updated 0, unchanged 49,897`. `SELECT COUNT(*), COUNT(DISTINCT order_id)` gives `49,897 | 49,897`. A new full pipeline run under a different run ID followed by a load also touched 0 rows (the `record_hash` is identical across runs). One deliberately corrupted DB row was detected by `validate` and repaired by the next load (`updated 1`).
- **Error-handling evidence:** missing source → `[stage=extract] Source file not found: …/orders.csv (dataset=orders run_id=…)`, exit 1. DB down → `[stage=load] Cannot connect to PostgreSQL at dss150p@localhost:5999/dss150p … caused by OperationalError`, exit 1. Wrong password → stage error, and the password is never printed.
  ▶ CAPTURE the same `COUNT(*) / COUNT(DISTINCT)` query through `docker exec -it dss150p-postgres psql …` after two `load`s.

## Week 6: Goal 3 storage

- **Benchmark table attached:** yes. `docs/evidence/benchmark_results_sandbox.csv` (+ timings, environment, EXPLAIN plans) and interpretation `docs/BENCHMARK_INTERPRETATION.md`.
  ▶ OPTIONAL: rerun `python -m src.cli benchmark --repeats 5` on your machine and attach your `data/benchmarks/benchmark_results.csv`. Sizes should match closely; update the seconds in the interpretation if they differ.
- **Partition selected:** `order_year=2026/order_month=1` (UTC month). The tree in `docs/evidence/partition_tree_sandbox.txt` has 21 partitions, 2025-01 to 2026-09.
- **Partition row count:** 2,506. All rows verified inside January 2026 UTC; `rows_outside_partition = 0`.
- **PostgreSQL verification query:**
  ```sql
  SELECT * FROM audit.partition_loads;
  -- order_year=2026/order_month=1 | <loaded_at_utc> | 2506 | <pipeline_run_id>
  SELECT COUNT(*), COUNT(DISTINCT order_id) FROM curated.sales_order_lines
  WHERE order_timestamp >= '2026-01-01T00:00:00Z' AND order_timestamp < '2026-02-01T00:00:00Z';  -- 2506 | 2506
  ```
  On an emptied table, load-partition gave `inserted 2,506`; the rerun gave `inserted 0 / unchanged 2,506`, and the audit row was updated in place (`docs/evidence/goal3_partition_load_demo.txt`).

## Week 7: Goal 4 Airflow

- **DAG ID:** `dss150p_sales_pipeline`
- **Schedule:** `0 2 * * *` in Asia/Manila (daily 02:00), `catchup=False`, `max_active_runs=1`, retries 2 (1 min, exponential up to 5 min), per-task `execution_timeout` 5–15 min, `dagrun_timeout` 1 h.
- **Parameters used:** full run `{"run_mode": "full"}`; partition run `{"run_mode": "partition", "year": 2026, "month": 1}`.
- **Successful run ID:** ▶ CAPTURE (e.g. `manual__2026-…`), with Graph and Grid screenshots.
- **Deliberate failure run ID:** ▶ CAPTURE. Method: `data/source/orders.csv` temporarily renamed.
- **Retry/failure-handling evidence:** ▶ CAPTURE the extract log for tries 1/3, 2/3 and 3/3 (`[stage=extract] Source file not found`), the `DSS150P TASK WILL RETRY` (×2) and `DSS150P TASK FAILED` lines, and `logs/airflow_failures.jsonl`.
- **Final recovery run ID:** ▶ CAPTURE (same run, extract cleared with downstream → all green), plus `COUNT(*) = COUNT(DISTINCT order_id) = 49,897`.

Pre-flight: the DAG commands, parameters, run-ID propagation, retries, callbacks and recovery were exercised with a small Airflow test double in the sandbox (`docs/evidence/goal4_dag_preflight_sandbox.txt`). That shows the behavior you should see in the UI, but it **is not Airflow evidence** and does not replace the screenshots above.
