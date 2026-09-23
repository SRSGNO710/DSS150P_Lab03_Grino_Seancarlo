# DSS150P Lab 3: Sales Pipeline (Construction, Storage, Orchestration)

A modular, rerun-safe data pipeline:

```
data/source (CSV/JSON) ─► raw (run snapshot) ─► staging (typed, cleaned, deduplicated) ─► curated (joined + business rules) ─► PostgreSQL
                                                     │                                       │
                                                     └──────────► data/quarantine ◄───────────┘   (invalid rows + reason)
Apache Airflow: schedule · parameters · dependencies · retries · failure handling     Git + Docker/Compose + external config throughout
```

| Doc | Contents |
|---|---|
| [`docs/ANSWERS.md`](docs/ANSWERS.md) | All 8 technical questions, the 5 Goal 3 analysis questions, the schedule/catchup/rerun/backfill explanations, and the technical reflection |
| [`docs/BENCHMARK_INTERPRETATION.md`](docs/BENCHMARK_INTERPRETATION.md) | Goal 3 benchmark interpretation (1–2 pages) plus partitioning |
| [`docs/RUN_EVIDENCE.md`](docs/RUN_EVIDENCE.md) | Completed run-evidence template (Weeks 4–7) |
| [`docs/SUBMISSION_CHECKLIST.md`](docs/SUBMISSION_CHECKLIST.md) | Section 17 checklist: status of every item and how it was verified |
| [`docs/data_dictionary.csv`](docs/data_dictionary.csv) | Every column per layer, with source field and rule |
| `docs/evidence/` | Captured command output (integrated run, partition demo, DAG pre-flight, benchmark files) |

---

## Repository layout

```
.env.example              template for secrets/environment values (copy to .env; .env is git-ignored)
config/settings.yml       non-secret defaults (paths, rules, hash columns, benchmark settings)
src/config.py             the ONLY module that reads settings.yml + .env
src/cli.py                thin entry point: python -m src.cli <command>
src/extract/              raw snapshots (copy + manifest), no business logic
src/transform/            staging.py (typing/cleanup/dedup) and curated.py (joins/amounts/audit)
src/load/                 PostgreSQL UPSERT (full + single partition), no cleaning
src/validate/             environment.py (validate-env), checks.py (read-only contract checks)
src/benchmark/            storage.py (format benchmark), partitioning.py (partitioned Parquet)
src/db.py, errors.py, audit.py   connection helper, stage exceptions/logging, run id + record_hash
dags/dss150p_pipeline.py  Airflow DAG (orchestration only; every task calls src.cli)
sql/init/                 00_create_databases.sql, 01_warehouse_schema.sql (run by the postgres container)
tests/                    unit tests (pytest), including DAG tests that run where Airflow is installed
Dockerfile, docker-compose.yml                 pipeline image + PostgreSQL
Dockerfile.airflow, docker-compose.airflow.yml Airflow (LocalExecutor) on the same PostgreSQL
data/source/              the three source files + SHA256SUMS (never modified by the pipeline)
data/{raw,staging,curated,quarantine,benchmarks,partitioned}/   generated, git-ignored
```

## Setup (Goal 1)

```bash
cp .env.example .env                 # then replace every placeholder value (avoid @ : / in the password)
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m src.cli validate-env       # OK even before PostgreSQL is up (the DB shows as a warning)
python -m pytest -q
```

`POSTGRES_HOST=localhost` in `.env` is for commands run on your machine. Inside
Docker the Compose files override it to `postgres` (the service name). You
never edit `.env` to switch between the two.

### Secret check and Git workflow (Tasks C and E)

```bash
git grep -n "change""_me" -- ":(exclude).env.example" || true   # must print nothing
git status --short                                            # .env must never appear
git log --oneline --decorate --graph --all -15                # goal1..goal4 checkpoints merged into main
```

(The quotes in `"change""_me"` are split on purpose. Bash joins them into the same search string as the
lab's command, but this README then does not match its own search. No committed file other than
`.env.example` contains the placeholder, so the handout's exact command also prints nothing.)

Each goal was developed on its own branch and merged into `main` with `--no-ff`:
`goal1-reproducible-environment` (commit `feat: add reproducible pipeline environment`),
`goal2-etl-pipeline`, `goal3-storage-benchmark`, `goal4-airflow-orchestration`, `docs-submission`.
The pattern used for each checkpoint:

```bash
git switch -c goal1-reproducible-environment
git add .
git commit -m "feat: add reproducible pipeline environment"
git log --oneline --decorate -5
```

### Docker / PostgreSQL

```bash
docker compose build pipeline
docker compose up -d postgres
docker compose ps                                                  # postgres should be "healthy"
docker compose run --rm pipeline python -m src.cli validate-env
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "\dn"
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "\dt curated.*"
```

The postgres container starts in the default `postgres` database, and
`sql/init/00_create_databases.sql` creates `dss150p` and `airflow`. That is why
`docker-compose.yml` sets `POSTGRES_DB: postgres`: if it were `dss150p`, the
init script's `CREATE DATABASE dss150p` would fail. The healthcheck uses
`-h 127.0.0.1`, so the service only turns healthy after the init scripts have
finished.

## Goal 2: ETL pipeline

```bash
python -m src.cli run-all        # extract -> staging -> curated (+ quarantine); prints layer counts
python -m src.cli load           # UPSERT into curated.sales_order_lines; prints inserted/updated/unchanged
python -m src.cli load           # rerun: 0 inserted, 0 updated, 49,897 unchanged
python -m src.cli validate       # contract checks on staging, curated and PostgreSQL
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "SELECT COUNT(*) total, COUNT(DISTINCT order_id) distinct_orders FROM curated.sales_order_lines;"
```

Expected counts (from profiling `data/source`):

| Layer | customers | products | orders |
|---|---:|---:|---:|
| raw (physical rows) | 3,003 | 601 | 50,005 |
| duplicates removed (latest `updated_at` kept) | 3 | 1 | 5 |
| staging | 3,000 (4 flagged `email_missing`) | 599 | 49,998 |
| staging quarantine | 0 | 1 (P0078 negative price) | 2 (O0000112 qty 0, O0004445 UNKNOWN status) |
| curated | | | **49,897** |
| curated quarantine | | | 101 (99 `product_quarantined_in_staging` = orders for P0078, 1 `orphan_customer_id` C99999, 1 `orphan_product_id` P9999) |

## Goal 3: Storage, benchmark, partitioning

```bash
python -m src.cli benchmark --repeats 5              # CSV / JSONL / Parquet / PostgreSQL + partitioned Parquet
cat data/benchmarks/benchmark_results.csv
ls -R data/partitioned | head -40                    # or: tree data/partitioned
python -m src.cli load-partition --year 2026 --month 1
python -m src.cli load-partition --year 2026 --month 1   # rerun: rows stay deduplicated
docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "SELECT * FROM audit.partition_loads;"
```

Partitions use the **UTC** month of `order_timestamp` (2026-01 has 2,506 rows).

## Goal 4: Airflow runbook 

```bash
mkdir -p airflow_logs logs
# Linux only: put AIRFLOW_UID=$(id -u) in .env so the containers can write data/ and logs/
docker compose -f docker-compose.yml -f docker-compose.airflow.yml build
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
docker compose -f docker-compose.yml -f docker-compose.airflow.yml ps
docker compose -f docker-compose.yml -f docker-compose.airflow.yml exec airflow-scheduler airflow dags list-import-errors
```

Open <http://localhost:8080> and sign in with `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env` (lab-only credentials). The DAG `dss150p_sales_pipeline` starts **paused**, so switch it on first.

1. **Full run (Task C).** Click *Trigger DAG* and keep the defaults (`run_mode=full`). Capture the Graph view (extract → transform → load → validate), the Grid view (all green), one task log, and the run's start/end times.
2. **Partition run (Task D).** *Trigger DAG w/ config* with `{"run_mode": "partition", "year": 2026, "month": 1}`. The `load` log shows `load-partition --year 2026 --month 1`. Then:
   `docker exec -it dss150p-postgres psql -U dss150p -d dss150p -c "SELECT * FROM audit.partition_loads;"`
3. **Deliberate failure (Task E).** Temporarily hide one source file. Its content is not changed.
   ```bash
   mv data/source/orders.csv data/source/orders.csv.hidden        # Windows: ren data\source\orders.csv orders.csv.hidden
   ```
   Trigger the DAG. `extract` goes orange (*up_for_retry*) twice, then red. Capture the extract log for attempts 1–3 (`[stage=extract] Source file not found …`), the `DSS150P TASK WILL RETRY` and `DSS150P TASK FAILED` lines, and `logs/airflow_failures.jsonl`.
4. **Recovery.** Restore the file, check it is unchanged, then in the Grid view click the failed `extract` → *Clear* (with downstream):
   ```bash
   mv data/source/orders.csv.hidden data/source/orders.csv
   cd data/source && sha256sum -c SHA256SUMS && cd ../..          # Windows: certutil -hashfile orders.csv SHA256
   ```
   Capture the green rerun and prove there are no duplicates with `SELECT COUNT(*), COUNT(DISTINCT order_id) FROM curated.sales_order_lines;`.

Optional DAG test inside the Airflow image:
`docker compose -f docker-compose.yml -f docker-compose.airflow.yml run --rm airflow-scheduler bash -c "pip install -q pytest && cd /opt/airflow/project && python -m pytest -q tests/test_dag.py"`

## Integrated technical acceptance test (Section 11)

```bash
python -m src.cli validate-env
docker compose up -d postgres
python -m src.cli run-all
python -m src.cli load
python -m src.cli validate
python -m src.cli benchmark --repeats 5
python -m src.cli load-partition --year 2026 --month 1
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

**Clean state**, if the instructor allows it:
`docker compose down -v` (removes the database volume), then remove the files
under `data/raw`, `data/staging`, `data/curated`, `data/quarantine`,
`data/benchmarks` and `data/partitioned`, keeping the `.gitkeep` files.

## CLI reference

| Command | Does |
|---|---|
| `validate-env [--require-db]` | Package versions, settings, source files, env vars, DB reachability and schemas |
| `extract [--run-id]` | `data/raw/run_id=<id>/` byte-identical copies + `_manifest.json` |
| `transform [--run-id]` | staging + curated + quarantine from that run's raw folder (extracts first if missing) |
| `run-all [--run-id]` | `extract` + `transform` |
| `load` | UPSERT on `order_id`, guarded by `record_hash`; writes `audit.pipeline_runs` |
| `validate [--skip-db] [--year Y --month M]` | Keys, statuses, amounts, audit columns, reconciliation, source checksums, DB duplicates and staleness |
| `benchmark [--repeats N] [--skip-postgres]` | Benchmark files plus partitioned Parquet and a selected-partition read check |
| `partition` | Partitioned Parquet only (used by the DAG's partition mode) |
| `load-partition --year Y --month M [--run-id]` | Load one partition + `audit.partition_loads`, in one transaction |

Run ID: `--run-id` if given, else `$PIPELINE_RUN_ID` (Airflow sets this to its `run_id`), else `run_<UTC timestamp>_<random>`.
Logs go to stderr and the JSON result goes to stdout. The exit code is 1 on any stage failure.

## Troubleshooting

| Symptom | Check |
|---|---|
| `validate-env` warns PostgreSQL unreachable | Run `docker compose up -d postgres` and wait for `healthy`. On the host, `POSTGRES_HOST` must be `localhost`. |
| Port 5432 already in use | A local PostgreSQL is running. Stop it, or set `POSTGRES_PORT=5433` in `.env` (Compose publishes that port). |
| `password authentication failed` after changing `.env` | The volume was initialized with the old password. Run `docker compose down -v` (only if a reset is allowed). |
| Airflow `Permission denied` on `data/` or `logs/` (Linux) | Set `AIRFLOW_UID=$(id -u)` in `.env`. Files created earlier by root containers may need `sudo chown -R $USER data logs`. |
| DAG not visible | `... exec airflow-scheduler airflow dags list-import-errors`; check that `dags/` is mounted at `/opt/airflow/dags`. |
| Parquet/pyarrow import error | Activate the venv, or use the container; run `pip install -r requirements.txt`. |
| Duplicate rows after rerun | Cannot happen through `load` (PK + UPSERT). Run `python -m src.cli validate`, which reports duplicates and stale hashes. |
