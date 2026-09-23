# DSS150P Lab 3: Written Answers 

Every numeric claim below comes from a run of this repository. The file
evidence is in `docs/evidence/`. Benchmark timings were measured in the
verification sandbox (2 vCPU Intel Xeon 2.10 GHz, 7.8 GB RAM, Ubuntu 24.04,
Python 3.11, pandas 3.0.2, pyarrow 25.0.1, PostgreSQL 16.13). Rerun
`python -m src.cli benchmark --repeats 5` on your own machine and update the
seconds columns. File sizes and row counts are deterministic and should match
closely.

---

## Part A: Section 15 technical questions

### 1. Why is `record_hash` useful for rerun-safe loading, and which columns should not be included in it?

`record_hash` is a SHA-256 fingerprint of a row's **business content**. The
load is an UPSERT on `order_id`, and its `DO UPDATE` branch carries a guard:

```sql
ON CONFLICT (order_id) DO UPDATE SET ...
WHERE curated.sales_order_lines.record_hash IS DISTINCT FROM EXCLUDED.record_hash
```

The primary key stops a rerun from ever creating a second row. The hash guard
stops a rerun from *rewriting* a row whose content did not change. Without it,
every rerun would update all 49,897 rows. Each update creates dead tuples and
WAL (the write-ahead log) and bumps `processed_at_utc`, so an unchanged row
would look changed. With the guard, the second load reported
`inserted 0 / updated 0 / unchanged 49,897`. When one row in the database was
deliberately corrupted, the next load reported `updated 1` and repaired exactly
that row. The same order hashed to the same value (`255321fa…`) in runs made at
different times under different run IDs.

**Columns excluded** (see `config/settings.yml → curated.hash_columns`):

| Column | Why it is excluded |
|---|---|
| `pipeline_run_id` | Differs on every run. Including it would make every row "changed" on every rerun. |
| `processed_at_utc`, `staged_at_utc` | Wall-clock time of processing, so also different every run. This is the classic "duplicate updates after rerun" bug in the troubleshooting table. |
| `source_updated_at` | Freshness metadata. A source "touch" that changes nothing in the business content should not force an update. |
| `record_hash` | A hash cannot include itself. |

Values are canonicalized before hashing: timestamps become UTC ISO strings,
numbers are formatted to 4 decimals, and nulls become `None`. That way the hash
does not flip because a column was read back as `int64` instead of `float64`.

### 2. Why should raw data usually be preserved even when staging/curated outputs are sufficient for analytics?

- **Reproducibility and audit.** `data/raw/run_id=<id>/` together with its `_manifest.json` (SHA-256 per file) records exactly what each run saw. Any curated number can be traced back to the input bytes.
- **Reprocessing when rules change.** Staging and curated outputs embed today's decisions (keep the latest version, quarantine `quantity = 0`). If a rule was wrong, or a new column is needed, the history can be rebuilt from raw instead of from an already-filtered copy.
- **Sources are not stable.** Upstream files get overwritten, APIs only return the current state, and databases get updated in place. Raw is often the only record of what the data looked like on a given day.
- **Debugging and quarantine reprocessing.** A quarantined record can be compared against its original bytes. This matters because cleaning can hide problems. For example, staging normalizes `"  manila  "` to `Manila`, and only raw shows that the source is dirty.
- **Decoupling.** Extraction can run on the source system's schedule, and transformation can be rerun on its own without touching the source again. (The DAG's `transform` reads the raw snapshot that `extract` created for the same run ID.)

### 3. What is the difference between a data-quality rejection and a system exception?

| | Data-quality rejection | System exception |
|---|---|---|
| **What failed** | One record breaks a business or validity rule | The pipeline itself cannot do its job |
| **Examples here** | `quantity = 0` (O0000112), status `UNKNOWN` (O0004445), negative catalog price (P0078), orphan `C99999` / `P9999` | `orders.csv` missing, PostgreSQL unreachable, bad password, unreadable Parquet, a broken output contract in `validate` |
| **Handling** | Route the row to `data/quarantine/*.csv` with a `quarantine_reason`. The run continues and the counts reconcile (49,897 curated + 101 quarantined = 49,998 staged). | Raise a `StageError` that names the stage and keeps the root cause (`raise … from err`). The CLI exits 1, and Airflow retries the task and then calls the failure callback. |
| **Who fixes it** | The data owner or source system. Records can be reprocessed later. | The engineer or operator (infrastructure, config, code). |
| **Retrying helps?** | No. The same record just fails the same way every time. | Often yes: a transient outage or a lock. |

Mixing them up is harmful in both directions. Throwing on every bad row means
one typo stops the whole load. Swallowing system errors (`except: pass`) means
the pipeline reports success while loading nothing.

### 4. Why might Parquet outperform CSV for selected analytical workloads even if both contain the same rows?

Measured on the same 49,897 rows: Parquet was **5.3 MB vs 14.9 MB** for CSV,
full reads took **0.036 s vs 0.238 s**, and the `status = DELIVERED` read took
**0.029 s vs 0.237 s**. The reasons:

- **Columnar layout.** A query that needs 3 of 20 columns reads only those 3 column chunks. CSV has to scan and split every byte of every line.
- **Typed, binary values.** Timestamps, integers and floats are stored already typed. CSV stores text, so every read re-parses numbers and dates. In fact `pd.read_csv` returned the timestamps as plain strings, so even more work is needed before the data is usable.
- **Encoding and compression.** Dictionary and RLE encoding store `status` (6 distinct values) in about 19 KB for 49,897 rows, versus 7 to 9 bytes of text per row in CSV. Snappy compresses the rest.
- **Predicate pushdown and statistics.** `read_parquet(filters=[("status","==","DELIVERED")])` evaluates the filter inside the reader, and row-group min/max statistics let a reader skip whole blocks. CSV can only filter after reading everything.
- **Schema travels with the data.** There is no type inference and no guessing about delimiters or encodings.

CSV still wins when a human or a spreadsheet must open the file, when a tool
only accepts CSV, or when rows are appended one at a time.

### 5. Why is a DAG that contains all transformation logic directly considered harder to maintain?

- **Testing.** Logic inside `PythonOperator` callables can only be exercised through the Airflow runtime. Here, `clean_orders()` and `build_curated_frame()` are pure functions covered by 10 unit tests that run in under a second, no Airflow needed.
- **Reuse.** The same code has to run from a laptop (`python -m src.cli run-all`), from a container (`docker compose run pipeline …`) and from Airflow. If it lives in the DAG, the other two contexts end up with a copy.
- **Scheduler load and parse errors.** The scheduler re-parses DAG files every few seconds. Heavy imports or top-level work slow parsing, and a bug in transformation code turns into a DAG import error that hides the whole pipeline.
- **Coupling and upgrades.** Business rules become tied to Airflow's version, operators and context API. Moving to another orchestrator, or upgrading Airflow, means rewriting business logic.
- **Separation of concerns.** The DAG should answer *when*, *in what order* and *what happens on failure*. `src/` answers *what* the data should look like. In this repository each task is one line: `python -m src.cli <command>`.

### 6. How do retries interact with idempotency? Give an example where retries without idempotency cause damage.

A retry re-executes a task that may have **partially or fully succeeded**
before failing. For example, the insert committed but the process was killed
before it could report success, or a timeout fired after the database finished.
Retries are therefore only safe when running a task twice gives the same end
state as running it once, which is idempotency.

**Damage example.** Suppose `load` were a plain
`INSERT INTO sales_order_lines SELECT … FROM curated` with no primary key. It
inserts 49,897 rows, then the network drops before the commit acknowledgment
arrives. Airflow marks the attempt failed and retries, inserting another
49,897 rows. Revenue dashboards now show double sales (about 18.55 billion instead of
9.27 billion), and nobody notices until the finance numbers disagree. An
append-style `audit.partition_loads` insert would likewise record the same
partition twice.

**How this pipeline makes retries safe:**

| Task | Why rerunning it is safe |
|---|---|
| `extract` | Copies into `data/raw/run_id=<same id>/`, overwriting identical bytes. The source is read-only. |
| `transform` | Deterministically rebuilds staging, curated and quarantine from that snapshot, overwriting the previous output. |
| `load` / `load-partition` | UPSERT on `order_id` plus the `record_hash` guard, in one transaction (all or nothing). `audit.partition_loads` is also an UPSERT on `partition_key`, in the same transaction. |
| `validate` | Read-only. |
| `partition` | Written to a temporary folder and swapped in, so a rerun cannot leave duplicate part files. pyarrow gives each file a random name, so a naive rewrite would *add* files. A test covers this. |

This was demonstrated in practice: the load failed three times while
PostgreSQL was down, the database was restarted, and the load was cleared and
rerun. The table still held 49,897 rows with 49,897 distinct `order_id`s, with
no manual cleanup.

### 7. What trade-off is introduced by partitioning too aggressively?

Partitioning trades **pruning** (skip files that cannot match) for **overhead**
(more files, more metadata, less compression). Even at month granularity, the
21 partition files add up to **6.67 MB versus 5.25 MB** for one file (+27%).
Each small file repeats its own schema, footer and dictionaries, and compresses
less because it sees fewer rows (951 to 2,540 per file). Going further, to
`order_date`, `customer_id` or `order_id`, produces thousands of tiny files.
The costs of that are:

- File-listing and metadata time dominates. Opening 10,000 files is slower than scanning one large file, and object stores such as S3 charge per request.
- Each file carries per-file overhead and poor compression, and row-group statistics become useless.
- The Airflow and job-planning side gets heavier (many tasks or partitions to track), and small-file compaction jobs become necessary.
- Queries that filter on *other* columns gain nothing and pay the full overhead.

The rule of thumb is to partition on columns that almost every query filters
on, and to keep each partition large (tens to hundreds of MB per file in
production).

### 8. How would you adapt the pipeline if the source became an API or database instead of local files?

Only `src/extract/` changes. Everything downstream still reads the raw snapshot.

- **Keep the raw contract.** `extract_sources(run_id)` would call the API or database and write what it received to `data/raw/run_id=<id>/` (JSON Lines pages for an API, or a Parquet/CSV export of the query). It would still write `_manifest.json` with counts and hashes. Staging stays unchanged because it keeps reading raw.
- **Incremental extraction.** Keep a watermark such as `max(updated_at)` or a CDC position in a state table (for example `audit.extract_watermarks`), and request only `updated_at > watermark` with a small overlap window. The existing dedup on the latest `updated_at` and the `record_hash` UPSERT already absorb the overlap.
- **Use the Airflow data interval.** Query `updated_at >= {{ data_interval_start }} AND < {{ data_interval_end }}` so every run, retry and backfill of a date fetches the same slice.
- **API concerns.** Pagination (cursor or offset), rate limiting with backoff, timeouts, auth tokens kept in `.env` or an Airflow Connection (never in code), and a schema check on each response. HTTP 5xx and timeouts are system exceptions (retry). Malformed records are quarantined.
- **Database concerns.** A read-only user, a consistent snapshot (`REPEATABLE READ`, or reading from a replica), indexed filter columns, and server-side cursors or `COPY … TO STDOUT` for large extracts.
- **Configuration.** The source type, base URL and table name go in `settings.yml`. Hosts and credentials go in `.env`. `src/config.py` stays the only place that reads them.

---

## Part B: Goal 3 analysis questions (Section 9.5)

Measured benchmark (`docs/evidence/benchmark_results_sandbox.csv`, median of 5 warm runs):

| Storage | Size | Write | Full read | Filtered read (`DELIVERED`, 8,355 rows) |
|---|---:|---:|---:|---:|
| CSV | 14,897,558 B | 1.007 s | 0.238 s | 0.237 s |
| JSON Lines | 30,240,770 B | 1.205 s | 0.514 s | 0.517 s |
| Parquet (snappy) | **5,254,700 B** | **0.057 s** | **0.036 s** | **0.029 s** |
| PostgreSQL (table + PK index) | 15,974,400 B* | 0.263 s (COPY) | 0.574 s | 0.086 s |
| PostgreSQL + index on `status` | 16,670,720 B* | n/a | n/a | 0.095 s |

\* `pg_total_relation_size`: heap 14,344,192 + indexes 1,589,248 bytes. This is
not a file, and it includes page headers, per-row tuple headers (about 24
bytes each) and free space.

### 1. Which file format was smallest on your machine, and what encoding/compression characteristics help explain the result?

**Parquet, at 5.25 MB.** That is 2.8× smaller than CSV (14.9 MB) and 5.8×
smaller than JSON Lines (30.2 MB). Parquet stores each column contiguously and
picks an encoding per column. Low-cardinality text such as `status`,
`category`, `brand`, `customer_tier` and `customer_city` uses dictionary plus
RLE encoding, so each of these columns takes about 13 to 25 KB for 49,897 rows.
Numbers and timestamps are stored as 8-byte binary values rather than 20 to 32
characters of text, and snappy compresses the pages. The per-column metadata
shows the one column that resists all of this: **`record_hash` is 63% of the
Parquet file (3.3 MB)**. It is 64 random hex characters per row, which by
design has no repetition to exploit. JSON Lines is the largest (2× CSV)
because it repeats all 20 column names, quotes and braces on every one of the
49,897 lines.

### 2. Which representation was fastest for a full dataset read? Does that imply it is best for every workload?

**Parquet, at 0.036 s**, versus 0.238 s for CSV, 0.514 s for JSONL and 0.574 s
for PostgreSQL. It reads typed columnar buffers straight into Arrow and then
pandas, with no text parsing. That does **not** make it best for every
workload:

- **Point lookups and updates.** "Change the status of O0000100" means rewriting a whole Parquet file. PostgreSQL does it in milliseconds through the primary key, in a transaction.
- **Concurrent readers and writers.** PostgreSQL provides locking, MVCC, constraints and access control. Parquet files have none of these.
- **Serving many small queries** (dashboards, APIs): a database with indexes and a buffer cache wins.
- **Append/stream ingestion.** JSON Lines or CSV can be appended one line at a time. Parquet is written in batches because the footer is at the end of the file.
- **Human or tool interchange.** CSV opens anywhere.

PostgreSQL's slow *full* read here is mostly **client-side transfer**: 49,897
rows crossed the socket and were turned into Python objects (including
`Decimal`s). That is the cost of shipping every row to a client, not of the
database finding them.

### 3. How did filtered retrieval differ between Parquet and PostgreSQL? What additional PostgreSQL design (such as an index) could change the result?

Parquet's filtered read (0.029 s) was about 3× faster than PostgreSQL's
(0.086 s), even though both returned the same 8,355 rows. For PostgreSQL the
filter helped a lot relative to its own full read (0.574 s to 0.086 s, 6.7×),
because only 16.7% of rows crossed the wire. For Parquet the gain was small
(0.036 s to 0.029 s). The file has one row group, so min/max statistics cannot
skip anything, and the saving comes from materializing fewer rows.

The index experiment, recorded in `docs/evidence/postgres_explain_sandbox.txt`:

- Without an index: `Seq Scan … Rows Removed by Filter: 41,542`, 6.1 ms server execution.
- With `CREATE INDEX ON … (status)`: `Bitmap Index Scan → Bitmap Heap Scan`, 3.2 ms server execution, about 2× faster inside the server.
- **End to end, the index did not help** (0.095 s vs 0.086 s, within noise). Over 90% of the elapsed time (about 80 of 86 ms) is sending 8,355 wide rows to Python, which no index changes. `status = DELIVERED` also matches about 17% of the table, and for predicates that broad the planner still has to visit nearly every page (1,741 of 1,751 heap blocks).

PostgreSQL designs that *would* change the result:

- Push the aggregation into SQL (`SELECT SUM(net_amount) … WHERE status='DELIVERED'`) so one row is returned instead of 8,355.
- Select only the needed columns instead of `SELECT *`.
- Use a **covering index** (`… (status) INCLUDE (net_amount, order_timestamp)`) to enable index-only scans.
- Use a **partial index** or a more selective predicate (an index shines when a query returns under a few percent of rows).
- Use `CLUSTER` on `status`, or **declarative partitioning** by status or month, so the matching rows are physically together.
- Use a **BRIN index** on `order_timestamp` for date-range queries.
- Keep statistics fresh with `ANALYZE`.

### 4. Why is JSON Lines generally more pipeline-friendly than one giant JSON array for append/stream-oriented processing?

- **Appending is O(1).** A new record is one more line (`echo '{…}' >> file.jsonl`). A JSON array must stay syntactically closed (`[…]`), so appending means rewriting the closing bracket or the whole file.
- **Streaming with constant memory.** A reader can process one line at a time (`for line in f: json.loads(line)`). A standard parser must load the whole array into memory before yielding anything. `products.json` has 601 items so it doesn't matter here, but it would for 50 million.
- **Parallel and splittable.** Newline boundaries let Spark, Hadoop or `split -l` break the file into chunks for workers. An array has no safe split points.
- **Fault isolation.** One corrupt line can be quarantined while the rest are processed. One missing bracket in a giant array invalidates the entire document.
- **Tooling.** `head`, `tail -f`, `grep`, `wc -l` and log shippers all work line by line, and many streaming systems (Kafka consumers, BigQuery loads, log pipelines) emit or accept JSONL directly.

The cost is verbosity: 30 MB here, twice the size of CSV. So JSONL suits
transport and landing, while Parquet suits analytical storage.

### 5. What happens if a partition key has extremely high cardinality or poor query locality?

- **High cardinality** (for example `order_id`, `customer_id`, or timestamps to the second) leads to a **small-files problem**: tens of thousands of directories with a few rows each. Listing files dominates query time, per-file footers and dictionaries swamp the data (month partitions already cost +27% here), compression falls apart, writers keep thousands of files open, and object-store request costs explode. Metastores and Airflow/Spark planning also slow down.
- **Poor query locality** (queries rarely filter on the partition key, or each query needs many partitions) means **no pruning benefit**, only overhead. If analysts filter by `category` but data is partitioned by `order_month`, every query still opens all 21 month folders, plus the extra file overhead.
- **Skew.** A key with uneven distribution (one huge partition, many tiny ones) unbalances parallel work.
- **Mitigations.** Choose low-to-moderate-cardinality keys that match the dominant filters (year/month suits time-sliced reporting and monthly reloads). Bucket or hash high-cardinality columns, or sort within files so min/max statistics prune instead. Compact small files regularly. Aim for large files (about 128 MB to 1 GB in production).

---

## Part C: Explanations required by individual tasks

### Goal 1, Task C/E: how configuration is separated from code

- `config/settings.yml` (committed) holds **non-secret defaults**: layer paths, source file names, allowed statuses, quantity range, hash columns, benchmark repeats and page size.
- `.env` (git-ignored; the committed template is `.env.example` with placeholder values) holds **environment-specific values and secrets**: `POSTGRES_HOST/PORT/DB/USER/PASSWORD`, Airflow admin credentials and `AIRFLOW_UID`.
- `src/config.py` is the **only** module that reads either file. It uses `load_dotenv(override=False)`, so variables already set by the process win. That is how the same code connects to `localhost` on the host and to `postgres` inside Compose. `docker-compose.yml` sets `POSTGRES_HOST: postgres` for the containers, and nothing is hard-coded.
- Credentials never appear in Python, YAML, SQL or DAG files. Compose files reference `${POSTGRES_PASSWORD}` and resolve it from `.env` at runtime. `PostgresSettings.describe()` is used in error messages so the password is never logged. Checks: the Task C `git grep` for the placeholder string (excluding `.env.example`) returns nothing, and `.env` never appears in `git status`.

### Goal 4, Task B: schedule and catchup choices

- **`0 2 * * *` in Asia/Manila (daily at 02:00).** The source files are daily snapshots. 02:00 local time falls after the business day closes and before analysts arrive, when the database is quiet. Running hourly would reprocess an unchanged snapshot 23 times a day. Running weekly would leave the served table up to 7 days stale. The DAG's `start_date` carries the timezone, so the cron is evaluated in Manila time, not UTC.
- **`catchup=False`.** Every run processes the **full current snapshot**, and the load is an idempotent UPSERT. Catching up about 265 missed intervals since `start_date=2026-01-01` would run 265 identical jobs that produce the same table. Historical corrections are done deliberately with `run_mode=partition` (see the backfill section below).
- **`max_active_runs=1`.** All runs share `data/staging` and `data/curated`, and two concurrent runs could interleave writes. `retries=2` with a 1-minute exponential `retry_delay` (maximum 5 minutes) and a per-task `execution_timeout` (5 to 15 minutes, plus a 1-hour `dagrun_timeout`) mean transient problems self-heal and a hung task cannot block the DAG forever.

### Goal 4, Task E: which steps are safe to rerun, and why

Every task is safe to rerun or clear (see the table in answer 6). The failure
experiment used the recommended method: `data/source/orders.csv` was
temporarily renamed, so no source bytes were modified (checksums were
verified against `data/source/SHA256SUMS` afterwards).
`extract` failed on attempts 1, 2 and 3. `on_retry_callback` fired twice and
`on_failure_callback` once, appending a JSON record to
`logs/airflow_failures.jsonl`. The downstream tasks became `upstream_failed`.
After the file was restored, clearing `extract` (with downstream) under the
same run ID succeeded. The table still had 49,897 rows and 49,897 distinct
`order_id`s, with no manual database cleanup. The one thing that is *not*
safe is running two DAG runs at the same time over the shared staging folder,
which is why `max_active_runs=1`.

### Goal 4 optional challenge: backfill reasoning

Suppose the DAG runs daily and March 2025 must be (re)loaded.

1. **Data interval.** An Airflow run with logical date D covers `[D, D+1 day)`. In this pipeline the extract takes the full snapshot, so the interval does not select the data; the `year` and `month` params select the slice. Against a source that is filtered by time, extract should use `{{ data_interval_start }}` and `{{ data_interval_end }}` so every rerun of a date fetches exactly the same slice.
2. **The backfill here.** Trigger one run with `{"run_mode": "partition", "year": 2025, "month": 3}`, or from the CLI: `airflow dags trigger dss150p_sales_pipeline --conf '{"run_mode":"partition","year":2025,"month":3}'`. `load-partition` upserts only `order_year=2025/order_month=3`, `audit.partition_loads` records it, and `validate --year 2025 --month 3` checks only that month. For the scheduled daily runs themselves, `airflow dags backfill -s 2025-03-01 -e 2025-03-31 dss150p_sales_pipeline` would create one run per day. That is 31 full reprocessings, which is why the partition parameter is the better tool for this pipeline.
3. **Avoiding double loads.** This is guaranteed by idempotency, not by care. Rows are keyed on `order_id` with the `record_hash` guard, so loading March twice, or loading March and then a full load, leaves exactly one row per order. The partition audit row is keyed on `partition_key` and updated in place. `max_active_runs=1` keeps a backfill run from colliding with the nightly run.
4. **Verification.** Compare the partition's row count in PostgreSQL with the Parquet partition and with `audit.partition_loads.row_count`, and check `COUNT(*) = COUNT(DISTINCT order_id)`.

---

## Part D: Technical reflection

**Modularity.** Each layer has one job and a small interface. `extract` copies
bytes. `transform.staging` cleans one source at a time. `transform.curated`
joins and computes. `load` persists. `validate` asserts. `benchmark`
materializes formats. `cli.py` is a thin router that every runtime (laptop,
Docker, Airflow) calls the same way. The payoff showed up during the lab: the
staging rules are pure functions with unit tests, and the Airflow DAG's task definitions are about 40 lines because they contain
no data logic.

**Idempotency.** Every step can run twice safely: run-specific raw folders,
overwritten staging output, a hash-guarded UPSERT, an UPSERTed audit table,
and atomic partition rewrites. This is what makes retries, "clear task" and
backfills routine instead of risky. The measurable result is that three
consecutive loads and a new pipeline run all left 49,897 rows and 49,897
distinct `order_id`s, touching 0 rows after the first.

**Storage trade-offs.** No format won everywhere. Parquet was smallest and
fastest for analytical scans. PostgreSQL was the right serving layer
(constraints, UPSERT, concurrent access, indexes), but a `SELECT *` into Python
is dominated by transfer cost. CSV and JSONL are for interchange and landing,
not analysis. Partitioning helps only when queries filter on the key, and even
monthly partitions cost 27% extra bytes on this dataset.

**Orchestration vs business logic.** Airflow decides *when* and *in what
order*, *how often to retry*, *how long to wait* and *whom to tell*. `src/`
decides *what correct data is*. Keeping that line means the pipeline runs and
is tested without Airflow, and the DAG can change (schedule, retries, a
partition mode) without touching a single transformation rule.
