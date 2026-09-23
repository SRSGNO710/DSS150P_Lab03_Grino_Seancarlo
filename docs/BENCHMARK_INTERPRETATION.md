# Goal 3: Storage Benchmark Interpretation 

**Dataset.** The curated `sales_order_lines`: 49,897 rows × 20 columns, the same logical row set in every representation (the benchmark raises an error if the CSV, JSONL and Parquet `order_id` sets differ).
**Command.** `python -m src.cli benchmark --repeats 5`
**Outputs.** `data/benchmarks/benchmark_results.csv`, `benchmark_timings.csv` (every run), `benchmark_environment.json`, `postgres_explain.txt`. Committed copies from the verification run are in `docs/evidence/*_sandbox.*`.
**Machine.** 2 vCPU Intel Xeon @ 2.10 GHz, 7.8 GB RAM, Linux (Ubuntu 24.04), Python 3.11.15, pandas 3.0.2, pyarrow 25.0.1, PostgreSQL 16.13 on the same host (local socket).

## Method

- **Write.** Median of 5 full writes. For files this is the whole file from a DataFrame. For PostgreSQL it is a bulk `COPY` into an empty copy of the served table (`LIKE … INCLUDING ALL`, with the same PK index).
- **Full read.** Median of 5 reads of all rows into a pandas DataFrame. For PostgreSQL this is `SELECT *` plus `fetchall()`.
- **Filtered read.** Median of 5 reads returning `status = 'DELIVERED'` (8,355 rows in every system). CSV and JSONL must read everything and then filter in pandas. Parquet uses `filters=` (pushdown into the reader). PostgreSQL uses `WHERE status = %s`.
- **Warm-up.** One untimed read before each series, so the medians compare warm-cache behavior consistently.
- **Size.** On-disk bytes for files. For PostgreSQL, `pg_total_relation_size` (heap + indexes + TOAST), because a server table is not a single file.

## Results

| Storage | Size (bytes) | Write (s) | Full read (s) | Filtered read (s) | Rows |
|---|---:|---:|---:|---:|---:|
| CSV | 14,897,558 | 1.007 | 0.238 | 0.237 | 49,897 |
| JSON Lines | 30,240,770 | 1.205 | 0.514 | 0.517 | 49,897 |
| Parquet (snappy) | **5,254,700** | **0.057** | **0.036** | **0.029** | 49,897 |
| PostgreSQL | 15,974,400 (heap 14.3 MB + PK index 1.6 MB) | 0.263 | 0.574 | 0.086 | 49,897 |
| PostgreSQL + `status` index | 16,670,720 | n/a | n/a | 0.095 | 49,897 |

## Interpretation

**CSV (parsing and typing).** Text is universal but every read re-tokenizes and re-infers types. The 49,897 timestamps came back as strings, so the 0.24 s read is not even the full cost of getting usable data. There is no way to skip rows or columns, so the filtered read costs the same as the full read (0.237 vs 0.238 s).

**JSON Lines (verbosity).** At 30.2 MB it is twice the size of CSV, because every line repeats all 20 key names plus quotes and braces. It is also the slowest to parse (0.51 s). Its strengths lie elsewhere: each line is independent, so it can be appended, streamed, split across workers, and have a single bad line quarantined. That makes it a good landing and transport format, not an analytical one.

**Parquet (columnar and compression).** It was the smallest (2.8× smaller than CSV) and the fastest to write and read (6.6× faster than CSV on a full read). Columns are stored contiguously with per-column dictionary and RLE encoding plus snappy compression. `status`, with 6 distinct values, takes about 19 KB for 49,897 rows. The file's column metadata shows where the bytes go: **63% of the file is `record_hash`**, random 64-character hex that cannot compress by design. Without the audit hash the file would be about 2 MB. Filtered reads gained less (0.036 to 0.029 s) because the file has a single row group, so min/max statistics cannot skip anything. With many row groups, or data sorted by `status`, whole blocks would be skipped.

**PostgreSQL (indexes, query engine, concurrency).** Its footprint (16.0 MB) is larger than CSV. Each row carries a tuple header (about 24 bytes) and each 8 KB page has free space, and the primary-key B-tree adds 1.6 MB. The `COPY` bulk load (0.26 s) was faster than writing CSV or JSONL from pandas. The slowest number, the full read at 0.57 s, measures moving 49,897 rows over a socket into Python objects, not finding them: the server executes the query in a few milliseconds. The filter shows the engine's value. Only 16.7% of the rows are shipped, so the filtered read is 6.7× faster than the full read. Adding a B-tree index on `status` halved server execution (Seq Scan 6.1 ms to Bitmap Index/Heap Scan 3.2 ms), yet end to end it was **no faster** (0.095 s vs 0.086 s). The predicate matches 17% of the table, so nearly every page is still visited, and transfer to the client dominates. Indexes pay off for selective predicates, aggregations pushed into SQL, and covering or partial indexes. Beyond speed, PostgreSQL is the only option here with constraints, transactions (the load is all-or-nothing), UPSERT, concurrent readers and writers, and access control. That is why it is the serving layer even though Parquet wins the scans.

**No universal winner.** Size alone does not measure quality. JSONL's size buys appendability, and PostgreSQL's buys transactions and indexes. The right choice depends on the workload: Parquet for analytical scans and archival, PostgreSQL for serving, updates and concurrency, JSONL or CSV for interchange and streaming ingestion.

## Partitioned Parquet: how partitioning reduces I/O

`python -m src.cli benchmark` (and `partition`) writes `data/partitioned/order_year=YYYY/order_month=M/*.parquet`. That gives 21 partitions from 2025-01 to 2026-09, based on the **UTC** month of `order_timestamp` (tree in `docs/evidence/partition_tree_sandbox.txt`). Reading `order_year=2026/order_month=1` opens one 334 KB file with 2,506 rows, all verified to have a 2026-01 timestamp. The other 20 files (about 6.3 MB) are never opened.

Because the partition key is encoded in the **directory path**, an engine can decide which files to read before touching any data. This is partition pruning. A query for January 2026 reads 1/21 of the data. Monthly reloads, retention ("drop 2025-01"), and targeted loads (`load-partition --year 2026 --month 1`) become folder-level operations. The benefit only exists when queries filter on the partition key. Partitioning also has a cost: the 21 small files total **6.67 MB versus 5.25 MB** for one file (+27%), because every file repeats its schema, footer and dictionaries and compresses fewer rows. Keys with higher cardinality (day, customer, order) would multiply that overhead into a small-files problem.

## Limitations

- These are single-machine, warm-cache, single-client measurements on a small (50k-row) dataset. Most measurements varied by 4 to 17% between repetitions (max minus min, divided by the median). The very short Parquet reads (about 30 ms) each had one outlier above 2× the median, probably a garbage-collection or scheduler pause, which is why **medians rather than means** are reported. All repetitions are in `benchmark_timings.csv`. Ratios can change at 50 M rows or on cold storage.
- PostgreSQL timings include client transfer and Python object creation, while file timings include pandas conversion. Each figure is "time until the data is in a DataFrame", not raw engine speed.
- Only one Parquet codec (snappy) and one row-group layout were tested. `zstd` would shrink the file further, at some CPU cost.
- The measurements here come from the verification sandbox. Numbers from your machine will differ, and the ranking is what the analysis relies on.
