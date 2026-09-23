"""
Storage benchmark: the SAME curated logical dataset materialized as CSV,
JSON Lines, Parquet (snappy) and PostgreSQL, measured the same way.

For every representation:
  write_seconds          median of `repeats` full writes
  full_read_seconds      median of `repeats` full reads into a pandas DataFrame
  filtered_read_seconds  median of `repeats` reads returning status = DELIVERED
  file_size_bytes        on-disk bytes (files) / pg_total_relation_size (table+indexes+TOAST)

The first (cold) read of each format is discarded as warm-up, so medians
compare warm-cache behaviour. Every individual timing is kept in
benchmark_timings.csv, and hardware/OS/library versions in
benchmark_environment.json, because results are machine-dependent.
"""
from __future__ import annotations

import io
import json
import os
import platform
import statistics
import time
from pathlib import Path

import pandas as pd

from src.config import SETTINGS, path_for
from src.errors import BenchmarkError, get_logger

log = get_logger(__name__)

RESULT_COLUMNS = ["storage_type", "file_size_bytes", "write_seconds", "full_read_seconds",
                  "filtered_read_seconds", "row_count", "notes"]


def _timed(fn, repeats: int, label: str, timings: list, warmup: bool = True):
    if warmup:
        fn()
    values, result = [], None
    for i in range(repeats):
        start = time.perf_counter()
        result = fn()
        values.append(time.perf_counter() - start)
        timings.append({"measurement": label, "repeat": i + 1, "seconds": values[-1]})
    return statistics.median(values), result


def environment_info() -> dict:
    cpu = platform.processor() or ""
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as fh:
            cpu = next((l.split(":", 1)[1].strip() for l in fh if l.startswith("model name")), cpu)
    except OSError as err:  # /proc is Linux-only; fall back to platform.processor()
        log.debug("CPU model not readable from /proc/cpuinfo (%s); using %r", err, cpu)
    try:
        ram_gb = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1024 ** 3, 1)
    except (ValueError, OSError, AttributeError):
        ram_gb = None
    import pyarrow

    return {"os": platform.platform(), "cpu": cpu, "logical_cpus": os.cpu_count(), "ram_gb": ram_gb,
            "python": platform.python_version(), "pandas": pd.__version__, "pyarrow": pyarrow.__version__}


# ------------------------------------------------------------------ file formats
def _bench_files(df: pd.DataFrame, out: Path, repeats: int, status: str, timings: list) -> list[dict]:
    csv_path = out / "sales_order_lines.csv"
    jsonl_path = out / "sales_order_lines.jsonl"
    pq_path = out / "sales_order_lines.parquet"

    formats = {
        "csv": (csv_path,
                lambda: df.to_csv(csv_path, index=False),
                lambda: pd.read_csv(csv_path),
                lambda: (lambda d: d[d["status"] == status])(pd.read_csv(csv_path)),
                "Text; every read re-parses and re-infers types (timestamps stay strings unless parsed). "
                "No pushdown: filter = read everything then filter in pandas."),
        "jsonl": (jsonl_path,
                  lambda: df.to_json(jsonl_path, orient="records", lines=True, date_format="iso"),
                  lambda: pd.read_json(jsonl_path, lines=True),
                  lambda: (lambda d: d[d["status"] == status])(pd.read_json(jsonl_path, lines=True)),
                  "One self-describing JSON object per line: column names repeated on every row, "
                  "so most verbose; slowest to parse; append/stream friendly. No pushdown."),
        "parquet": (pq_path,
                    lambda: df.to_parquet(pq_path, index=False, compression="snappy"),
                    lambda: pd.read_parquet(pq_path),
                    lambda: pd.read_parquet(pq_path, filters=[("status", "==", status)]),
                    "Columnar + dictionary/RLE encoding + snappy; typed schema stored in file. "
                    "Filter pushed into the reader (filters=), row-group statistics can skip data."),
    }
    rows, frames = [], {}
    for name, (path, write, read_full, read_filtered, note) in formats.items():
        write_s, _ = _timed(write, repeats, f"{name}.write", timings, warmup=False)
        full_s, full_df = _timed(read_full, repeats, f"{name}.full_read", timings)
        filt_s, filt_df = _timed(read_filtered, repeats, f"{name}.filtered_read", timings)
        frames[name] = full_df
        rows.append({"storage_type": name, "file_size_bytes": path.stat().st_size,
                     "write_seconds": write_s, "full_read_seconds": full_s,
                     "filtered_read_seconds": filt_s, "row_count": len(full_df),
                     "notes": f"{note} filtered_rows={len(filt_df)}; median of {repeats}"})
    # Same logical row set in every file format?
    ids = {n: set(f["order_id"].astype(str)) for n, f in frames.items()}
    if len({len(f) for f in frames.values()}) != 1 or any(v != ids["parquet"] for v in ids.values()):
        raise BenchmarkError("CSV/JSONL/Parquet do not contain the same logical row set")
    return rows


# ------------------------------------------------------------------ PostgreSQL
def _bench_postgres(df: pd.DataFrame, out: Path, repeats: int, status: str, timings: list) -> list[dict]:
    from src.db import connect

    table = "curated.sales_order_lines"
    bench = "staging.benchmark_sales_order_lines"
    cols = [c for c in df.columns]
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    csv_text = buf.getvalue()

    conn = connect(BenchmarkError)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            pg_version = cur.fetchone()[0].split(" on ")[0]

            def query(sql, params=None, target=table):
                def run():
                    cur.execute(sql.format(t=target), params)
                    return pd.DataFrame(cur.fetchall())
                return run

            # write: bulk COPY of the same rows into an empty copy of the table
            cur.execute(f"DROP TABLE IF EXISTS {bench}; CREATE TABLE {bench} (LIKE {table} INCLUDING ALL)")

            def copy_write():
                cur.execute(f"TRUNCATE {bench}")
                cur.copy_expert(f"COPY {bench} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv)",
                                io.StringIO(csv_text))
            write_s, _ = _timed(copy_write, repeats, "postgresql.write_copy", timings, warmup=False)
            cur.execute(f"ANALYZE {bench}; ANALYZE {table}")

            full_s, full = _timed(query("SELECT * FROM {t}"), repeats, "postgresql.full_read", timings)
            filt_sql = "SELECT * FROM {t} WHERE status = %s"
            filt_s, filt = _timed(query(filt_sql, (status,)), repeats, "postgresql.filtered_read", timings)
            cur.execute(f"SELECT pg_total_relation_size('{table}'), pg_relation_size('{table}'), "
                        f"pg_indexes_size('{table}')")
            total, heap, idx = cur.fetchone()
            cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM {table} WHERE status = %s", (status,))
            plan_no_index = "\n".join(r[0] for r in cur.fetchall())

            # Index experiment on the benchmark copy (the served table is left as designed)
            cur.execute(f"CREATE INDEX benchmark_status_idx ON {bench} (status); ANALYZE {bench}")
            idx_s, idx_rows = _timed(query(filt_sql, (status,), bench), repeats,
                                     "postgresql_status_index.filtered_read", timings)
            cur.execute(f"EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM {bench} WHERE status = %s", (status,))
            plan_index = "\n".join(r[0] for r in cur.fetchall())
            cur.execute(f"SELECT pg_total_relation_size('{bench}')")
            bench_total = cur.fetchone()[0]
            cur.execute(f"DROP TABLE {bench}")
    except BenchmarkError:
        raise
    except Exception as err:  # psycopg2 errors, re-raised with context
        raise BenchmarkError(f"PostgreSQL benchmark failed: {err}") from err
    finally:
        conn.close()

    (out / "postgres_explain.txt").write_text(
        f"-- {pg_version}\n-- {table} (primary key on order_id only)\n{plan_no_index}\n\n"
        f"-- {bench} with an added index on status\n{plan_index}\n", encoding="utf-8")
    if len(full) != len(df):
        raise BenchmarkError(f"PostgreSQL returned {len(full)} rows, curated has {len(df)}; run `load` first")
    return [
        {"storage_type": "postgresql", "file_size_bytes": total, "write_seconds": write_s,
         "full_read_seconds": full_s, "filtered_read_seconds": filt_s, "row_count": len(full),
         "notes": f"{pg_version}. Size = pg_total_relation_size (heap {heap} + indexes {idx} bytes), not a file. "
                  f"Write = COPY into an empty copy of the table. Reads = query + fetch over a local socket "
                  f"into pandas. Filter uses PK-only table (sequential scan). filtered_rows={len(filt)}; "
                  f"median of {repeats}"},
        {"storage_type": "postgresql_status_index", "file_size_bytes": bench_total, "write_seconds": "",
         "full_read_seconds": "", "filtered_read_seconds": idx_s, "row_count": len(full),
         "notes": f"Experiment: same rows + B-tree index on status. filtered_rows={len(idx_rows)}; "
                  f"see postgres_explain.txt for the chosen plan; median of {repeats}"},
    ]


# ------------------------------------------------------------------ entry point
def run_benchmark(curated_path: Path | None = None, output_dir: Path | None = None,
                  repeats: int | None = None, include_postgres: bool = True) -> dict:
    curated_path = Path(curated_path or SETTINGS.curated_file)
    out = Path(output_dir or path_for("benchmarks"))
    repeats = repeats or SETTINGS.benchmark_repeats
    if repeats < 5:
        log.warning("repeats=%d is below the required minimum of 5", repeats)
    status = SETTINGS.benchmark_filter_status
    out.mkdir(parents=True, exist_ok=True)
    try:
        df = pd.read_parquet(curated_path)
    except (OSError, ValueError) as err:
        raise BenchmarkError(f"Cannot read curated dataset {curated_path}: {err}") from err

    timings: list = []
    rows = _bench_files(df, out, repeats, status, timings)
    if include_postgres:
        rows += _bench_postgres(df, out, repeats, status, timings)

    results = pd.DataFrame(rows, columns=RESULT_COLUMNS)
    for col in ("write_seconds", "full_read_seconds", "filtered_read_seconds"):
        results[col] = results[col].map(lambda v: f"{v:.4f}" if isinstance(v, float) else v)
    results.to_csv(out / "benchmark_results.csv", index=False)
    pd.DataFrame(timings).to_csv(out / "benchmark_timings.csv", index=False)
    env = {**environment_info(), "repeats": repeats, "filter": f"status = {status}",
           "rows": len(df), "note": "first read of each format discarded as warm-up"}
    (out / "benchmark_environment.json").write_text(json.dumps(env, indent=2), encoding="utf-8")
    log.info("Benchmark written to %s", out / "benchmark_results.csv")
    return {"results_csv": str(out / "benchmark_results.csv"), "environment": env,
            "results": results.to_dict(orient="records")}


# The starter placed write_partitioned_parquet in this module; keep that import path working.
from src.benchmark.partitioning import write_partitioned_parquet  # noqa: E402,F401
