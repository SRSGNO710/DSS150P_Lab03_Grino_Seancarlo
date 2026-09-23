""" 
PostgreSQL persistence only: rerun-safe UPSERT of already-curated rows.
No source cleaning or business calculations happen here.

Idempotency contract
* ON CONFLICT (order_id): a rerun can never create a second row per order.
* ... DO UPDATE ... WHERE target.record_hash IS DISTINCT FROM EXCLUDED.record_hash:
  a row is rewritten only when its business content actually changed, so an
  identical rerun touches 0 rows (no churn, no new dead tuples, audit columns
  keep pointing at the run that last changed the row).
* RETURNING (xmax = 0) tells inserted vs updated rows apart; rows skipped by the
  WHERE clause are not returned, which gives the "unchanged" count.
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from src.audit import utc_now_iso
from src.config import SETTINGS, path_for
from src.db import connect
from src.errors import LoadError, get_logger
from src.transform.curated import CURATED_COLUMNS

log = get_logger(__name__)

_UPDATE_COLS = [c for c in CURATED_COLUMNS if c != "order_id"]

UPSERT_SQL = (
    f"INSERT INTO curated.sales_order_lines ({', '.join(CURATED_COLUMNS)}) VALUES %s "
    f"ON CONFLICT (order_id) DO UPDATE SET "
    + ", ".join(f"{c} = EXCLUDED.{c}" for c in _UPDATE_COLS)
    + " WHERE curated.sales_order_lines.record_hash IS DISTINCT FROM EXCLUDED.record_hash "
    "RETURNING (xmax = 0) AS inserted"
)


def _py(value):
    """numpy/pandas scalars -> plain Python objects psycopg2 can adapt."""
    if value is None or value is pd.NaT:
        return None
    if isinstance(value, float) and np.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.generic):
        return value.item()
    return value


def dataframe_rows(df: pd.DataFrame) -> list[tuple]:
    return [tuple(_py(v) for v in row) for row in df[CURATED_COLUMNS].itertuples(index=False, name=None)]


def upsert_rows(df: pd.DataFrame, after=None) -> dict:
    """Upsert curated rows in ONE transaction (all-or-nothing) and return
    inserted/updated/unchanged counts. `after(cur, stats)` runs inside the same
    transaction (used to write the audit row atomically with the data)."""
    from psycopg2.extras import execute_values

    if df[SETTINGS.conflict_key].duplicated().any():
        raise LoadError("Input contains duplicate order_id values; refusing to load")
    rows = dataframe_rows(df)
    conn = connect(LoadError)
    try:
        with conn:  # commit on success, rollback on any error
            with conn.cursor() as cur:
                result = execute_values(cur, UPSERT_SQL, rows,
                                        page_size=SETTINGS.load_page_size, fetch=True)
                inserted = sum(1 for (flag,) in result if flag)
                stats = {"rows_in": len(rows), "inserted": inserted, "updated": len(result) - inserted,
                         "unchanged": len(rows) - len(result)}
                if after is not None:
                    after(cur, stats)
    except LoadError:
        raise
    except Exception as err:  # psycopg2.Error, adaptation errors: re-raised with context
        raise LoadError(f"UPSERT into curated.sales_order_lines failed and was rolled back: {err}",
                        rows=len(rows)) from err
    finally:
        conn.close()
    return stats


def _record_pipeline_run(summary: dict, stats: dict, status: str) -> None:
    conn = connect(LoadError)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit.pipeline_runs (pipeline_run_id, started_at_utc, completed_at_utc, status,
                                                 rows_staging, rows_curated, rows_quarantined, message)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pipeline_run_id) DO UPDATE SET
                  completed_at_utc = EXCLUDED.completed_at_utc, status = EXCLUDED.status,
                  rows_staging = EXCLUDED.rows_staging, rows_curated = EXCLUDED.rows_curated,
                  rows_quarantined = EXCLUDED.rows_quarantined, message = EXCLUDED.message
                """,
                (summary["pipeline_run_id"], summary["started_at_utc"], utc_now_iso(), status,
                 summary.get("rows_staging"), summary.get("rows_curated"),
                 summary.get("rows_quarantined"), json.dumps(stats)),
            )
    finally:
        conn.close()


def load_curated() -> dict:
    path = SETTINGS.curated_file
    if not path.exists():
        raise LoadError(f"Curated dataset not found: {path}. Run `python -m src.cli run-all` first.")
    try:
        df = pd.read_parquet(path)
    except (OSError, ImportError, ValueError) as err:
        raise LoadError(f"Cannot read {path}: {err}") from err

    stats = upsert_rows(df)
    summary_path = path_for("curated") / "_run_summary.json"
    if summary_path.exists():
        with open(summary_path, encoding="utf-8") as fh:
            summary = json.load(fh)
        _record_pipeline_run(summary, stats, "loaded")
        stats["pipeline_run_id"] = summary["pipeline_run_id"]
    log.info("Full load: %s", stats)
    return stats


def load_partition(year: int, month: int, run_id: str) -> dict:
    """Load ONE Parquet partition (order_year=Y/order_month=M) with the same
    rerun-safe UPSERT, and record it in audit.partition_loads in the same
    transaction. Rerunning the same partition updates the audit row in place
    and leaves business rows deduplicated."""
    from src.benchmark.partitioning import partition_path, read_partition, verify_partition

    try:
        df = read_partition(year, month)
    except FileNotFoundError as err:
        raise LoadError(f"Partition {partition_path(year, month)} does not exist. Run "
                        "`python -m src.cli partition` (or benchmark) after run-all.", year=year, month=month) from err
    check = verify_partition(df, year, month)
    if not check["ok"]:
        raise LoadError(f"Partition content check failed: {check}")
    key = check["partition"]

    def audit(cur, stats):
        cur.execute(
            """
            INSERT INTO audit.partition_loads (partition_key, loaded_at_utc, row_count, pipeline_run_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (partition_key) DO UPDATE SET loaded_at_utc = EXCLUDED.loaded_at_utc,
              row_count = EXCLUDED.row_count, pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            (key, utc_now_iso(), stats["rows_in"], run_id),
        )

    stats = upsert_rows(df[CURATED_COLUMNS], after=audit)
    stats.update({"partition_key": key, "pipeline_run_id": run_id})
    log.info("Partition load: %s", stats)
    return stats
