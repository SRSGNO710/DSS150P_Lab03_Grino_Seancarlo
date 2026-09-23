"""
Partitioned Parquet: data/partitioned/order_year=YYYY/order_month=M/*.parquet

Format materialization only — the rows are the curated rows unchanged, plus two
derived partition columns (UTC year/month of order_timestamp).
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd

from src.config import SETTINGS, path_for
from src.errors import BenchmarkError, get_logger

log = get_logger(__name__)
PARTITION_COLS = ["order_year", "order_month"]


def add_partition_columns(df: pd.DataFrame) -> pd.DataFrame:
    ts = pd.to_datetime(df["order_timestamp"], utc=True)
    return df.assign(order_year=ts.dt.year.astype("int32"), order_month=ts.dt.month.astype("int32"))


def write_partitioned_parquet(df: pd.DataFrame | None = None, output_dir: Path | None = None) -> dict:
    """Rewrite the whole partitioned dataset atomically. pyarrow names files with
    random UUIDs, so writing into an existing tree would ADD files and duplicate
    rows on rerun; instead we write to a temp folder and swap it in."""
    output_dir = Path(output_dir or path_for("partitioned"))
    if df is None:
        df = pd.read_parquet(SETTINGS.curated_file)
    df = add_partition_columns(df)

    tmp = output_dir.with_name(output_dir.name + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    try:
        df.to_parquet(tmp, partition_cols=PARTITION_COLS, index=False, compression="snappy")
    except (OSError, ImportError, ValueError) as err:
        shutil.rmtree(tmp, ignore_errors=True)
        raise BenchmarkError(f"Cannot write partitioned Parquet: {err}") from err
    for child in output_dir.glob("order_year=*"):
        shutil.rmtree(child)
    output_dir.mkdir(parents=True, exist_ok=True)
    for child in tmp.iterdir():
        child.rename(output_dir / child.name)
    tmp.rmdir()

    counts = df.groupby(PARTITION_COLS).size()
    partitions = {f"order_year={y}/order_month={m}": int(n) for (y, m), n in counts.items()}
    log.info("Wrote %d partitions (%d rows) under %s", len(partitions), len(df), output_dir)
    return {"output_dir": str(output_dir), "partition_count": len(partitions),
            "rows": len(df), "partitions": partitions}


def partition_path(year: int, month: int, base: Path | None = None) -> Path:
    return Path(base or path_for("partitioned")) / f"order_year={year}" / f"order_month={month}"


def read_partition(year: int, month: int, base: Path | None = None) -> pd.DataFrame:
    """Read ONLY the files of one partition directory (other months are never opened)."""
    path = partition_path(year, month, base)
    if not path.is_dir() or not any(path.glob("*.parquet")):
        raise FileNotFoundError(path)
    df = pd.read_parquet(path)  # partition keys live in the path, not in the files
    return df.assign(order_year=year, order_month=month)


def verify_partition(df: pd.DataFrame, year: int, month: int) -> dict:
    ts = pd.to_datetime(df["order_timestamp"], utc=True)
    wrong = int(((ts.dt.year != year) | (ts.dt.month != month)).sum())
    return {"partition": f"order_year={year}/order_month={month}", "rows": len(df),
            "rows_outside_partition": wrong, "ok": wrong == 0 and len(df) > 0}


def partition_tree(base: Path | None = None, max_years: int = 10) -> str:
    base = Path(base or path_for("partitioned"))
    lines = [f"{base.relative_to(base.parent.parent) if base.parent.parent in base.parents else base}/"]
    for ydir in sorted(base.glob("order_year=*"))[:max_years]:
        lines.append(f"  {ydir.name}/")
        for mdir in sorted(ydir.glob("order_month=*"), key=lambda p: int(p.name.split("=")[1])):
            files = list(mdir.glob("*.parquet"))
            lines.append(f"    {mdir.name}/  ({len(files)} file, {sum(f.stat().st_size for f in files):,} bytes)")
    return "\n".join(lines)
