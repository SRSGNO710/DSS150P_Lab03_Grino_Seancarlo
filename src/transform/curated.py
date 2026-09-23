"""
Curated layer: cross-source joins, business calculations, consumer-shaped
`sales_order_lines`, and audit columns.

Orders are joined to *valid* staged customers/products. Orders whose reference
does not resolve are never dropped silently; they go to
data/quarantine/curated_orders.csv with one of these reasons:
  orphan_customer_id               customer id does not exist in the source at all
  customer_quarantined_in_staging  customer exists but was rejected in staging
  orphan_product_id                product id does not exist in the source at all
  product_quarantined_in_staging   product exists but was rejected in staging
                                   (e.g. the catalog row had a negative price)
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from src.audit import record_hash, utc_now_iso
from src.config import SETTINGS, path_for
from src.errors import TransformError, get_logger
from src.transform.staging import QUARANTINE_REASON, _reasons, _split, write_quarantine

log = get_logger(__name__)

CURATED_COLUMNS = [
    "order_id", "customer_id", "product_id", "order_timestamp",
    "customer_city", "customer_tier", "product_name", "category", "brand",
    "quantity", "unit_price", "discount_pct",
    "gross_amount", "discount_amount", "net_amount", "status",
    "source_updated_at", "pipeline_run_id", "processed_at_utc", "record_hash",
]


def _canonical(value):
    """Stable text form for hashing, independent of pandas/numpy dtypes and of
    float repr noise (e.g. 3 vs 3.0 vs np.int64(3))."""
    if value is None or (isinstance(value, float) and np.isnan(value)) or value is pd.NaT:
        return None
    if isinstance(value, pd.Timestamp):
        return value.tz_convert("UTC").isoformat() if value.tzinfo else value.isoformat()
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)):
        return str(int(value))
    if isinstance(value, (float, np.floating)):
        return f"{float(value):.4f}"
    return str(value)


def add_record_hash(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    records = df[columns].to_dict(orient="records")
    return pd.Series([record_hash({k: _canonical(v) for k, v in r.items()}, columns) for r in records],
                     index=df.index)


def calculate_amounts(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["gross_amount"] = (df["quantity"] * df["unit_price"]).round(2)
    df["discount_amount"] = (df["gross_amount"] * df["discount_pct"]).round(2)
    df["net_amount"] = (df["gross_amount"] - df["discount_amount"]).round(2)
    return df


def build_curated_frame(orders, customers, products, run_id,
                        quarantined_customer_ids=frozenset(), quarantined_product_ids=frozenset(),
                        processed_at: str | None = None):
    """Pure function: staged frames in -> (curated, quarantine)."""
    processed_at = processed_at or utc_now_iso()
    cust = customers[["customer_id", "city", "customer_tier", "updated_at"]].rename(
        columns={"city": "customer_city", "updated_at": "customer_updated_at"})
    prod = products[["product_id", "name", "category_name", "brand", "updated_at"]].rename(
        columns={"name": "product_name", "category_name": "category", "updated_at": "product_updated_at"})

    joined = (orders.drop(columns=["pipeline_run_id", "staged_at_utc"], errors="ignore")
              .merge(cust, on="customer_id", how="left", indicator="_cust")
              .merge(prod, on="product_id", how="left", indicator="_prod"))
    no_cust = joined["_cust"] == "left_only"
    no_prod = joined["_prod"] == "left_only"
    cust_q = joined["customer_id"].isin(quarantined_customer_ids)
    prod_q = joined["product_id"].isin(quarantined_product_ids)

    reasons = _reasons({
        "orphan_customer_id": no_cust & ~cust_q,
        "customer_quarantined_in_staging": no_cust & cust_q,
        "orphan_product_id": no_prod & ~prod_q,
        "product_quarantined_in_staging": no_prod & prod_q,
    }, joined.index)
    joined = joined.drop(columns=["_cust", "_prod"])
    valid, quarantine = _split(joined, reasons)

    valid = calculate_amounts(valid)
    # Freshest contributing source update. Pairwise comparison instead of a row-wise
    # .max(axis=1), which is unreliable on tz-aware columns in older pandas versions.
    freshest = valid["updated_at"]
    for col in ("customer_updated_at", "product_updated_at"):
        freshest = freshest.where(freshest >= valid[col], valid[col])
    valid["source_updated_at"] = freshest
    valid["pipeline_run_id"] = run_id
    valid["processed_at_utc"] = pd.Timestamp(processed_at)
    valid["record_hash"] = add_record_hash(valid, SETTINGS.hash_columns)
    curated = valid[CURATED_COLUMNS].sort_values("order_id").reset_index(drop=True)
    return curated, quarantine


def _read_quarantined_ids(name: str, key: str) -> set:
    path = path_for("quarantine") / f"staging_{name}.csv"
    if not path.exists() or path.stat().st_size == 0:
        return set()
    try:
        return set(pd.read_csv(path, usecols=[key], dtype=str)[key].dropna())
    except (ValueError, pd.errors.EmptyDataError):
        return set()


def run_curated(run_id: str) -> dict:
    staging = path_for("staging")
    try:
        orders = pd.read_parquet(staging / "orders.parquet")
        customers = pd.read_parquet(staging / "customers.parquet")
        products = pd.read_parquet(staging / "products.parquet")
    except (OSError, ImportError, ValueError) as err:
        raise TransformError(f"Cannot read staging Parquet from {staging}: {err}", run_id=run_id) from err

    curated, quarantine = build_curated_frame(
        orders, customers, products, run_id,
        quarantined_customer_ids=_read_quarantined_ids("customers", "customer_id"),
        quarantined_product_ids=_read_quarantined_ids("products", "product_id"),
    )
    out = SETTINGS.curated_file
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        curated.to_parquet(out, index=False)
    except (OSError, ImportError, ValueError) as err:
        raise TransformError(f"Cannot write {out}: {err}", run_id=run_id) from err
    write_quarantine(quarantine, "curated_orders", run_id, "curated")

    summary = {
        "staged_orders_in": len(orders),
        "curated_rows": len(curated),
        "quarantined_rows": len(quarantine),
        "quarantine_reasons": quarantine[QUARANTINE_REASON].value_counts().to_dict() if len(quarantine) else {},
        "net_amount_total": round(float(curated["net_amount"].sum()), 2),
    }
    log.info("Curated complete: %s", summary)
    return summary


def write_run_summary(run_id: str, started_at: str, raw_dir, staging: dict, curated: dict) -> dict:
    summary = {
        "pipeline_run_id": run_id,
        "started_at_utc": started_at,
        "completed_at_utc": utc_now_iso(),
        "raw_dir": str(raw_dir),
        "staging": staging,
        "curated": curated,
        "rows_staging": sum(v["staged_rows"] for v in staging.values()),
        "rows_curated": curated["curated_rows"],
        "rows_quarantined": sum(v["quarantined_rows"] for v in staging.values()) + curated["quarantined_rows"],
    }
    with open(path_for("curated") / "_run_summary.json", "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)
    return summary
