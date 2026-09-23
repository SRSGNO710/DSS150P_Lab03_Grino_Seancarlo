"""
Staging layer: typing, normalization, source-level deduplication and
record-level validity checks. One source at a time — no cross-source joins
(that is curated's job) and no Airflow code.

Each `clean_*` function is pure (DataFrame in -> (clean, quarantine) out) so the
rules can be unit-tested; `run_staging` does the file I/O.

Order of operations for every dataset:
  1. trim text, parse timestamps as UTC
  2. rows whose business key or updated_at is unusable -> quarantine (cannot dedup them)
  3. deduplicate: keep the latest updated_at per business key (ties -> later source row)
  4. validity rules on the surviving (current) version -> quarantine with reasons
  5. add pipeline_run_id and staged_at_utc
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.audit import utc_now_iso
from src.config import SETTINGS, path_for
from src.errors import TransformError, get_logger

log = get_logger(__name__)

QUARANTINE_REASON = "quarantine_reason"


# ----------------------------------------------------------------- helpers
def _strip_all(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].map(lambda v: v.strip() if isinstance(v, str) else v)
    return df


def _blank_to_none(s: pd.Series) -> pd.Series:
    return s.map(lambda v: None if v is None or (isinstance(v, float) and np.isnan(v))
                 or (isinstance(v, str) and v == "") else v).astype(object)


def parse_utc(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, utc=True, errors="coerce", format="ISO8601")


def dedup_latest(df: pd.DataFrame, key: str, updated_col: str = "updated_at") -> tuple[pd.DataFrame, int]:
    """Keep the most recent version of each business key. Deterministic: ties on
    updated_at are broken by source row order (the later physical row wins)."""
    df = df.assign(_source_row=np.arange(len(df)))
    df = df.sort_values([key, updated_col, "_source_row"], kind="mergesort")
    deduped = df.drop_duplicates(subset=[key], keep="last")
    removed = len(df) - len(deduped)
    return deduped.sort_values("_source_row").drop(columns="_source_row").reset_index(drop=True), removed


def _reasons(checks: dict[str, pd.Series], index) -> pd.Series:
    """Combine boolean failure masks into 'reason_a;reason_b' strings ('' = valid)."""
    out = pd.Series([""] * len(index), index=index, dtype=object)
    for name, mask in checks.items():
        mask = mask.fillna(True).astype(bool)
        out[mask] = out[mask].map(lambda r, n=name: f"{r};{n}" if r else n)
    return out


def _split(df: pd.DataFrame, reasons: pd.Series) -> tuple[pd.DataFrame, pd.DataFrame]:
    bad = reasons != ""
    quarantine = df[bad].copy()
    quarantine[QUARANTINE_REASON] = reasons[bad]
    return df[~bad].copy(), quarantine


def _stamp(df: pd.DataFrame, run_id: str, staged_at: str) -> pd.DataFrame:
    df = df.copy()
    df["pipeline_run_id"] = run_id
    df["staged_at_utc"] = pd.Timestamp(staged_at)
    return df


# ----------------------------------------------------------------- customers
def clean_customers(raw: pd.DataFrame, run_id: str, staged_at: str | None = None):
    staged_at = staged_at or utc_now_iso()
    df = _strip_all(raw)
    df["created_at"] = parse_utc(df["created_at"])
    df["updated_at"] = parse_utc(df["updated_at"])

    pre = _reasons({"missing_customer_id": df["customer_id"].fillna("") == "",
                    "invalid_updated_at": df["updated_at"].isna()}, df.index)
    df, q_pre = _split(df, pre)
    df, dups = dedup_latest(df, "customer_id")

    email = df["email"].fillna("").str.lower()
    df["email"] = _blank_to_none(email)
    df["email_missing"] = df["email"].isna()           # visible quality flag, row is kept
    df["city"] = _blank_to_none(df["city"].fillna("").str.split().str.join(" ").str.title())
    df["customer_tier"] = _blank_to_none(df["customer_tier"].fillna("").str.title())
    for col in ("first_name", "last_name"):
        df[col] = _blank_to_none(df[col].fillna(""))

    return _stamp(df, run_id, staged_at), q_pre, {"duplicates_removed": dups,
                                                    "missing_email": int(df["email_missing"].sum())}


# ----------------------------------------------------------------- products
def clean_products(records: list[dict], run_id: str, staged_at: str | None = None):
    staged_at = staged_at or utc_now_iso()
    df = pd.json_normalize(records)
    df = df.rename(columns={"category.name": "category_name", "category.department": "category_department"})
    for col in ("category_name", "category_department"):
        if col not in df.columns:
            df[col] = None
    df = _strip_all(df)
    df["updated_at"] = parse_utc(df["updated_at"])

    pre = _reasons({"missing_product_id": df["product_id"].fillna("").astype(str) == "",
                    "invalid_updated_at": df["updated_at"].isna()}, df.index)
    df, q_pre = _split(df, pre)
    df, dups = dedup_latest(df, "product_id")

    price = pd.to_numeric(df["unit_price"], errors="coerce")
    reasons = _reasons({"invalid_unit_price": price.isna(),
                        "negative_unit_price": price < 0}, df.index)
    df["unit_price"] = price
    df, q_rules = _split(df, reasons)
    return _stamp(df, run_id, staged_at), pd.concat([q_pre, q_rules], ignore_index=True), {
        "duplicates_removed": dups}


# ----------------------------------------------------------------- orders
def clean_orders(raw: pd.DataFrame, run_id: str, staged_at: str | None = None):
    staged_at = staged_at or utc_now_iso()
    rules = SETTINGS.order_rules
    df = _strip_all(raw)
    df["order_timestamp"] = parse_utc(df["order_timestamp"])
    df["updated_at"] = parse_utc(df["updated_at"])

    pre = _reasons({"missing_order_id": df["order_id"].fillna("") == "",
                    "invalid_updated_at": df["updated_at"].isna()}, df.index)
    df, q_pre = _split(df, pre)
    df, dups = dedup_latest(df, "order_id")

    qty = pd.to_numeric(df["quantity"], errors="coerce")
    price = pd.to_numeric(df["unit_price"], errors="coerce")
    disc = pd.to_numeric(df["discount_pct"], errors="coerce")
    status = df["status"].fillna("").str.upper()

    reasons = _reasons({
        "missing_customer_id": df["customer_id"].fillna("") == "",
        "missing_product_id": df["product_id"].fillna("") == "",
        "invalid_order_timestamp": df["order_timestamp"].isna(),
        "invalid_quantity": qty.isna() | (qty != qty.round())
                            | (qty < rules["quantity_min"]) | (qty > rules["quantity_max"]),
        "invalid_unit_price": price.isna() | (price < 0),
        "invalid_discount_pct": disc.isna() | (disc < rules["discount_pct_min"])
                                | (disc > rules["discount_pct_max"]),
        "invalid_status": ~status.isin(rules["allowed_statuses"]),
    }, df.index)

    df["quantity"], df["unit_price"], df["discount_pct"], df["status"] = qty, price, disc, status
    df, q_rules = _split(df, reasons)
    df["quantity"] = df["quantity"].astype("int64")
    return _stamp(df, run_id, staged_at), pd.concat([q_pre, q_rules], ignore_index=True), {
        "duplicates_removed": dups}


# ----------------------------------------------------------------- I/O
def write_quarantine(df: pd.DataFrame, name: str, run_id: str, layer: str) -> Path:
    out = path_for("quarantine") / f"{name}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df = df.copy()
    df["quarantine_layer"] = layer
    df["pipeline_run_id"] = run_id
    df["quarantined_at_utc"] = utc_now_iso()
    df.to_csv(out, index=False)
    return out


def run_staging(raw_dir: Path, run_id: str) -> dict:
    files = SETTINGS.source_files
    staging_dir = path_for("staging")
    staging_dir.mkdir(parents=True, exist_ok=True)
    staged_at = utc_now_iso()
    summary: dict = {}
    try:
        customers_raw = pd.read_csv(raw_dir / files["customers"], dtype=str, keep_default_na=False)
        orders_raw = pd.read_csv(raw_dir / files["orders"], dtype=str, keep_default_na=False)
        with open(raw_dir / files["products"], encoding="utf-8") as fh:
            products_raw = json.load(fh)
    except (OSError, ValueError, pd.errors.ParserError) as err:
        raise TransformError(f"Cannot read raw snapshot in {raw_dir}: {err}", run_id=run_id) from err

    for name, raw_rows, (clean, quarantine, extra) in (
        ("customers", len(customers_raw), clean_customers(customers_raw, run_id, staged_at)),
        ("products", len(products_raw), clean_products(products_raw, run_id, staged_at)),
        ("orders", len(orders_raw), clean_orders(orders_raw, run_id, staged_at)),
    ):
        try:
            clean.to_parquet(staging_dir / f"{name}.parquet", index=False)
        except (OSError, ImportError, ValueError) as err:
            raise TransformError(f"Cannot write staging/{name}.parquet: {err}", run_id=run_id) from err
        write_quarantine(quarantine, f"staging_{name}", run_id, "staging")
        summary[name] = {
            "raw_rows": raw_rows,
            "staged_rows": len(clean),
            "quarantined_rows": len(quarantine),
            "quarantine_reasons": quarantine[QUARANTINE_REASON].value_counts().to_dict()
            if len(quarantine) else {},
            **extra,
        }
    log.info("Staging complete: %s", {k: (v["staged_rows"], v["quarantined_rows"]) for k, v in summary.items()})
    return summary
