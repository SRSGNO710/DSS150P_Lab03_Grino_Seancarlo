"""
Data-contract assertions over staging, curated and PostgreSQL. Read-only: this
module never writes business data. Each check_* function is pure and returns
a list of human-readable failures (empty list = pass), so tests can feed it
deliberately broken frames.
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from src.config import SETTINGS, path_for
from src.db import connect
from src.errors import ValidationError, get_logger

log = get_logger(__name__)

HASH_RE = re.compile(r"^[0-9a-f]{64}$")


def check_business_key(df: pd.DataFrame, key: str, label: str) -> list[str]:
    failures = []
    nulls = int(df[key].isna().sum() + (df[key].astype(str).str.strip() == "").sum())
    dups = int(df[key].dropna().duplicated().sum())
    if nulls:
        failures.append(f"{label}: {nulls} null/blank {key}")
    if dups:
        failures.append(f"{label}: {dups} duplicate {key}")
    return failures


def check_statuses(df: pd.DataFrame, allowed, label: str) -> list[str]:
    bad = ~df["status"].isin(list(allowed))
    return [f"{label}: {int(bad.sum())} row(s) with status outside {sorted(allowed)}"] if bad.any() else []


def check_amounts(df: pd.DataFrame, label: str = "curated") -> list[str]:
    failures = []
    money = ["gross_amount", "discount_amount", "net_amount"]
    if df[money].isna().any().any():
        failures.append(f"{label}: null monetary amounts")
    negative = (df[money] < 0).any(axis=1)
    if negative.any():
        failures.append(f"{label}: {int(negative.sum())} row(s) with negative amounts")
    q = df["quantity"]
    if ((q < SETTINGS.order_rules["quantity_min"]) | (q > SETTINGS.order_rules["quantity_max"])).any():
        failures.append(f"{label}: quantity outside allowed range")
    gross_off = (df["gross_amount"] - (df["quantity"] * df["unit_price"])).abs() > 0.01
    if gross_off.any():
        failures.append(f"{label}: {int(gross_off.sum())} row(s) where gross_amount != quantity*unit_price")
    net_off = (df["net_amount"] - (df["gross_amount"] - df["discount_amount"])).abs() > 0.01
    if net_off.any():
        failures.append(f"{label}: {int(net_off.sum())} row(s) where net_amount != gross-discount")
    return failures


def check_audit_columns(df: pd.DataFrame, label: str = "curated") -> list[str]:
    failures = []
    for col in ("source_updated_at", "pipeline_run_id", "processed_at_utc", "record_hash"):
        if col not in df.columns or df[col].isna().any():
            failures.append(f"{label}: audit column {col} missing or null")
    if "record_hash" in df.columns and not df["record_hash"].astype(str).map(HASH_RE.match).notna().all():
        failures.append(f"{label}: record_hash is not a 64-char SHA-256 hex digest")
    return failures


def check_source_unchanged(source_dir: Path) -> list[str]:
    """Compare data/source files with the committed data/source/SHA256SUMS."""
    from src.extract.files import sha256_of

    sums = source_dir / "SHA256SUMS"
    if not sums.exists():
        return []
    failures = []
    for line in sums.read_text().splitlines():
        if not line.strip():
            continue
        expected, name = line.split()
        path = source_dir / name
        if not path.exists():
            failures.append(f"source: {name} is missing")
        elif sha256_of(path) != expected:
            failures.append(f"source: {name} was modified (SHA-256 mismatch)")
    return failures


def check_database(curated: pd.DataFrame) -> tuple[list[str], dict]:
    conn = connect(ValidationError)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*), COUNT(DISTINCT order_id) FROM curated.sales_order_lines")
            total, distinct = cur.fetchone()
            cur.execute("SELECT order_id, record_hash FROM curated.sales_order_lines")
            db = pd.DataFrame(cur.fetchall(), columns=["order_id", "record_hash"])
    finally:
        conn.close()
    failures = []
    if total != distinct:
        failures.append(f"postgres: {total - distinct} duplicate order_id rows")
    merged = curated[["order_id", "record_hash"]].merge(db, on="order_id", how="left", suffixes=("", "_db"))
    missing = int(merged["record_hash_db"].isna().sum())
    stale = int(((merged["record_hash"] != merged["record_hash_db"]) & merged["record_hash_db"].notna()).sum())
    if missing:
        failures.append(f"postgres: {missing} expected row(s) not loaded")
    if stale:
        failures.append(f"postgres: {stale} row(s) whose record_hash differs from curated")
    return failures, {"db_total_rows": int(total), "db_distinct_order_id": int(distinct),
                      "expected_rows_checked": len(curated), "missing": missing, "stale": stale}


def validate_outputs(skip_db: bool = False, year: int | None = None, month: int | None = None) -> dict:
    staging_dir = path_for("staging")
    try:
        orders = pd.read_parquet(staging_dir / "orders.parquet")
        customers = pd.read_parquet(staging_dir / "customers.parquet")
        products = pd.read_parquet(staging_dir / "products.parquet")
        curated = pd.read_parquet(SETTINGS.curated_file)
    except (OSError, ImportError, ValueError) as err:
        raise ValidationError(f"Pipeline outputs missing/unreadable ({err}). Run run-all first.") from err

    allowed = SETTINGS.order_rules["allowed_statuses"]
    failures: list[str] = []
    failures += check_source_unchanged(path_for("source"))
    failures += check_business_key(customers, "customer_id", "staging.customers")
    failures += check_business_key(products, "product_id", "staging.products")
    failures += check_business_key(orders, "order_id", "staging.orders")
    failures += check_statuses(orders, allowed, "staging.orders")
    failures += check_business_key(curated, "order_id", "curated")
    for col in ("customer_id", "product_id"):
        nulls = int(curated[col].isna().sum())
        if nulls:
            failures.append(f"curated: {nulls} null {col}")
    failures += check_statuses(curated, allowed, "curated")
    failures += check_amounts(curated)
    failures += check_audit_columns(curated)

    q_path = path_for("quarantine") / "curated_orders.csv"
    quarantined = len(pd.read_csv(q_path)) if q_path.exists() and q_path.stat().st_size else 0
    if len(orders) != len(curated) + quarantined:
        failures.append(f"reconciliation: staged orders {len(orders)} != curated {len(curated)} "
                        f"+ curated quarantine {quarantined}")

    report = {"staging_rows": {"customers": len(customers), "products": len(products), "orders": len(orders)},
              "curated_rows": len(curated), "curated_quarantine_rows": quarantined}

    if not skip_db:
        scope = curated
        if year is not None and month is not None:
            ts = pd.to_datetime(curated["order_timestamp"], utc=True)
            scope = curated[(ts.dt.year == year) & (ts.dt.month == month)]
            report["db_scope"] = f"order_year={year}/order_month={month}"
        db_failures, db_report = check_database(scope)
        failures += db_failures
        report.update(db_report)

    report["failures"] = failures
    report["status"] = "FAILED" if failures else "PASSED"
    if failures:
        raise ValidationError(f"{len(failures)} contract violation(s): {failures}")
    log.info("Validation passed: %s", report)
    return report
