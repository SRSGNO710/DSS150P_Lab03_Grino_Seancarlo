"""Unit tests for extraction, staging, curated, and validation rules."""
import json  

import pandas as pd
import pytest

from src.transform.curated import build_curated_frame
from src.transform.staging import clean_customers, clean_orders, clean_products
from src.validate import checks

RUN = "run_test"


def customers_frame(rows):
    cols = ["customer_id", "first_name", "last_name", "email", "city", "customer_tier", "created_at", "updated_at"]
    return pd.DataFrame(rows, columns=cols).astype(str)


def orders_frame(rows):
    cols = ["order_id", "customer_id", "product_id", "order_timestamp", "quantity",
            "unit_price", "discount_pct", "status", "updated_at"]
    return pd.DataFrame(rows, columns=cols).astype(str)


# ---------------------------------------------------------------- raw layer
def test_extract_creates_run_specific_identical_snapshot(isolated_data):
    from src.config import SETTINGS
    from src.extract.files import extract_sources, sha256_of

    for name, body in {"customers.csv": "customer_id\nC1\n", "orders.csv": "order_id\nO1\n",
                       "products.json": json.dumps([{"product_id": "P1"}])}.items():
        (SETTINGS.source_dir / name).write_text(body)
    before = {p.name: sha256_of(p) for p in SETTINGS.source_dir.iterdir()}

    raw = extract_sources("run_abc")

    assert raw.name == "run_id=run_abc"
    for name, digest in before.items():
        assert sha256_of(raw / name) == digest                     # byte-identical copy
        assert sha256_of(SETTINGS.source_dir / name) == digest     # source untouched
    manifest = json.loads((raw / "_manifest.json").read_text())
    assert manifest["files"]["orders"]["physical_records"] == 1


def test_extract_missing_source_is_a_stage_error(isolated_data):
    from src.errors import ExtractError
    from src.extract.files import extract_sources

    with pytest.raises(ExtractError, match="stage=extract"):
        extract_sources("run_missing")


# ---------------------------------------------------------------- staging
def test_customers_keep_latest_version_and_normalize():
    raw = customers_frame([
        ["C1", "Ana", "Diaz", "ana@x.com", "Pasig", "Gold", "2023-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00"],
        ["C1", "Ana", "Diaz", " ANA@X.COM ", "  quezon   city ", "gold", "2023-01-01T00:00:00+00:00",
         "2025-02-01T00:00:00+00:00"],
        ["C2", "Bo", "Cruz", "", "Manila", "Silver", "2023-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00"],
    ])
    clean, quarantine, stats = clean_customers(raw, RUN)
    c1 = clean.set_index("customer_id").loc["C1"]
    assert len(clean) == 2 and stats["duplicates_removed"] == 1
    assert c1["email"] == "ana@x.com"          # latest version, lowercased and trimmed
    assert c1["city"] == "Quezon City"         # trimmed, whitespace collapsed, title-case
    assert str(c1["updated_at"]) == "2025-02-01 00:00:00+00:00"
    c2 = clean.set_index("customer_id").loc["C2"]
    assert bool(c2["email_missing"]) and pd.isna(c2["email"])   # kept, but visibly flagged
    assert (clean["pipeline_run_id"] == RUN).all() and clean["staged_at_utc"].notna().all()
    assert quarantine.empty


def test_products_flatten_category_and_quarantine_bad_prices():
    records = [
        {"product_id": "P1", "name": "A", "category": {"name": "Tablet", "department": "Computing"},
         "brand": "X", "unit_price": 10.5, "active": True, "updated_at": "2025-01-01T00:00:00+00:00"},
        {"product_id": "P2", "name": "B", "category": {"name": "Audio", "department": "Electronics"},
         "brand": "Y", "unit_price": -1, "active": True, "updated_at": "2025-01-01T00:00:00+00:00"},
        {"product_id": "P3", "name": "C", "category": {"name": "Audio", "department": "Electronics"},
         "brand": "Y", "unit_price": "abc", "active": True, "updated_at": "2025-01-01T00:00:00+00:00"},
    ]
    clean, quarantine, _ = clean_products(records, RUN)
    assert list(clean["product_id"]) == ["P1"]
    assert {"category_name", "category_department"} <= set(clean.columns)
    assert clean.iloc[0]["category_name"] == "Tablet"
    reasons = dict(zip(quarantine["product_id"], quarantine["quarantine_reason"]))
    assert reasons == {"P2": "negative_unit_price", "P3": "invalid_unit_price"}


def test_orders_dedup_before_rules_and_quarantine_with_reasons():
    ts = "2025-01-01T00:00:00+00:00"
    raw = orders_frame([
        ["O1", "C1", "P1", ts, "2", "100", "0.1", "PENDING", "2025-01-01T00:00:00+00:00"],
        ["O1", "C1", "P1", ts, "2", "100", "0.1", "DELIVERED", "2025-01-03T00:00:00+00:00"],  # newer wins
        ["O2", "C1", "P1", ts, "0", "100", "0", "PAID", ts],        # quantity below 1
        ["O3", "C1", "P1", ts, "21", "100", "0", "PAID", ts],       # quantity above 20
        ["O4", "C1", "P1", ts, "1", "100", "0", "UNKNOWN", ts],     # status not allowed
        ["O5", "C1", "P1", "not-a-date", "1", "100", "0", "PAID", ts],
    ])
    clean, quarantine, stats = clean_orders(raw, RUN)
    assert list(clean["order_id"]) == ["O1"] and clean.iloc[0]["status"] == "DELIVERED"
    assert stats["duplicates_removed"] == 1
    assert clean["quantity"].dtype == "int64"
    reasons = dict(zip(quarantine["order_id"], quarantine["quarantine_reason"]))
    assert reasons == {"O2": "invalid_quantity", "O3": "invalid_quantity",
                       "O4": "invalid_status", "O5": "invalid_order_timestamp"}


# ---------------------------------------------------------------- curated
def _staged():
    ts = "2025-01-01T00:00:00+00:00"
    orders, _, _ = clean_orders(orders_frame([
        ["O1", "C1", "P1", ts, "3", "100.00", "0.10", "DELIVERED", "2025-01-02T00:00:00+00:00"],
        ["O2", "C9", "P1", ts, "1", "100.00", "0", "PAID", ts],   # customer does not exist
        ["O3", "C1", "PX", ts, "1", "100.00", "0", "PAID", ts],   # product rejected in staging
    ]), RUN)
    customers, _, _ = clean_customers(customers_frame([
        ["C1", "Ana", "Diaz", "a@x.com", "Pasig", "Gold", ts, "2025-03-01T00:00:00+00:00"]]), RUN)
    products, _, _ = clean_products([{"product_id": "P1", "name": "Tab", "brand": "X", "unit_price": 99,
                                      "category": {"name": "Tablet", "department": "Computing"},
                                      "updated_at": ts}], RUN)
    return orders, customers, products


def test_curated_amounts_audit_columns_and_orphans():
    orders, customers, products = _staged()
    curated, quarantine = build_curated_frame(orders, customers, products, RUN,
                                              quarantined_product_ids={"PX"})
    row = curated.iloc[0]
    assert (row["gross_amount"], row["discount_amount"], row["net_amount"]) == (300.0, 30.0, 270.0)
    assert str(row["source_updated_at"]) == "2025-03-01 00:00:00+00:00"   # newest of order/customer/product
    assert row["pipeline_run_id"] == RUN and len(row["record_hash"]) == 64
    reasons = dict(zip(quarantine["order_id"], quarantine["quarantine_reason"]))
    assert reasons == {"O2": "orphan_customer_id", "O3": "product_quarantined_in_staging"}


def test_record_hash_ignores_run_metadata_but_tracks_business_changes():
    orders, customers, products = _staged()
    a, _ = build_curated_frame(orders, customers, products, "run_1", processed_at="2026-01-01T00:00:00+00:00")
    b, _ = build_curated_frame(orders, customers, products, "run_2", processed_at="2026-06-01T00:00:00+00:00")
    assert a["record_hash"].tolist() == b["record_hash"].tolist()

    changed = orders.copy()
    changed.loc[changed["order_id"] == "O1", "status"] = "CANCELLED"
    c, _ = build_curated_frame(changed, customers, products, "run_3")
    assert c.loc[0, "record_hash"] != a.loc[0, "record_hash"]


# ---------------------------------------------------------------- validation
def test_validation_detects_bad_keys_amounts_and_statuses():
    orders, customers, products = _staged()
    good, _ = build_curated_frame(orders, customers, products, RUN)
    assert checks.check_business_key(good, "order_id", "t") == []
    assert checks.check_amounts(good) == [] and checks.check_audit_columns(good) == []

    dup = pd.concat([good, good])
    assert any("duplicate" in f for f in checks.check_business_key(dup, "order_id", "t"))
    nulls = good.assign(order_id=[None])
    assert any("null" in f for f in checks.check_business_key(nulls, "order_id", "t"))
    neg = good.assign(net_amount=[-5.0])
    assert any("negative" in f for f in checks.check_amounts(neg))
    wrong = good.assign(gross_amount=[1.0])
    assert any("gross_amount" in f for f in checks.check_amounts(wrong))
    assert checks.check_statuses(good.assign(status=["LOST"]), ["PAID"], "t")
