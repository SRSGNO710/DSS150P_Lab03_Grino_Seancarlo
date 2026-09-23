import pandas as pd

from src.benchmark.partitioning import read_partition, verify_partition, write_partitioned_parquet


def _frame():
    ts = pd.to_datetime(["2025-12-31T23:59:00Z", "2026-01-01T00:00:00Z", "2026-01-31T23:59:00Z",
                         "2026-02-01T00:00:00Z"], utc=True)
    return pd.DataFrame({"order_id": ["A", "B", "C", "D"], "order_timestamp": ts, "net_amount": [1.0, 2, 3, 4]})


def test_partitioned_layout_and_single_partition_read(tmp_path):
    out = tmp_path / "partitioned"
    result = write_partitioned_parquet(_frame(), out)
    assert set(result["partitions"]) == {"order_year=2025/order_month=12", "order_year=2026/order_month=1",
                                         "order_year=2026/order_month=2"}
    assert (out / "order_year=2026" / "order_month=1").is_dir()

    jan = read_partition(2026, 1, out)
    assert sorted(jan["order_id"]) == ["B", "C"]          # UTC month boundaries respected
    assert verify_partition(jan, 2026, 1)["ok"]


def test_rewriting_partitions_does_not_duplicate_rows(tmp_path):
    out = tmp_path / "partitioned"
    write_partitioned_parquet(_frame(), out)
    write_partitioned_parquet(_frame(), out)               # rerun
    assert len(read_partition(2026, 1, out)) == 2
    assert len(list((out / "order_year=2026" / "order_month=1").glob("*.parquet"))) == 1
