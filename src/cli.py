"""
Thin command-line entry point. Each command parses arguments, calls exactly one
module function, and prints a JSON summary. No transformation logic lives here.

    python -m src.cli validate-env
    python -m src.cli extract          [--run-id ID]
    python -m src.cli transform        [--run-id ID]   # staging + curated from raw
    python -m src.cli run-all          [--run-id ID]   # extract + transform
    python -m src.cli load
    python -m src.cli validate         [--skip-db] [--year Y --month M]
    python -m src.cli benchmark        [--repeats 5] [--skip-postgres]
    python -m src.cli partition                        # partitioned Parquet only
    python -m src.cli load-partition   --year Y --month M [--run-id ID]

Run identity: --run-id, else $PIPELINE_RUN_ID (Airflow sets it to its run_id),
else a new run_<UTC timestamp>_<random> id.

Error handling: every stage runs inside run_stage(). A StageError (or any
unexpected exception, wrapped as one) is logged with the stage name, run id,
duration and original cause, then the process exits with status 1 so callers
such as Airflow see the failure and can retry.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback

from src.audit import new_run_id, utc_now_iso
from src.errors import StageError, get_logger

log = get_logger("src.cli")


class UnexpectedStageError(StageError):
    def __init__(self, stage: str, err: BaseException, run_id: str | None):
        self.stage = stage
        super().__init__(f"unexpected {type(err).__name__}: {err}", run_id=run_id)


def _print(obj) -> None:
    print(json.dumps(obj, indent=2, default=str))


def run_stage(stage: str, fn, run_id: str | None = None):
    run_id = run_id or os.getenv("PIPELINE_RUN_ID") or None
    start = time.perf_counter()
    log.info("START stage=%s run_id=%s", stage, run_id)
    try:
        result = fn()
    except StageError:
        log.error("FAILED stage=%s run_id=%s after %.2fs", stage, run_id, time.perf_counter() - start)
        raise
    except Exception as err:  # not swallowed: logged with traceback and re-raised as a StageError
        log.error("FAILED stage=%s run_id=%s after %.2fs\n%s", stage, run_id,
                  time.perf_counter() - start, traceback.format_exc())
        raise UnexpectedStageError(stage, err, run_id) from err
    log.info("END stage=%s run_id=%s in %.2fs", stage, run_id, time.perf_counter() - start)
    return result


def _run_id(args) -> str:
    return getattr(args, "run_id", None) or new_run_id()


# ------------------------------------------------------------------ commands
def cmd_validate_env(args):
    from src.validate.environment import validate_env

    _print(run_stage("validate-env", lambda: validate_env(require_db=args.require_db)))


def cmd_extract(args):
    from src.extract.files import extract_sources

    run_id = _run_id(args)
    raw = run_stage("extract", lambda: extract_sources(run_id), run_id)
    _print({"pipeline_run_id": run_id, "raw_dir": str(raw)})


def _transform(run_id: str, started_at: str) -> dict:
    from src.extract.files import extract_sources, raw_dir_for
    from src.transform.curated import run_curated, write_run_summary
    from src.transform.staging import run_staging

    raw = raw_dir_for(run_id)
    if not raw.exists():  # transform run on its own: take the snapshot first
        raw = run_stage("extract", lambda: extract_sources(run_id), run_id)
    staging = run_stage("transform.staging", lambda: run_staging(raw, run_id), run_id)
    curated = run_stage("transform.curated", lambda: run_curated(run_id), run_id)
    return write_run_summary(run_id, started_at, raw, staging, curated)


def cmd_transform(args):
    run_id = _run_id(args)
    _print(_transform(run_id, utc_now_iso()))


def cmd_run_all(args):
    from src.extract.files import extract_sources

    run_id, started = _run_id(args), utc_now_iso()
    run_stage("extract", lambda: extract_sources(run_id), run_id)
    _print(_transform(run_id, started))


def cmd_load(args):
    from src.load.postgres import load_curated

    _print(run_stage("load", load_curated))


def cmd_validate(args):
    from src.validate.checks import validate_outputs

    if (args.year is None) != (args.month is None):
        raise StageError("--year and --month must be given together")
    _print(run_stage("validate", lambda: validate_outputs(skip_db=args.skip_db,
                                                           year=args.year, month=args.month)))


def cmd_partition(args):
    from src.benchmark.partitioning import partition_tree, write_partitioned_parquet

    result = run_stage("partition", write_partitioned_parquet)
    result["tree"] = partition_tree()
    _print(result)


def cmd_benchmark(args):
    from src.benchmark.partitioning import (partition_tree, read_partition, verify_partition,
                                            write_partitioned_parquet)
    from src.benchmark.storage import run_benchmark

    bench = run_stage("benchmark", lambda: run_benchmark(repeats=args.repeats,
                                                        include_postgres=not args.skip_postgres))
    parts = run_stage("partition", write_partitioned_parquet)
    y, m = args.check_year, args.check_month
    check = verify_partition(read_partition(y, m), y, m)
    _print({"benchmark": bench, "partitioning": {k: v for k, v in parts.items() if k != "partitions"},
            "partition_tree": partition_tree(), "selected_partition_check": check})


def cmd_load_partition(args):
    from src.load.postgres import load_partition

    run_id = _run_id(args)
    _print(run_stage("load-partition", lambda: load_partition(args.year, args.month, run_id), run_id))


# ------------------------------------------------------------------ parser
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m src.cli", description="DSS150P sales pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("validate-env", help="Check packages, config, source files and PostgreSQL")
    p.add_argument("--require-db", action="store_true", help="Fail (not warn) if PostgreSQL is unreachable")
    p.set_defaults(func=cmd_validate_env)

    for name, func, help_ in (
        ("extract", cmd_extract, "Snapshot source files into data/raw/run_id=<id>/"),
        ("transform", cmd_transform, "Raw -> staging -> curated (+ quarantine)"),
        ("run-all", cmd_run_all, "extract + transform"),
    ):
        p = sub.add_parser(name, help=help_)
        p.add_argument("--run-id", default=None)
        p.set_defaults(func=func)

    p = sub.add_parser("load", help="Rerun-safe UPSERT of curated data into PostgreSQL")
    p.set_defaults(func=cmd_load)

    p = sub.add_parser("validate", help="Contract checks on staging/curated/PostgreSQL")
    p.add_argument("--skip-db", action="store_true", help="Only check files, not PostgreSQL")
    p.add_argument("--year", type=int)
    p.add_argument("--month", type=int)
    p.set_defaults(func=cmd_validate)

    p = sub.add_parser("benchmark", help="CSV/JSONL/Parquet/PostgreSQL benchmark + partitioned Parquet")
    p.add_argument("--repeats", type=int, default=None, help="default from config/settings.yml (5)")
    p.add_argument("--skip-postgres", action="store_true")
    p.add_argument("--check-year", type=int, default=2026, help="partition to read back and verify")
    p.add_argument("--check-month", type=int, default=1)
    p.set_defaults(func=cmd_benchmark)

    p = sub.add_parser("partition", help="Write data/partitioned/order_year=/order_month=")
    p.set_defaults(func=cmd_partition)

    p = sub.add_parser("load-partition", help="Rerun-safe load of one year/month partition + audit row")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True, choices=range(1, 13), metavar="1-12")
    p.add_argument("--run-id", default=None)
    p.set_defaults(func=cmd_load_partition)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
        return 0
    except StageError as err:
        log.error("PIPELINE FAILURE at %s: %s", utc_now_iso(), err)
        if err.__cause__ is not None:
            log.error("caused by %s: %s", type(err.__cause__).__name__, err.__cause__)
        return 1


if __name__ == "__main__":
    sys.exit(main())
