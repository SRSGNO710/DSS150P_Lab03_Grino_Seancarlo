"""
DSS150P sales pipeline DAG — orchestration only.

Every task is a one-line call into the project CLI (python -m src.cli ...);
all extraction/transformation/loading/validation rules live in src/. This file
only declares ORDER, SCHEDULE, PARAMETERS, RETRIES, TIMEOUTS and FAILURE HANDLING.

Design decisions
----------------
schedule "0 2 * * *" (Asia/Manila)
    The sources are daily snapshots. 02:00 local time is after the business day
    has closed and before analysts start work, and is a quiet time for the
    database. Running more often would reprocess the same snapshot; less often
    would leave the served table up to a week stale.

catchup=False
    Each run re-reads the FULL current snapshot and the load is an idempotent
    UPSERT, so running every missed interval since start_date would repeat
    identical work (dozens of runs on first unpause) without adding data.
    Historical months are handled deliberately with run_mode=partition
    (see docs/ANSWERS.md, backfill section).

max_active_runs=1
    All runs share data/staging and data/curated; two concurrent runs could
    interleave writes. Serialising runs removes that race.

retries=2, retry_delay=1 min (exponential, max 5 min), execution_timeout per task
    Transient faults (DB restarting, file briefly locked) get two more attempts.
    A timeout guarantees a hung task fails (and retries) instead of blocking the
    DAG forever. Retries are safe because every task is idempotent: extract
    overwrites the same raw folder, transform overwrites staging/curated, load is
    UPSERT guarded by record_hash, validate is read-only.

Run identity
    Every task exports PIPELINE_RUN_ID="{{ run_id }}", so all four tasks (and
    their retries) stamp the same pipeline_run_id and the same raw folder.

Parameters
    run_mode = full       -> load the whole curated table, validate everything
    run_mode = partition  -> materialize partitions, load only order_year/order_month
                             (params.year / params.month), validate that partition
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = os.environ.get("DSS150P_PROJECT_DIR", "/opt/airflow/project")
FAILURE_LOG = os.path.join(PROJECT, "logs", "airflow_failures.jsonl")

# Shared prefix: run from the project root with ONE run id for the whole DAG run.
ENV = f'cd {PROJECT} && export PIPELINE_RUN_ID="{{{{ run_id }}}}" && '
CLI = "python -m src.cli"


def _task_context(context: dict, event: str) -> dict:
    ti = context["task_instance"]
    exc = context.get("exception")
    return {
        "event": event,
        "at_utc": datetime.now(timezone.utc).isoformat(),
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": context.get("run_id"),
        "try_number": ti.try_number,
        "max_tries": ti.max_tries + 1,
        "logical_date": str(context.get("logical_date")),
        "params": dict(context.get("params") or {}),
        "error_type": type(exc).__name__ if exc else None,
        "error": str(exc)[:2000] if exc else None,
        "log_url": getattr(ti, "log_url", None),
    }


def failure_callback(context):
    """Runs once a task has FAILED for good (all retries used): print a concise
    record into the task log and append it to logs/airflow_failures.jsonl."""
    record = _task_context(context, "task_failed")
    print("DSS150P TASK FAILED " + json.dumps(record, default=str))
    try:
        os.makedirs(os.path.dirname(FAILURE_LOG), exist_ok=True)
        with open(FAILURE_LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, default=str) + "\n")
    except OSError as err:  # callback must never mask the original failure
        print(f"DSS150P could not write {FAILURE_LOG}: {err}")


def retry_callback(context):
    """Runs before each retry, so retry attempts are visible in the logs too."""
    record = _task_context(context, "task_up_for_retry")
    print("DSS150P TASK WILL RETRY " + json.dumps(record, default=str))


DEFAULT_ARGS = {
    "owner": "dss150p",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": failure_callback,
    "on_retry_callback": retry_callback,
}

with DAG(
    dag_id="dss150p_sales_pipeline",
    description="raw -> staging -> curated -> PostgreSQL, full or single-partition",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Manila"),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    params={
        "run_mode": Param("full", enum=["full", "partition"],
                          description="full = whole table; partition = one order_year/order_month"),
        "year": Param(2026, type="integer", minimum=2000, maximum=2100,
                      description="order_year for run_mode=partition"),
        "month": Param(1, type="integer", minimum=1, maximum=12,
                       description="order_month for run_mode=partition"),
    },
    render_template_as_native_obj=False,
    tags=["DSS150P"],
    doc_md=__doc__,
) as dag:
    extract = BashOperator(
        task_id="extract",
        bash_command=f"{ENV}{CLI} extract",
        execution_timeout=timedelta(minutes=5),
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=f"{ENV}{CLI} transform",
        execution_timeout=timedelta(minutes=15),
    )

    load = BashOperator(
        task_id="load",
        bash_command=(
            f"{ENV}"
            "{% if params.run_mode == 'partition' %}"
            f"{CLI} partition && {CLI} load-partition "
            "--year {{ params.year }} --month {{ params.month }} --run-id \"$PIPELINE_RUN_ID\""
            "{% else %}"
            f"{CLI} load"
            "{% endif %}"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    validate = BashOperator(
        task_id="validate",
        bash_command=(
            f"{ENV}{CLI} validate"
            "{% if params.run_mode == 'partition' %} --year {{ params.year }} --month {{ params.month }}{% endif %}"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    extract >> transform >> load >> validate
