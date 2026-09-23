"""DAG checks. Skipped where Airflow is not installed; run inside the Airflow image: 
docker compose -f docker-compose.yml -f docker-compose.airflow.yml run --rm airflow-scheduler \
    bash -c "pip install -q pytest && cd /opt/airflow/project && python -m pytest -q tests/test_dag.py"
"""
from datetime import timedelta
from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")


@pytest.fixture(scope="module")
def dag():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(Path(__file__).resolve().parents[1] / "dags"), include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("dss150p_sales_pipeline")


def test_operational_configuration(dag):
    assert dag.schedule_interval == "0 2 * * *"
    assert dag.catchup is False and dag.max_active_runs == 1
    assert set(dag.params.keys()) == {"run_mode", "year", "month"}
    for task in dag.tasks:
        assert task.retries >= 2 and task.retry_delay == timedelta(minutes=1)
        assert task.execution_timeout is not None
        assert task.on_failure_callback is not None


def test_dependencies_and_delegation(dag):
    assert [t.task_id for t in dag.topological_sort()] == ["extract", "transform", "load", "validate"]
    for task in dag.tasks:
        assert "python -m src.cli" in task.bash_command
        assert 'PIPELINE_RUN_ID="{{ run_id }}"' in task.bash_command
