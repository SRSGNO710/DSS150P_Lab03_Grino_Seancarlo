# Section 17: Final Submission Checklist Audit 

Status key: **✅ Done and verified**, **🟡 Done, re-run on your machine** (verified in the sandbox; the lab expects your own output), **🔴 Your action needed** (cannot be produced without Docker Desktop or the Airflow UI).

## Checklist A

| Item | Status | How it was verified | What you still do |
|---|---|---|---|
| No .env/secrets committed | ✅ | `.env` is in `.gitignore` and `.dockerignore`, and `git log --all --name-only` shows it was never tracked. Scanning every commit on every branch (`git log --all -p`) finds 0 occurrences of the local password. The Task C `git grep` for the placeholder string (excluding `.env.example`) prints nothing: verified on a fresh clone. Python, YAML, SQL and DAG files read credentials only through `src/config.py` → `os.environ`. Compose files use `${POSTGRES_PASSWORD}` interpolation. | Before pushing, run `git status --short` and make sure `.env` is not listed. |
| Source files unchanged | ✅ | `data/source/SHA256SUMS` is committed. `cmp` against the originally uploaded files: all 3 identical. `validate` rechecks the checksums on every run, and the failure experiment only *renamed* `orders.csv` (checksums OK afterwards). | Run `cd data/source && sha256sum -c SHA256SUMS` (Windows: `certutil -hashfile <file> SHA256`). |
| Rerun-safe PostgreSQL load verified | ✅ / 🟡 | Load 1: 49,897 inserted. Load 2: 0 inserted, 0 updated, 49,897 unchanged. A new pipeline run plus load: 0 touched. `COUNT(*) = COUNT(DISTINCT order_id) = 49,897`. Also verified after both failure experiments. | Repeat `run-all; load; load` against the Docker PostgreSQL and capture the `docker exec … COUNT(DISTINCT order_id)` output. |
| Partitioned Parquet and selected-partition load verified | ✅ / 🟡 | 21 partitions `order_year=YYYY/order_month=M`. Reading only `2026/1` gives 2,506 rows, 0 outside the partition. `load-partition --year 2026 --month 1` twice: 2,506 inserted then 2,506 unchanged. `audit.partition_loads` has one row, updated in place. `validate --year 2026 --month 1` PASSED. Two partitioning unit tests. | Capture the `data/partitioned` tree and `SELECT * FROM audit.partition_loads;` on your machine. |
| Git history includes Goal 1–4 checkpoints | ✅ | `main` holds the starter import plus 5 `--no-ff` merges of `goal1-reproducible-environment` (`feat: add reproducible pipeline environment`, the exact commit from Task E), `goal2-etl-pipeline`, `goal3-storage-benchmark`, `goal4-airflow-orchestration` and `docs-submission`. | Push all branches: `git remote add origin <url>`, then `git push -u origin --all`. If needed, set your own author name with `git config user.name`. |

## Checklist B

| Item | Status | How it was verified | What you still do |
|---|---|---|---|
| All required commands documented in README | ✅ | README covers setup, Docker, Goals 1–4, the full Airflow runbook (full, partition, failure, recovery), the Section 11 integrated sequence, a CLI reference and troubleshooting. Every command in the lab handout appears. | None. |
| Staging/curated/quarantine outputs reproducible | ✅ | Two `run-all` runs with different run IDs produced content-identical staging (3), curated (1) and quarantine (4) outputs once the run-metadata columns were removed. `record_hash` is stable across runs. | None. |
| Benchmark results and interpretation included | ✅ / 🟡 | `docs/evidence/benchmark_results_sandbox.csv` (+ per-run timings, hardware context, EXPLAIN plans). `docs/BENCHMARK_INTERPRETATION.md`. All 5 Goal 3 questions answered in `docs/ANSWERS.md` Part B. | Optional but recommended: run `python -m src.cli benchmark --repeats 5` on your machine, since the lab asks about results "on your machine". File sizes and the ranking should match (a fresh clone reproduced every file size byte-for-byte; PostgreSQL's size can differ by a few 8 KB pages because it depends on insert history). Update the seconds if they differ. |
| Airflow full/partition/failure/recovery evidence included | 🔴 | The DAG is complete and was exercised end to end with an Airflow stand-in (`docs/evidence/goal4_dag_preflight_sandbox.txt`): full run, partition run, 3 attempts + retry/failure callbacks, recovery with no duplicates. **Real Airflow screenshots cannot be generated in the sandbox** (no Docker Hub or PyPI access). | Follow README → "Goal 4: Airflow runbook" steps 1–4 and save screenshots of Graph, Grid, task logs, the partition run, the failed extract with retries, `logs/airflow_failures.jsonl`, and the recovery. |
| Repository runs without relying on undocumented manual edits | ✅ / 🟡 | From a clean state (databases dropped, data folders emptied), the Section 11 sequence ran with every step exiting 0 (`docs/evidence/integrated_run_sandbox.txt`). The only manual step is `cp .env.example .env` and setting a password, which is documented. Compose files pass `docker compose config`. | Run the Section 11 sequence once with Docker. |

## Technical questions: all answered

- Section 15, Q1–Q8: `docs/ANSWERS.md` Part A
- Section 9.5 Goal 3 analysis, Q1–Q5: `docs/ANSWERS.md` Part B and `docs/BENCHMARK_INTERPRETATION.md`
- Explanations asked inside tasks (config separation, schedule cadence, catchup, safe reruns, optional backfill): `docs/ANSWERS.md` Part C
- Technical reflection (modularity, idempotency, storage trade-offs, orchestration vs business logic): `docs/ANSWERS.md` Part D

## Known limits of the sandbox verification

- **Docker was not executed.** Images could not be pulled. The Dockerfile is the provided starter one, and the Compose files were validated with `docker compose config`. PostgreSQL 16 was run directly and initialized with the same `sql/init` scripts under the same entrypoint rules.
- **Library versions differed.** Tests ran on pandas 3.0.2 and pyarrow 25.0.1. The pinned versions for your venv and the Docker image are pandas 2.2.3 and pyarrow 18.1.0 (the Airflow image gets pandas 2.1.4 and pyarrow 16.1.0). The code avoids version-specific APIs, but the first `pip install -r requirements.txt` plus `python -m pytest -q` on your machine is the real confirmation.
- **Airflow was not executed** (see above). `tests/test_dag.py` checks the DAG with the real `DagBag` inside the Airflow image. The command is in the README.
