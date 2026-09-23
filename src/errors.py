"""
Exceptions and logging shared by every stage.
 
Rule used across the project:
* A *data-quality* problem in a record (bad quantity, unknown status, orphan key)
  is NOT an exception. The record goes to data/quarantine/ with a reason and the
  run continues.
* A *system/pipeline* problem (missing file, unreadable config, database down,
  broken contract) raises a StageError. The error names the stage, keeps the
  original exception as __cause__, and makes the CLI exit non-zero so Airflow
  marks the task failed and retries it.
There is no `except: pass` anywhere in src/.
"""
from __future__ import annotations

import logging
import sys

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def get_logger(name: str) -> logging.Logger:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, stream=sys.stderr)
    return logging.getLogger(name)


class StageError(RuntimeError):
    stage = "pipeline"

    def __init__(self, message: str, **context):
        self.context = context
        ctx = " ".join(f"{k}={v}" for k, v in context.items())
        super().__init__(f"[stage={self.stage}] {message}" + (f" ({ctx})" if ctx else ""))


class EnvironmentCheckError(StageError):
    stage = "validate-env"


class ExtractError(StageError):
    stage = "extract"


class TransformError(StageError):
    stage = "transform"


class LoadError(StageError):
    stage = "load"


class ValidationError(StageError):
    stage = "validate"


class BenchmarkError(StageError):
    stage = "benchmark"
