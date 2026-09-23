"""validate-env: prove the environment and configuration contract are usable."""
from __future__ import annotations

import importlib
import platform
import sys

from src.config import SETTINGS, ConfigError
from src.errors import EnvironmentCheckError, get_logger

log = get_logger(__name__)

REQUIRED_PACKAGES = {
    "pandas": "pandas",
    "pyarrow": "pyarrow",
    "psycopg2": "psycopg2-binary",
    "yaml": "PyYAML",
    "dotenv": "python-dotenv",
}


def validate_env(require_db: bool = False) -> dict:
    problems: list[str] = []
    warnings: list[str] = []

    packages = {}
    for module, dist in REQUIRED_PACKAGES.items():
        try:
            mod = importlib.import_module(module)
            packages[dist] = getattr(mod, "__version__", "installed")
        except ImportError:
            packages[dist] = "MISSING"
            problems.append(f"Python package {dist} is not installed")

    for name in SETTINGS.source_files.values():
        if not (SETTINGS.source_dir / name).is_file():
            problems.append(f"Source file missing: {SETTINGS.source_dir / name}")

    for layer_dir in (SETTINGS.raw_dir, SETTINGS.staging_dir, SETTINGS.curated_dir,
                      SETTINGS.quarantine_dir, SETTINGS.benchmark_dir, SETTINGS.partitioned_dir):
        layer_dir.mkdir(parents=True, exist_ok=True)

    missing_vars = SETTINGS.missing_db_vars()
    if missing_vars:
        problems.append(f"Missing environment variables: {missing_vars} (copy .env.example to .env)")

    db_status = "not checked"
    if not missing_vars and "MISSING" not in (packages["psycopg2-binary"],):
        db_status = _check_database()
        if not db_status.startswith("ok"):
            (problems if require_db else warnings).append(f"PostgreSQL: {db_status}")

    try:
        target = SETTINGS.postgres.describe()
    except ConfigError:
        target = "unconfigured"

    report = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "packages": packages,
        "source_dir": str(SETTINGS.source_dir),
        "postgres_target": target,
        "postgres": db_status,
        "warnings": warnings,
        "problems": problems,
        "status": "FAILED" if problems else "OK",
    }
    if problems:
        raise EnvironmentCheckError("; ".join(problems))
    return report


def _check_database() -> str:
    import psycopg2

    try:
        conn = psycopg2.connect(**SETTINGS.postgres.connect_kwargs())
    except psycopg2.Error as err:
        return f"unreachable ({str(err).strip().splitlines()[0]})"
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT string_agg(schema_name, ',' ORDER BY schema_name) FROM information_schema.schemata "
                "WHERE schema_name IN ('staging','curated','audit')"
            )
            schemas = cur.fetchone()[0] or ""
        missing = {"staging", "curated", "audit"} - set(schemas.split(","))
        return f"ok (schemas: {schemas})" if not missing else f"connected but schemas missing: {sorted(missing)}"
    finally:
        conn.close()
