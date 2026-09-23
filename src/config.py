"""
The single place that turns configuration into usable settings.

* config/settings.yml  -> non-secret defaults (committed)
* .env / real env vars -> environment-specific values and secrets (never committed)

Every other module imports SETTINGS (or path_for) from here and never reads
YAML, .env or os.environ for configuration itself.

Precedence: a variable already present in the process environment wins over
.env (load_dotenv(override=False)). That is how docker-compose.yml can set
POSTGRES_HOST=postgres inside containers while .env keeps POSTGRES_HOST=localhost
for host-side commands — one contract, two contexts, no hard-coded host.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SETTINGS_FILE = PROJECT_ROOT / "config" / "settings.yml"
ENV_FILE = PROJECT_ROOT / ".env"

REQUIRED_DB_VARS = ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")

load_dotenv(ENV_FILE, override=False)


class ConfigError(RuntimeError):
    """Configuration is missing or invalid (a system problem, not a data problem)."""


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    connect_timeout: int

    def connect_kwargs(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "connect_timeout": self.connect_timeout,
        }

    def describe(self) -> str:
        """Safe for logs: never includes the password."""
        return f"{self.user}@{self.host}:{self.port}/{self.dbname}"


class Settings:
    def __init__(self, raw: dict):
        self.raw = raw
        paths = raw["paths"]
        self.source_dir = self._resolve(os.getenv("PIPELINE_SOURCE_DIR") or paths["source_dir"])
        self.raw_dir = self._resolve(paths["raw_dir"])
        self.staging_dir = self._resolve(paths["staging_dir"])
        self.curated_dir = self._resolve(paths["curated_dir"])
        self.quarantine_dir = self._resolve(paths["quarantine_dir"])
        self.benchmark_dir = self._resolve(paths["benchmark_dir"])
        self.partitioned_dir = self._resolve(paths["partitioned_dir"])

        self.source_files: dict = raw["source_files"]
        self.order_rules: dict = raw["staging"]["orders"]
        self.curated_table: str = raw["curated"]["table"]
        self.conflict_key: str = raw["curated"]["conflict_key"]
        self.hash_columns: list = raw["curated"]["hash_columns"]
        self.benchmark_repeats: int = int(raw["benchmark"]["repeats"])
        self.benchmark_filter_status: str = raw["benchmark"]["filter_status"]
        self.load_page_size: int = int(raw["postgres"]["load_page_size"])
        self._connect_timeout = int(raw["postgres"]["connect_timeout_seconds"])

    @staticmethod
    def _resolve(p: str) -> Path:
        path = Path(p)
        return path if path.is_absolute() else PROJECT_ROOT / path

    @property
    def curated_file(self) -> Path:
        return self.curated_dir / "sales_order_lines.parquet"

    def missing_db_vars(self) -> list[str]:
        return [v for v in REQUIRED_DB_VARS if not os.getenv(v)]

    @property
    def postgres(self) -> PostgresSettings:
        missing = self.missing_db_vars()
        if missing:
            raise ConfigError(
                f"Missing environment variable(s) {missing}. Copy .env.example to .env and fill them in."
            )
        return PostgresSettings(
            host=os.environ["POSTGRES_HOST"],
            port=int(os.environ["POSTGRES_PORT"]),
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
            connect_timeout=self._connect_timeout,
        )


def load_settings(path: Path = SETTINGS_FILE) -> Settings:
    if not path.exists():
        raise ConfigError(f"Non-secret settings file not found: {path}")
    with open(path, encoding="utf-8") as fh:
        return Settings(yaml.safe_load(fh) or {})


SETTINGS = load_settings()


def path_for(layer: str) -> Path:
    """Starter-code helper: path_for('raw') -> data/raw, etc."""
    mapping = {
        "source": SETTINGS.source_dir,
        "raw": SETTINGS.raw_dir,
        "staging": SETTINGS.staging_dir,
        "curated": SETTINGS.curated_dir,
        "quarantine": SETTINGS.quarantine_dir,
        "benchmarks": SETTINGS.benchmark_dir,
        "partitioned": SETTINGS.partitioned_dir,
    }
    try:
        return mapping[layer]
    except KeyError as err:
        raise ConfigError(f"Unknown layer '{layer}'. Valid: {sorted(mapping)}") from err
