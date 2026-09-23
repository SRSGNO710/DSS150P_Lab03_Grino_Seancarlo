"""Thin PostgreSQL connection helper shared by load/, validate/ and benchmark/."""
from __future__ import annotations

from src.config import SETTINGS, ConfigError


def connect(error_cls):
    """Open a psycopg2 connection or raise `error_cls` (a StageError subclass)
    carrying the target (never the password) and the original cause."""
    try:
        import psycopg2
    except ImportError as err:
        raise error_cls("psycopg2 is not installed; run pip install -r requirements.txt") from err
    try:
        pg = SETTINGS.postgres
    except ConfigError as err:
        raise error_cls(str(err)) from err
    try:
        return psycopg2.connect(**pg.connect_kwargs())
    except psycopg2.Error as err:
        raise error_cls(
            f"Cannot connect to PostgreSQL at {pg.describe()}: {str(err).strip()}. "
            "Is `docker compose up -d postgres` running, and is POSTGRES_HOST right for "
            "this context (localhost on the host, postgres inside containers)?"
        ) from err
