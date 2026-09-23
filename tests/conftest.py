import os 
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Unit tests never touch a real database; provide harmless placeholders so
# src.config can be imported on a machine without a .env file.
for var, val in {"POSTGRES_HOST": "localhost", "POSTGRES_PORT": "5432", "POSTGRES_DB": "test",
                 "POSTGRES_USER": "test", "POSTGRES_PASSWORD": "not-a-real-password"}.items():
    os.environ.setdefault(var, val)


@pytest.fixture
def isolated_data(tmp_path, monkeypatch):
    """Point every data layer at a temp folder so tests never touch data/."""
    from src.config import SETTINGS

    for attr in ("raw_dir", "staging_dir", "curated_dir", "quarantine_dir", "benchmark_dir", "partitioned_dir"):
        d = tmp_path / attr
        d.mkdir()
        monkeypatch.setattr(SETTINGS, attr, d)
    src = tmp_path / "source"
    src.mkdir()
    monkeypatch.setattr(SETTINGS, "source_dir", src)
    return tmp_path
