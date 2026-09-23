"""
Raw layer: copy immutable source snapshots into data/raw/run_id=<run_id>/.

Only copies bytes and records a manifest (size, SHA-256, physical line/record
count). No parsing into business types, no cleaning, no business rules.
Files in data/source/ are opened read-only and never modified.
"""
from __future__ import annotations

import hashlib
import json
import re
import shutil
from pathlib import Path

from src.audit import utc_now_iso
from src.config import SETTINGS, path_for
from src.errors import ExtractError, get_logger

log = get_logger(__name__)


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _physical_records(path: Path) -> int:
    """Physical data records as delivered (CSV lines minus header / JSON array length)."""
    if path.suffix == ".json":
        with open(path, encoding="utf-8") as fh:
            return len(json.load(fh))
    with open(path, encoding="utf-8") as fh:
        return max(sum(1 for line in fh if line.strip()) - 1, 0)


def raw_dir_for(run_id: str) -> Path:
    """Folder for one run. Airflow run ids contain ':' and '+'
    (manual__2026-01-01T00:00:00+00:00), which are invalid in Windows paths
    (Docker Desktop bind mounts), so the folder name is sanitized. The exact
    run id is still stored in pipeline_run_id columns and in _manifest.json."""
    safe = re.sub(r"[^A-Za-z0-9_.=-]", "_", run_id)
    return path_for("raw") / f"run_id={safe}"


def extract_sources(run_id: str) -> Path:
    """Copy customers.csv, products.json and orders.csv into a run-specific raw
    folder and return it. Safe to rerun for the same run_id (files are replaced
    with identical copies)."""
    source_dir = path_for("source")
    target = raw_dir_for(run_id)
    manifest = {"pipeline_run_id": run_id, "extracted_at_utc": utc_now_iso(),
                "source_dir": str(source_dir), "files": {}}
    try:
        target.mkdir(parents=True, exist_ok=True)
        for logical, name in SETTINGS.source_files.items():
            src = source_dir / name
            if not src.is_file():
                raise ExtractError(f"Source file not found: {src}", dataset=logical, run_id=run_id)
            dst = target / name
            shutil.copy2(src, dst)
            src_hash, dst_hash = sha256_of(src), sha256_of(dst)
            if src_hash != dst_hash:
                raise ExtractError(f"Raw copy of {name} differs from source", run_id=run_id)
            manifest["files"][logical] = {
                "file": name,
                "bytes": dst.stat().st_size,
                "sha256": dst_hash,
                "physical_records": _physical_records(dst),
            }
        with open(target / "_manifest.json", "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2)
    except ExtractError:
        raise
    except (OSError, ValueError) as err:
        raise ExtractError(f"Could not snapshot sources into {target}: {err}", run_id=run_id) from err

    log.info("Raw snapshot %s: %s", target,
             {k: v["physical_records"] for k, v in manifest["files"].items()})
    return target
