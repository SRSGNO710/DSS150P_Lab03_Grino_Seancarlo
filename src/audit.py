from datetime import datetime, timezone 
import os
import hashlib
import json
import uuid

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def new_run_id() -> str:
    return os.getenv('PIPELINE_RUN_ID') or f"run_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}"

def record_hash(record: dict, keys: list[str]) -> str:
    payload = {k: record.get(k) for k in keys}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()
