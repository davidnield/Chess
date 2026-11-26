"""An immutable consumer evaluation binding, independent of historical count metadata.

The lookup validates its own arrays/fallback generation when opened. This receipt
pins the entry manifest, not a claim of independent source-policy acceptance.
"""
import hashlib
import json
from pathlib import Path

SCHEMA = "eval-binding-v1"


def read_binding(path):
    path = Path(path).resolve()
    raw = path.read_bytes()
    record = json.loads(raw)
    if record.get("schema") != SCHEMA or record.get("key_bits") != 128:
        raise ValueError("unsupported evaluation binding")
    root = Path(record["lookup"])
    if not root.is_absolute():
        raise ValueError("evaluation binding requires an absolute lookup path")
    manifest = root / "lookup.json"
    sha = hashlib.sha256(manifest.read_bytes()).hexdigest()
    if sha != record.get("lookup_sha256"):
        raise ValueError("evaluation binding lookup manifest changed")
    return dict(record, binding_path=str(path), binding_sha256=hashlib.sha256(raw).hexdigest())
