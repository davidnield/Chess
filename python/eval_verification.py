"""Success receipt for the independent check of an eval DB (`verify_evals.py`), added 2026-11-20.

The ply-51 coordinator (`run_ply51_evals.py`) used to rerun the whole independent verification on
every restart, even when only the lookup after it had been interrupted. It now writes this receipt
when, and only when, the verifier exits 0 with every check passed, and reuses it on a later start
only if the identity recomputed then equals the identity the receipt recorded:

  db        the DB directory, the sha256 of _build.meta.json, _manifest.parquet and _DONE, and the
            size and mtime of every other top-level file (the 512 buckets among them; the manifest
            carries their content hashes, which the verifier checked)
  book      the explorer book directory, the sha256 of _book.meta.json and _BOOK.DONE's presence
  sources   size and mtime of every raw Cloud/Fishnet file the build meta lists (the meta, hashed
            above, records their download sha256)
  verifier  sha256 of verify_evals.py and the local modules it imports
  native    sha256 of the explorer-extract binary that built the DB
  settings  the verifier's arguments, which choose the sample and its resources

A receipt that does not parse, names another schema, fails its own digest or does not record a
pass is an error, never a reason to reuse or to silently overwrite: the coordinator stops and a
person looks. A well-formed receipt whose identity differs is stale: it is set aside and the
verification runs again. A log line, _DONE, file presence or mtime alone never counts as a pass.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path

SCHEMA = "eval-verification-receipt-v1"
VERIFIER_FILES = ("verify_evals.py", "explorer_storage.py", "duck_compat.py", "zobrist.py", "zobrist128.py")
DB_HASHED = ("_build.meta.json", "_manifest.parquet", "_DONE")


class ReceiptError(ValueError):
    """The receipt exists but cannot be trusted."""


def file_sha(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8 << 20):
            h.update(chunk)
    return h.hexdigest()


def _stat(path: Path) -> list:
    s = path.stat()
    return [s.st_size, s.st_mtime_ns]


def digest(obj) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode("utf-8")).hexdigest()


def identity(db: Path, book: Path, verifier_dir: Path, native: Path, settings: dict) -> dict:
    meta = json.loads((db / "_build.meta.json").read_text(encoding="utf-8"))
    inputs = meta["inputs"]
    sources = {}
    for kind, files in (("cloud", inputs["cloud_files"]), ("fishnet", inputs["fishnet_files"])):
        root = Path(inputs[kind])
        for name in files:
            sources[f"{kind}/{name}"] = _stat(root / name)
    return {
        "db": str(Path(db).resolve()),
        "db_schema": meta.get("schema"),
        "db_hashes": {name: file_sha(db / name) for name in DB_HASHED},
        "db_files": {p.name: _stat(p) for p in sorted(Path(db).iterdir())
                     if p.is_file() and p.name not in DB_HASHED},
        "book": str(Path(book).resolve()),
        "book_meta_sha256": file_sha(book / "_book.meta.json"),
        "book_done": (book / "_BOOK.DONE").is_file(),
        "db_book_meta_sha256": inputs.get("book_meta_sha256"),
        "sources": sources,
        "verifier": {name: file_sha(verifier_dir / name) for name in VERIFIER_FILES},
        "native_sha256": file_sha(native),
        "settings": settings,
    }


def differences(recorded: dict, current: dict) -> list[str]:
    """The top-level identity fields that differ (empty: the receipt applies)."""
    return sorted(k for k in set(recorded) | set(current) if recorded.get(k) != current.get(k))


def _body_digest(body: dict) -> str:
    return digest({k: v for k, v in body.items() if k != "digest"})


def write_receipt(path: Path, ident: dict, result: dict, log: Path) -> dict:
    """Publish a receipt for a verification that passed. Refuses anything that is not a pass."""
    if result.get("all_pass") is not True or result.get("failures") or not result.get("checks"):
        raise ValueError(f"verification did not pass: {result.get('failures')}")
    if result.get("db_meta_sha256") != ident["db_hashes"]["_build.meta.json"]:
        raise ValueError("the verifier checked another DB generation")
    body = {"schema": SCHEMA, "identity": ident, "result": result, "log": str(log),
            "log_sha256": file_sha(log), "written": dt.datetime.now().isoformat(timespec="seconds")}
    body["digest"] = _body_digest(body)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(body, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)
    return body


def load_receipt(path: Path) -> dict:
    try:
        body = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ReceiptError(f"unreadable verification receipt {path}: {exc}") from exc
    if not isinstance(body, dict) or body.get("schema") != SCHEMA:
        raise ReceiptError(f"{path} is not an {SCHEMA}")
    if body.get("digest") != _body_digest(body):
        raise ReceiptError(f"{path} fails its own digest")
    result = body.get("result") or {}
    if result.get("all_pass") is not True or result.get("failures") or not isinstance(body.get("identity"), dict):
        raise ReceiptError(f"{path} does not record a pass")
    return body
