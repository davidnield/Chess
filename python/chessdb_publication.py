"""Verify and publish completed ChessDB menus without repeating extraction.

CLI: python chessdb_publication.py --menus E:/chess/chessdb-menus-20260702 --workers 4
Every invocation hashes position and move bytes against the original receipts.
_MENUS.COMPLETE.json is distinct from the _done directory even on Windows.
Resumable verification records are evidence, never substitutes for input checks.
No cleanup or input rewriting is performed.
"""
from __future__ import annotations
import argparse
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
from pathlib import Path
import pyarrow.parquet as pq
from month_archive import read_json, write_json, file_lock, sha
from chessdb_integrity import exact_buckets, fingerprint, unchanged

MARKER = "_MENUS.COMPLETE.json"
SCHEMA = "chessdb-menu-publication-v1"

def verify_bucket(root, b):
    root = Path(root)
    receipt = root / "_done" / f"bkt{b:03d}.json"
    ri = fingerprint(receipt)
    data = read_json(receipt)
    if data.get("bucket") != b or data.get("bad_key", 0) or data.get("bad_move", 0):
        raise ValueError(f"invalid original receipt at bucket {b}")
    if not 0 <= data["found"] <= data["positions"]:
        raise ValueError(f"invalid found count at bucket {b}")
    result = dict(bucket=b, receipt=ri)
    for kind in ("positions", "moves"):
        p = root / kind / f"bkt{b:03d}.parquet"
        ident = fingerprint(p, data[f"{kind}_sha256"])
        header = pq.ParquetFile(p)
        if header.metadata.num_rows != data[kind]:
            raise ValueError(f"row count differs: {p}")
        columns = set(header.schema_arrow.names)
        required = {"lo", "hi", "found", "best_score", "white_to_move"} if kind == "positions" else {"lo", "hi", "uci"}
        if not required <= columns:
            raise ValueError(f"missing columns in {p}: {required-columns}")
        unchanged(p, ident)
        result[kind] = dict(rows=data[kind], **ident)
    unchanged(receipt, ri)
    result["found"] = data["found"]
    result["duplicate_keys_resolved_by_lookup"] = data.get("duplicate_keys_resolved_by_lookup", 0)
    return result

def publish(root, workers=4, expected=range(512)):
    root = Path(root)
    buckets = list(expected)
    if workers < 1:
        raise ValueError("workers must be positive")
    with file_lock(root / "_publication.lock"):
        names = [f"bkt{b:03d}" for b in buckets]
        exact_buckets(root / "_done", "*.json", [n+".json" for n in names])
        for kind in ("positions", "moves"):
            exact_buckets(root / kind, "*.parquet", [n+".parquet" for n in names])
        params = fingerprint(root / "_params.json")
        records = {}
        write_json(root / "_publication-status.json", dict(state="verifying", completed=0, total=len(buckets)))
        with cf.ThreadPoolExecutor(max_workers=workers) as executor:
            pending = {executor.submit(verify_bucket, root, b): b for b in buckets}
            for future in cf.as_completed(pending):
                b = pending[future]
                rec = future.result()
                records[b] = rec
                write_json(root / "_verification" / f"bkt{b:03d}.json", rec)
                if len(records) % 16 == 0 or len(records) == len(buckets):
                    write_json(root / "_publication-status.json", dict(state="verifying", completed=len(records), total=len(buckets)))
                    print(f"verified {len(records)}/{len(buckets)} buckets", flush=True)
        unchanged(root / "_params.json", params)
        for b, rec in records.items():
            unchanged(root / "_done" / f"bkt{b:03d}.json", rec["receipt"])
            for kind in ("positions", "moves"):
                unchanged(root / kind / f"bkt{b:03d}.parquet", rec[kind])
        result = dict(schema=SCHEMA, written=dt.datetime.now(dt.timezone.utc).isoformat(),
                      params=params, verifier_sha256=sha(Path(__file__)),
                      integrity_sha256=sha(Path(__file__).with_name("chessdb_integrity.py")),
                      buckets={str(b): records[b] for b in sorted(records)},
                      totals={k: sum(r[k] for r in records.values()) for k in ("found", "duplicate_keys_resolved_by_lookup")})
        for kind in ("positions", "moves"):
            result["totals"][kind] = sum(r[kind]["rows"] for r in records.values())
        # A bounded synthetic test may verify buckets but must never publish a full generation.
        if buckets == list(range(512)):
            write_json(root / MARKER, result)
        write_json(root / "_publication-status.json", dict(state="verified" if len(buckets)==512 else "partial-verified", completed=len(records), total=len(buckets), totals=result["totals"]))
        return result

if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--menus", required=True, type=Path)
    p.add_argument("--workers", type=int, default=4)
    a = p.parse_args()
    r = publish(a.menus, a.workers)
    print(json.dumps(r["totals"], sort_keys=True))
