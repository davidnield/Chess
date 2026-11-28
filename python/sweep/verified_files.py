"""Verification of immutable input files against their manifests' SHA-256, once per process or once per receipt.

The pool readers, the bound eval lookups and pinned artifacts check every file against the SHA-256 its
manifest names before using it. One budget-book build opens the same publication through several
readers, each of which used to hash every file it touched again, one at a time: ~3 TB and ~2 h per
Black build (measured 27 Nov 2026). Two modes, chosen by ``CHESS_VERIFY_MODE``:

``full`` (the default; the original contract). A file is hashed in full the first time any reader in
this process needs it, and again whenever its (size, mtime_ns) fingerprint changes, so no reader uses
bytes that were not verified against their manifest in this process. ``verify_many`` hashes the files
a fetch needs on several threads (hashlib releases the GIL; 8 threads read 2.2 GB/s from the NVMe
pools, one thread 0.42).

``receipt`` (David, 27 Nov 2026; for sweeps that build dozens of books from the same publications).
The first full hash of a file writes a receipt record: its size, mtime_ns, file ID (NTFS file index)
and SHA-256, under ``CHESS_VERIFY_RECEIPTS`` (default D:/chess/verify-receipts), never inside the
publication. Later processes accept a file whose current size, mtime_ns and file ID equal a record for
the expected SHA-256, without reading it. Any rewrite, replacement or truncation changes at least one
of them and is hashed in full again (and refused if it differs). What this gives up: corruption that
leaves size and mtime untouched goes unseen until a full check. Two guards bound that:

- a random ``CHESS_VERIFY_SAMPLE`` fraction (default 1%) of receipt-accepted files is hashed in full
  anyway, and a mismatch is an error;
- ``python -m sweep.verified_files --check DIR ...`` re-hashes every receipt record under the given
  directories in full; run it before a pilot acceptance report, before a book is promoted to final,
  and after any drive was disconnected.

``--warm MANIFEST ...`` hashes every file named by the given manifests in full, in parallel, and records
receipts, so the first build of a sweep does not pay for it either.

Acceptance is still a full hash; a receipt only records that nothing has changed since one.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from .stages import file_sha

THREADS = int(os.environ.get("CHESS_VERIFY_THREADS", "8"))
RECEIPT_SCHEMA = "verified-files-receipt-v1"
_lock = threading.Lock()
_verified: dict[tuple[str, str], tuple[int, int]] = {}
_receipts: dict[str, dict] = {}
stats = dict(hashed_files=0, hashed_bytes=0, memo_hits=0, receipt_hits=0, sampled_files=0, sampled_bytes=0)


def mode():
    m = os.environ.get("CHESS_VERIFY_MODE", "full")
    if m not in ("full", "receipt"):
        raise ValueError(f"CHESS_VERIFY_MODE must be full or receipt, not {m!r}")
    return m


def receipts_root():
    return Path(os.environ.get("CHESS_VERIFY_RECEIPTS", "D:/chess/verify-receipts"))


def sample_fraction():
    return float(os.environ.get("CHESS_VERIFY_SAMPLE", "0.01"))


def provenance():
    """For build metadata: how this process verified its inputs (None in the default full mode)."""
    if mode() == "full":
        return None
    with _lock:
        return dict(mode="receipt", receipts=str(receipts_root()), sample_fraction=sample_fraction(),
                    stats=dict(stats))


def _key(path, sha):
    return str(Path(path).resolve()), sha


def _fingerprint(path):
    st = Path(path).stat()
    return st.st_size, st.st_mtime_ns


def _identity(path):
    st = Path(path).stat()
    return dict(size=st.st_size, mtime_ns=st.st_mtime_ns, file_id=st.st_ino, device=st.st_dev)


def _receipt_path(directory):
    tag = hashlib.sha256(str(directory).lower().encode()).hexdigest()[:24]
    return receipts_root() / f"{tag}.json"


def _load_receipt(directory):
    """Caller holds _lock."""
    if directory not in _receipts:
        p = _receipt_path(directory)
        rec = json.loads(p.read_text(encoding="utf8")) if p.exists() else None
        if rec is not None and (rec.get("schema") != RECEIPT_SCHEMA or rec.get("directory") != directory):
            raise ValueError(f"{p}: not a verification receipt for {directory}")
        _receipts[directory] = rec or dict(schema=RECEIPT_SCHEMA, directory=directory, files={})
    return _receipts[directory]


def _save_receipts(directories):
    """Merge this process's records into each directory's receipt (atomic replace; a lost race only
    costs a later re-hash)."""
    root = receipts_root()
    root.mkdir(parents=True, exist_ok=True)
    for d in directories:
        p = _receipt_path(d)
        with _lock:
            mine = dict(_receipts[d]["files"])
        disk = json.loads(p.read_text(encoding="utf8"))["files"] if p.exists() else {}
        disk.update(mine)
        tmp = p.with_name(f"{p.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        tmp.write_text(json.dumps(dict(schema=RECEIPT_SCHEMA, directory=d, files=disk), indent=1), encoding="utf8")
        os.replace(tmp, p)
        with _lock:
            _receipts[d]["files"] = disk


def _drop(directories, paths):
    """Remove records for ``paths`` from the on-disk receipts too (the merge in _save_receipts keeps
    disk records this process does not hold)."""
    for d in directories:
        p = _receipt_path(d)
        if not p.exists():
            continue
        rec = json.loads(p.read_text(encoding="utf8"))
        keep = {n: r for n, r in rec["files"].items() if str(Path(d) / n) not in paths}
        if len(keep) != len(rec["files"]):
            tmp = p.with_name(f"{p.name}.{os.getpid()}.{threading.get_ident()}.tmp")
            tmp.write_text(json.dumps(dict(rec, files=keep), indent=1), encoding="utf8")
            os.replace(tmp, p)


def _receipt_ok(path, sha):
    resolved = Path(path).resolve()
    with _lock:
        rec = _load_receipt(str(resolved.parent))["files"].get(resolved.name)
    return rec is not None and rec["sha256"] == sha and {k: rec[k] for k in ("size", "mtime_ns", "file_id", "device")} == _identity(path)


def is_verified(path, sha):
    with _lock:
        return _verified.get(_key(path, sha)) == _fingerprint(path)


def verify_many(items, threads=None):
    """``items``: (path, expected sha256). Raise ValueError naming the first file whose bytes differ."""
    receipt = mode() == "receipt"
    todo, sampled = [], []
    for path, sha in items:
        k = _key(path, sha)
        fp = _fingerprint(path)
        with _lock:
            if _verified.get(k) == fp:
                stats["memo_hits"] += 1
                continue
        if receipt and _receipt_ok(path, sha):
            if random.random() < sample_fraction():
                sampled.append((path, sha, fp))
                todo.append((path, sha, fp))
            else:
                with _lock:
                    _verified[k] = fp
                    stats["receipt_hits"] += 1
            continue
        todo.append((path, sha, fp))
    if not todo:
        return
    def check(item):
        path, sha, before = item
        identity = _identity(path) if receipt else None
        got = file_sha(path)
        return path, sha, before, got, _fingerprint(path), identity
    n = max(1, min(threads or THREADS, len(todo)))
    if n == 1:
        results = [check(t) for t in todo]
    else:
        with ThreadPoolExecutor(n) as ex:
            results = list(ex.map(check, todo))
    bad = [r for r in results if r[3] != r[1] or r[2] != r[4]]
    sampled_keys = {_key(p, s) for p, s, _ in sampled}
    touched = set()
    with _lock:
        for path, sha, before, got, after, identity in results:
            if got == sha and before == after:
                _verified[_key(path, sha)] = after
                if _key(path, sha) in sampled_keys:
                    stats["sampled_files"] += 1
                    stats["sampled_bytes"] += after[0]
                else:
                    stats["hashed_files"] += 1
                    stats["hashed_bytes"] += after[0]
                if receipt and identity == _identity(path):
                    resolved = Path(path).resolve()
                    _load_receipt(str(resolved.parent))["files"][resolved.name] = dict(
                        identity, sha256=sha, verified_utc=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
                    touched.add(str(resolved.parent))
    if receipt and bad:
        # a file that failed must not stay acceptable by its receipt
        with _lock:
            for path, *_ in bad:
                resolved = Path(path).resolve()
                if _load_receipt(str(resolved.parent))["files"].pop(resolved.name, None) is not None:
                    touched.add(str(resolved.parent))
        _drop(sorted(touched), {str(Path(r[0]).resolve()) for r in bad})
    if touched:
        _save_receipts(sorted(touched))
    if bad:
        path, sha, before, got, after, _ = bad[0]
        reason = ("changed while being hashed" if before != after else
                  "a receipt-accepted file failed its random full check" if _key(path, sha) in sampled_keys else
                  "hash differs from its manifest")
        raise ValueError(f"{path}: {reason}")


def forget():
    """Tests only: drop the process memo and cached receipts."""
    with _lock:
        _verified.clear()
        _receipts.clear()
        for k in stats:
            stats[k] = 0


def check_directories(directories, threads=None):
    """Re-hash every receipt record under ``directories`` in full; return (checked, mismatched names)."""
    wanted = [str(Path(d).resolve()) for d in directories]
    root = receipts_root()
    records = []
    for p in sorted(root.glob("*.json")) if root.exists() else []:
        rec = json.loads(p.read_text(encoding="utf8"))
        if rec.get("schema") == RECEIPT_SCHEMA and any(
                rec["directory"].lower() == w.lower() or rec["directory"].lower().startswith(w.lower() + os.sep)
                for w in wanted):
            records += [(Path(rec["directory"]) / name, r["sha256"]) for name, r in rec["files"].items()]
    def one(item):
        path, sha = item
        return path, path.exists() and file_sha(path) == sha
    with ThreadPoolExecutor(max(1, threads or THREADS)) as ex:
        results = list(ex.map(one, records))
    bad = [str(p) for p, ok in results if not ok]
    if bad:
        _drop(sorted({str(Path(b).parent) for b in bad}), set(bad))
    return len(results), bad


def manifest_items(path):
    """(file, sha256) for every file a publication manifest names: a partitioned pool's edges.json /
    aux.json, a count pool's pool.json, or an eval lookup's lookup.json (with its base generation)."""
    path = Path(path).resolve()
    m = json.loads(path.read_text(encoding="utf8"))
    if path.name == "lookup.json":
        items = [(path.parent / name, rec["sha256"]) for name, rec in sorted(m["buckets"].items())]
        if m.get("base"):
            items += manifest_items(Path(m["base"]["root"]) / "lookup.json")
        return items
    if isinstance(m.get("files"), list):
        return [(path.parent / f["file"], f["sha256"]) for f in m["files"]]
    raise ValueError(f"{path}: not a manifest this tool knows")


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--check", nargs="+", metavar="DIR",
                   help="re-hash in full every receipt record under these directories")
    g.add_argument("--warm", nargs="+", metavar="MANIFEST",
                   help="receipt mode: hash every file these manifests name (edges.json, aux.json, pool.json, "
                        "lookup.json) in full now and record receipts, so builds start without hashing")
    p.add_argument("--threads", type=int, default=None)
    a = p.parse_args()
    t = time.time()
    if a.warm:
        if mode() != "receipt":
            raise SystemExit("--warm records receipts: set CHESS_VERIFY_MODE=receipt")
        items = [i for m in a.warm for i in manifest_items(m)]
        os.environ["CHESS_VERIFY_SAMPLE"] = "0"          # every file not yet receipted is hashed in full
        for start in range(0, len(items), 256):        # receipts saved as it goes
            verify_many(items[start:start + 256], a.threads)
        print(json.dumps(dict(files=len(items), stats=stats, seconds=round(time.time() - t)), indent=2))
        return
    n, bad = check_directories(a.check, a.threads)
    print(json.dumps(dict(checked=n, mismatched=bad, seconds=round(time.time() - t)), indent=2))
    raise SystemExit(1 if bad else 0)


if __name__ == "__main__":
    main()
