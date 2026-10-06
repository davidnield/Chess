"""Shared, memory-mapped (position_hash -> eval_cp) arrays.

The canonical eval DB is D:/chess/eval_full (`explorer-extract evals`: 5.9B rows
keyed by (position_hash, epd), 196 GB). No consumer can load that, and the fused
extract needs the lookup inside every worker. So it is materialised ONCE as two
.npy files sorted by hash (6.0B entries, ~60 GB), np.load(mmap_mode) by each
consumer. Windows backs mmaps of the same file with the same page-cache pages, so
N workers share one resident copy instead of N private ones.

eval_cp is stored int16. Every source caps decisive evals at +-2000, which fits,
and the cast is checked at build time rather than assumed — a silent wrap here
would turn a won position into a lost one.

Lookups are BATCHED on purpose. A binary search over a 48 GB array is ~33 random
accesses; doing that per ply during a replay is cache-hostile. Sorting the query
batch first turns the searches into a mostly-sequential sweep, which is why
lookup_evals sorts, searches, then scatters back.

Usage:
    h, e = open_eval_arrays()                                     # the defaults
    cp = lookup_evals(np.array([...], dtype=np.int64), h, e)      # MISSING where absent

Two kinds of source. An eval DB DIRECTORY (the default, D:/chess/eval_full) is
built by eval_arrays_build.py, with the hashes a hash-only lookup must not answer
(collision twins, ambiguous child hashes) excluded and the book's checkmates added.
Its fingerprint is the directory's _DONE, manifest, build meta and every bucket
file's (name, size, mtime), plus the book it was matched against:

    .venv/Scripts/python.exe python/eval_arrays.py [--force]     # ~2 h, ~120 GB temp

A single (position_hash, eval_cp) parquet -- the retired unified_eval_db, one row
per hash -- is read here: pass --eval-db <file> --out-dir <dir>.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import numpy as np

DEFAULT_EVAL_DB = Path("D:/chess/eval_full")
DEFAULT_ARRAY_DIR = Path("D:/chess/eval_arrays_full")

# Sentinel for "this position is not in the eval DB". Outside any real eval
# (capped at +-2000) and outside int16's usable range for
# real data, so it can never collide with a genuine evaluation.
MISSING = np.int16(-32768)

META_NAME = "eval_arrays.meta.json"

# The meta `kind` of arrays built from an eval DB directory, and the fields of its
# fingerprint a verify compares.
DIR_KIND = "eval-db-dir-v1"
DIR_FP_KEYS = ("done", "manifest_sha256", "build_meta_sha256", "bucket_files",
               "bucket_stat_digest", "source_rows")
BOOK_FP_KEYS = ("book_meta_sha256", "collisions_sha256")


def _paths(array_dir: Path) -> tuple[Path, Path]:
    return array_dir / "eval_hash.npy", array_dir / "eval_cp.npy"


def is_eval_db_dir(p: Path) -> bool:
    """An eval DB directory (explorer-extract evals), not a single parquet."""
    p = Path(p)
    return p.is_dir() and (p / "_manifest.parquet").is_file()


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while chunk := f.read(8 << 20):
            h.update(chunk)
    return h.hexdigest()


def dir_fingerprint(db: Path) -> dict:
    """Identity of an eval DB directory, cheap enough to check before every run:
    _DONE, the sha256 of _manifest.parquet and _build.meta.json, a digest of every
    bucket file's (name, size, mtime) -- a bucket replaced without a manifest update
    is caught -- and the row count from the manifest (read with pyarrow: Polars
    cannot read that file). A directory without _DONE is incomplete: refused."""
    import pyarrow.parquet as pq
    db = Path(db)
    done = db / "_DONE"
    if not done.is_file():
        raise FileNotFoundError(f"{db} has no _DONE: the eval DB is incomplete")
    h = hashlib.sha256()
    n = 0
    for p in sorted(db.glob("bkt*.parquet")):
        st = p.stat()
        h.update(f"{p.name} {st.st_size} {st.st_mtime_ns}\n".encode())
        n += 1
    rows = pq.ParquetFile(db / "_manifest.parquet").read(columns=["rows"]).column("rows").to_pylist()
    return {"source": str(db), "kind": DIR_KIND,
            "done": done.read_text(encoding="utf-8").strip(),
            "manifest_sha256": _sha256(db / "_manifest.parquet"),
            "build_meta_sha256": _sha256(db / "_build.meta.json"),
            "bucket_files": n, "bucket_stat_digest": h.hexdigest(),
            "source_rows": int(sum(rows))}


def book_fingerprint(book: Path) -> dict:
    """The explorer book the arrays were matched against: its collision list decides
    which hashes are excluded, its moves which checkmates are added."""
    book = Path(book)
    return {"book": str(book),
            "book_meta_sha256": _sha256(book / "_book.meta.json"),
            "collisions_sha256": _sha256(book / "_collisions.parquet")}


# ── staleness ─────────────────────────────────────────────────────────────────
# These arrays are a DERIVED copy of the eval DB, and the skip gate used to be
# `if the .npy files exist, use them`. That is silent corruption waiting to
# happen: rebuild the eval DB (which has already happened twice) and every later
# extract keeps reading the OLD evals, with no error. It would land in the
# child_eval feeding the other-moves bucket (and in Stage 3 itself), so the
# repertoire would shift for a reason nothing in the logs could explain.
#
# So the arrays record what they were built from, and callers verify.

def source_fingerprint(eval_db: Path) -> dict:
    """Identity of the source DB: size + mtime + row count.

    Row count is read from the parquet FOOTER (no column data), so this stays
    cheap enough to call before every run.
    """
    import pyarrow.parquet as pq
    if is_eval_db_dir(eval_db):
        return dir_fingerprint(eval_db)
    st = eval_db.stat()
    return {"source": str(eval_db), "size": st.st_size,
            "mtime_ns": st.st_mtime_ns,
            "n_rows": pq.read_metadata(eval_db).num_rows}


def read_meta(array_dir: Path = DEFAULT_ARRAY_DIR) -> dict | None:
    p = array_dir / META_NAME
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _write_meta(array_dir: Path, fp: dict) -> None:
    p = array_dir / META_NAME
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(fp, indent=2), encoding="utf-8")
    tmp.replace(p)


def verify_eval_arrays(array_dir: Path = DEFAULT_ARRAY_DIR,
                       eval_db: Path | None = None,
                       adopt: bool = True) -> str:
    """Raise unless the arrays match the eval DB they were built from.

    Returns a one-line status for logging. Raises FileNotFoundError if the
    arrays or the source are absent, ValueError if they no longer agree.

    `adopt` handles arrays built before this metadata existed: there is no
    fingerprint to compare, but if the row count matches the source AND the
    arrays post-date it, they are almost certainly current — record the
    fingerprint and move on rather than forcing a needless 400M-row rebuild.
    A mismatch on either signal is still a hard failure.
    """
    hp, cp = _paths(array_dir)
    if not (hp.exists() and cp.exists()):
        raise FileNotFoundError(
            f"eval arrays missing at {array_dir}. Build them with:\n"
            f"    .venv/Scripts/python.exe python/eval_arrays.py --out-dir {array_dir}")
    meta = read_meta(array_dir)
    src = Path(eval_db or (meta or {}).get("source") or DEFAULT_EVAL_DB)
    if not src.exists():
        raise FileNotFoundError(
            f"eval arrays at {array_dir} cannot be verified: their source "
            f"{src} is gone. Point --eval-db at the current DB or rebuild.")
    if is_eval_db_dir(src):
        # No adoption for a directory source: its arrays always carry a meta.
        if meta is None or meta.get("kind") != DIR_KIND:
            raise ValueError(f"eval arrays at {array_dir} carry no directory fingerprint for {src}. "
                             f"Rebuild: python/eval_arrays.py --eval-db {src} --out-dir {array_dir} --force")
        fp = dir_fingerprint(src)
        drift = [k for k in DIR_FP_KEYS if meta.get(k) != fp[k]]
        bk = meta.get("book") or {}
        if bk:
            bfp = book_fingerprint(Path(bk["book"]))
            drift += [f"book.{k}" for k in BOOK_FP_KEYS if bk.get(k) != bfp[k]]
        if drift:
            raise ValueError(f"eval arrays at {array_dir} are STALE: {src} changed ({', '.join(drift)}). "
                             f"Rebuild: python/eval_arrays.py --eval-db {src} --out-dir {array_dir} --force")
        return (f"verified against {src} ({fp['source_rows']:,} source rows, "
                f"{meta.get('n_rows', 0):,} entries; _DONE {fp['done']})")
    fp = source_fingerprint(src)

    if meta is None:
        n = int(np.load(hp, mmap_mode="r").shape[0])
        if n != fp["n_rows"]:
            raise ValueError(
                f"eval arrays at {array_dir} hold {n:,} entries but {src.name} "
                f"has {fp['n_rows']:,} rows — they were built from a different "
                f"DB. Rebuild: python/eval_arrays.py --force")
        if hp.stat().st_mtime_ns < fp["mtime_ns"]:
            raise ValueError(
                f"eval arrays at {array_dir} pre-date {src.name} "
                f"({fp['n_rows']:,} rows matched, but the DB is newer) — they "
                f"may be stale. Rebuild: python/eval_arrays.py --force")
        if adopt:
            _write_meta(array_dir, fp)
            return (f"adopted legacy arrays ({n:,} entries, row count and mtime "
                    f"consistent with {src.name}); fingerprint recorded")
        return f"unverified legacy arrays ({n:,} entries)"

    drift = [k for k in ("size", "mtime_ns", "n_rows") if meta.get(k) != fp[k]]
    if drift:
        raise ValueError(
            f"eval arrays at {array_dir} are STALE: {src.name} changed "
            f"({', '.join(drift)}). Built from {meta.get('n_rows', '?'):,} rows, "
            f"source now has {fp['n_rows']:,}. Rebuild:\n"
            f"    .venv/Scripts/python.exe python/eval_arrays.py --force")
    return f"verified against {src.name} ({fp['n_rows']:,} rows)"


def describe_eval_source(path: Path) -> dict:
    """What a repertoire's provenance records about its eval source: an arrays
    directory's meta and verify status, or a parquet / eval DB directory's
    fingerprint. Never raises: a failure is recorded as the status."""
    path = Path(path)
    try:
        if (path / META_NAME).is_file():
            try:
                status = verify_eval_arrays(path, adopt=False)
            except (FileNotFoundError, ValueError) as e:
                status = f"UNVERIFIED: {e}"
            return {"kind": "arrays", "path": str(path), "meta": read_meta(path), "status": status}
        return {"kind": "eval-db-dir" if is_eval_db_dir(path) else "parquet", "path": str(path),
                "fingerprint": source_fingerprint(path)}
    except Exception as e:                                         # noqa: BLE001
        return {"kind": "unknown", "path": str(path), "error": str(e)}


def build_eval_arrays(eval_db: Path = DEFAULT_EVAL_DB,
                      array_dir: Path = DEFAULT_ARRAY_DIR,
                      force: bool = False, **dir_opts) -> tuple[Path, Path]:
    """Materialise sorted (hash, cp) .npy pair. Idempotent; skip-gated.

    An eval DB directory goes to eval_arrays_build.build_from_db_dir (dir_opts:
    book, terminal, work, threads, mem, tmp, terminal_sample, keep_work,
    check_pool); a parquet is read here."""
    import polars as pl

    hp, cp = _paths(array_dir)
    if is_eval_db_dir(eval_db):
        if hp.exists() and cp.exists() and not force:
            try:
                verify_eval_arrays(array_dir, eval_db)
                return hp, cp
            except (FileNotFoundError, ValueError) as e:
                print(f"eval arrays: rebuilding — {e}", file=sys.stderr)
        from eval_arrays_build import build_from_db_dir
        build_from_db_dir(Path(eval_db), Path(array_dir), **dir_opts)
        return hp, cp
    if hp.exists() and cp.exists() and not force:
        # Skip gate is a VERIFICATION, not an existence check — see
        # verify_eval_arrays. Staleness rebuilds here rather than raising:
        # regenerating is exactly this function's job, and it is the one caller
        # that can fix the problem instead of reporting it.
        try:
            verify_eval_arrays(array_dir, eval_db)
            return hp, cp
        except (FileNotFoundError, ValueError) as e:
            print(f"eval arrays: rebuilding — {e}", file=sys.stderr)
    array_dir.mkdir(parents=True, exist_ok=True)

    df = pl.read_parquet(eval_db, columns=["position_hash", "eval_cp"])
    h = df["position_hash"].to_numpy().astype(np.int64, copy=False)
    e = df["eval_cp"].to_numpy()

    lo, hi = int(e.min()), int(e.max())
    if lo < -32767 or hi > 32767:
        raise ValueError(f"eval_cp range [{lo}, {hi}] does not fit int16 — "
                         f"storing it would silently wrap. Widen the dtype.")
    if lo == int(MISSING) or hi == int(MISSING):
        raise ValueError(f"eval_cp contains the MISSING sentinel {int(MISSING)}")

    order = np.argsort(h, kind="stable")
    h = np.ascontiguousarray(h[order])
    e = np.ascontiguousarray(e[order].astype(np.int16))

    # Duplicate hashes would make the lookup's answer depend on search position.
    dups = int(h.shape[0] - np.unique(h).shape[0])
    if dups:
        raise ValueError(f"{dups:,} duplicate position_hash values in {eval_db.name}; "
                         f"the lookup would be ambiguous. De-duplicate first.")

    # Atomic: a half-written array that still loads is the dangerous failure.
    # np.save appends '.npy' to any path that lacks it, so write through an open
    # handle — otherwise the temp lands at <name>.npy.tmp.npy and the rename
    # fails on a file that was never there.
    for path, arr in ((hp, h), (cp, e)):
        tmp = path.with_name(path.name + ".tmp")
        with open(tmp, "wb") as fh:
            np.save(fh, arr)
        tmp.replace(path)
    # LAST, like every other _SUCCESS-style sentinel here: the fingerprint must
    # only exist once both arrays are complete, or a crash between the two
    # renames would leave a half-built pair that verifies clean.
    _write_meta(array_dir, source_fingerprint(eval_db))
    return hp, cp


def open_eval_arrays(array_dir: Path = DEFAULT_ARRAY_DIR
                     ) -> tuple[np.ndarray, np.ndarray]:
    """mmap the pair read-only. Cheap enough to call per worker."""
    hp, cp = _paths(array_dir)
    if not (hp.exists() and cp.exists()):
        raise FileNotFoundError(
            f"eval arrays not built at {array_dir} — run build_eval_arrays() first")
    return np.load(hp, mmap_mode="r"), np.load(cp, mmap_mode="r")


def lookup_evals(keys: np.ndarray, mm_hash: np.ndarray,
                 mm_cp: np.ndarray) -> np.ndarray:
    """Batched hash -> eval_cp. Returns int16, MISSING where absent.

    Sorts the queries before searching: the binary searches then walk the big
    array roughly in order instead of jumping randomly across 3.2 GB.
    """
    keys = np.asarray(keys, dtype=np.int64)
    n = keys.shape[0]
    out = np.full(n, MISSING, dtype=np.int16)
    if n == 0 or mm_hash.shape[0] == 0:
        return out
    order = np.argsort(keys, kind="stable")
    sk = keys[order]
    idx = np.searchsorted(mm_hash, sk)
    np.clip(idx, 0, mm_hash.shape[0] - 1, out=idx)
    hit = np.asarray(mm_hash[idx]) == sk
    vals = np.where(hit, np.asarray(mm_cp[idx]), MISSING).astype(np.int16)
    out[order] = vals
    return out


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--eval-db", default=str(DEFAULT_EVAL_DB))
    ap.add_argument("--out-dir", default=str(DEFAULT_ARRAY_DIR))
    ap.add_argument("--force", action="store_true")
    # For an eval DB directory (see eval_arrays_build.py):
    ap.add_argument("--book", default=None, help="the explorer book (default: the DB's _build.meta.json)")
    ap.add_argument("--work-dir", default=None, help="temp folder (default: <out-dir>_work), ~60 GB")
    ap.add_argument("--no-terminal", action="store_true", help="do not add the book's checkmates")
    ap.add_argument("--terminal-sample", type=int, default=20_000)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--mem", default="8GB")
    ap.add_argument("--tmp-dir", default="D:/chess_duckdb_tmp")
    ap.add_argument("--keep-work", action="store_true")
    ap.add_argument("--check-pool", default=None, help="a pooled-stats parquet: report its eval coverage")
    a = ap.parse_args()
    if is_eval_db_dir(Path(a.eval_db)):
        build_eval_arrays(Path(a.eval_db), Path(a.out_dir), a.force,
                          book=Path(a.book) if a.book else None, terminal=not a.no_terminal,
                          work=Path(a.work_dir) if a.work_dir else None, threads=a.threads, mem=a.mem,
                          tmp=Path(a.tmp_dir), terminal_sample=a.terminal_sample, keep_work=a.keep_work,
                          check_pool=Path(a.check_pool) if a.check_pool else None)
        meta = read_meta(Path(a.out_dir)) or {}
        print(json.dumps({k: meta.get(k) for k in ("n_rows", "source_rows", "counts", "validation")}, indent=1))
        print(f"status: {verify_eval_arrays(Path(a.out_dir), Path(a.eval_db))}")
        return
    hp, cp = build_eval_arrays(Path(a.eval_db), Path(a.out_dir), a.force)
    h, e = open_eval_arrays(Path(a.out_dir))
    print(f"hashes {h.shape[0]:,}  ({hp.stat().st_size/1e9:.2f} GB)")
    print(f"evals  {e.shape[0]:,}  ({cp.stat().st_size/1e9:.2f} GB)")
    print(f"sorted: {bool(np.all(h[:-1] <= h[1:]))}")
    print(f"cp range: [{int(e.min())}, {int(e.max())}]")
    print(f"status: {verify_eval_arrays(Path(a.out_dir), Path(a.eval_db))}")


if __name__ == "__main__":
    sys.exit(main())
