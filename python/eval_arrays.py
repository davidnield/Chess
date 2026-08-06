"""Shared, memory-mapped (position_hash -> eval_cp) arrays.

The unified eval DB is 400M rows. Stage 3 loads it into process memory as sorted
numpy arrays (~4.8 GB) and that is fine for one process — but the fused extract
needs the same lookup inside every worker, and 6 workers x 4.8 GB is not.

So: materialise it ONCE as two .npy files sorted by hash, then np.load(mmap_mode)
in each worker. Windows backs mmaps of the same file with the same page-cache
pages, so N workers share one resident copy instead of N private ones.

eval_cp is stored int16. build_lichess_eval_db.py caps decisive evals at +-2000,
which fits, and the cast is checked at build time rather than assumed — a silent
wrap here would turn a won position into a lost one.

Lookups are BATCHED on purpose. A binary search over a 3.2 GB array is ~28 random
accesses; doing that per ply during a replay is cache-hostile. Sorting the query
batch first turns the searches into a mostly-sequential sweep, which is why
lookup_evals sorts, searches, then scatters back.

Usage:
    build_eval_arrays(Path("E:/chess/unified_eval_db.parquet"))   # one-time
    h, e = open_eval_arrays()
    cp = lookup_evals(np.array([...], dtype=np.int64), h, e)      # MISSING where absent
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

DEFAULT_EVAL_DB = Path("E:/chess/unified_eval_db.parquet")
DEFAULT_ARRAY_DIR = Path("E:/chess/eval_arrays")

# Sentinel for "this position is not in the eval DB". Outside any real eval
# (build_lichess_eval_db.py caps at +-2000) and outside int16's usable range for
# real data, so it can never collide with a genuine evaluation.
MISSING = np.int16(-32768)


def _paths(array_dir: Path) -> tuple[Path, Path]:
    return array_dir / "eval_hash.npy", array_dir / "eval_cp.npy"


def build_eval_arrays(eval_db: Path = DEFAULT_EVAL_DB,
                      array_dir: Path = DEFAULT_ARRAY_DIR,
                      force: bool = False) -> tuple[Path, Path]:
    """Materialise sorted (hash, cp) .npy pair. Idempotent; skip-gated."""
    import polars as pl

    hp, cp = _paths(array_dir)
    if hp.exists() and cp.exists() and not force:
        return hp, cp
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
    a = ap.parse_args()
    hp, cp = build_eval_arrays(Path(a.eval_db), Path(a.out_dir), a.force)
    h, e = open_eval_arrays(Path(a.out_dir))
    print(f"hashes {h.shape[0]:,}  ({hp.stat().st_size/1e9:.2f} GB)")
    print(f"evals  {e.shape[0]:,}  ({cp.stat().st_size/1e9:.2f} GB)")
    print(f"sorted: {bool(np.all(h[:-1] <= h[1:]))}")
    print(f"cp range: [{int(e.min())}, {int(e.max())}]")


if __name__ == "__main__":
    sys.exit(main())
