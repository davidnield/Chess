"""A sorted, memory-mapped index of an explorer table dir's move counts, for the referee and the
samplers (convertibility_eval.Counts): the same counts as Counts' DuckDB query, without rescanning
the table's parquet files for every batch of positions (profile 2026-11-12: 20% of a stop-table
sample went to those scans, about 2.2 s a batch).

What it holds, per position (parent_hash): every move's game count summed over the table's rows
and folds, keyed by the move with its check marks stripped -- exactly what Counts.get builds
(SUM(total) per (parent, move_san), then summed per stripped move; integer counts, so the
summation order cannot change a value). Unfolded only: bootstrap replicates (Counts._rep) still
read the parquet files. --fold-half H indexes only the games in folds with fold % 2 == H
(Counts' fold_half, for sampling tables on one half of the games and scoring on the other);
it lives beside the whole table's index, in _move_index_h<H>.

Layout: <dir>/_move_index/ with keys.npy (sorted distinct parent hashes), ptr.npy (CSR offsets),
move.npy (int32 codes into moves.json), n.npy (int64 counts), and meta.json, written last. The
meta records convertibility_eval.table_manifest(dir) (every data file's path, size and mtime);
an index whose manifest no longer matches the table is ignored, and Counts falls back to DuckDB.

    .venv/Scripts/python.exe python/move_index.py <table dir> [<table dir> ...] [--mem 16GB] [--fold-half 0|1]
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np

INDEX = "_move_index"
FILES = ("keys.npy", "ptr.npy", "move.npy", "n.npy", "moves.json")


def index_dir(root: str, half: int | None = None) -> Path:
    return Path(root) / (INDEX if half is None else f"{INDEX}_h{int(half)}")


def manifest(root: str) -> list:
    import convertibility_eval as cv
    return cv.table_manifest(root)


class MoveIndex:
    """An index opened read-only (memory-mapped)."""

    def __init__(self, root: str, half: int | None = None):
        d = index_dir(root, half)
        self.root, self.half = root, half
        self.keys = np.load(d / "keys.npy", mmap_mode="r")
        self.ptr = np.load(d / "ptr.npy", mmap_mode="r")
        self.move = np.load(d / "move.npy", mmap_mode="r")
        self.n = np.load(d / "n.npy", mmap_mode="r")
        self.moves = json.loads((d / "moves.json").read_text(encoding="utf-8"))

    @staticmethod
    def open(root: str, half: int | None = None) -> "MoveIndex | None":
        """The index (of one fold half, if given) if it exists and matches the table's data
        files, else None."""
        d = index_dir(root, half)
        mp = d / "meta.json"
        if not mp.exists() or any(not (d / f).exists() for f in FILES):
            return None
        meta = json.loads(mp.read_text(encoding="utf-8"))
        if meta.get("manifest") != manifest(root) or meta.get("fold_half") != half:
            return None
        return MoveIndex(root, half)

    def lookup(self, hashes: np.ndarray):
        """For each hash: (start, stop) into move/n, or (0, 0) if the position is absent."""
        hashes = np.asarray(hashes, dtype=np.int64)
        i = np.searchsorted(self.keys, hashes)
        ic = np.minimum(i, max(len(self.keys) - 1, 0))
        hit = (self.keys[ic] == hashes) if len(self.keys) else np.zeros(len(hashes), bool)
        lo = np.where(hit, np.asarray(self.ptr[ic]), 0)
        hi = np.where(hit, np.asarray(self.ptr[np.minimum(ic + 1, len(self.ptr) - 1)]), 0)
        return lo, hi


def build(root: str, mem: str = "16GB", threads: int = 4, tmp: str | None = None,
          log=print, half: int | None = None) -> Path:
    """Write <root>/_move_index, or _move_index_h<half> for one fold half (skipped when a
    matching one exists)."""
    import duckdb
    root = str(root)
    if half is not None and half not in (0, 1):
        raise SystemExit("--fold-half is 0 or 1")
    where = "" if half is None else f"WHERE fold % 2 = {int(half)}"
    if MoveIndex.open(root, half) is not None:
        log(f"{root}: index is current")
        return index_dir(root, half)
    t0 = time.time()
    man = manifest(root)
    if not man:
        raise SystemExit(f"{root}: no data files")
    glob = f"{root}/month=*/bkt=*/*.parquet".replace("\\", "/")
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{mem}'; SET threads={threads}; SET preserve_insertion_order=false")
    if tmp:
        con.execute(f"SET temp_directory='{Path(tmp).as_posix()}'")
    moves = [r[0] for r in con.execute(
        f"SELECT DISTINCT rtrim(move_san, '+#') FROM read_parquet('{glob}') {where} ORDER BY 1").fetchall()]
    con.execute("CREATE TEMP TABLE mv (m VARCHAR, c INTEGER)")
    con.executemany("INSERT INTO mv VALUES (?, ?)", [(m, i) for i, m in enumerate(moves)])
    from duck_compat import duck_arrow
    tab = duck_arrow(con, f"""
        SELECT s.parent_hash AS h, mv.c AS c, SUM(s.total)::BIGINT AS n
        FROM read_parquet('{glob}') s JOIN mv ON mv.m = rtrim(s.move_san, '+#')
        {where} GROUP BY 1, 2 ORDER BY 1, 2""")
    con.close()
    h = tab.column("h").to_numpy()
    c = tab.column("c").to_numpy().astype(np.int32)
    n = tab.column("n").to_numpy().astype(np.int64)
    del tab
    starts = np.flatnonzero(np.concatenate([[True], h[1:] != h[:-1]])) if len(h) else np.zeros(0, np.int64)
    keys = h[starts].astype(np.int64)
    ptr = np.concatenate([starts, [len(h)]]).astype(np.int64)
    d = index_dir(root, half)
    d.mkdir(exist_ok=True)
    (d / "meta.json").unlink(missing_ok=True)        # written last: an index exists only if complete
    for name, arr in (("keys", keys), ("ptr", ptr), ("move", c), ("n", n)):
        tmpf = d / f"{name}.tmp.npy"
        np.save(tmpf, arr)
        tmpf.replace(d / f"{name}.npy")
    (d / "moves.json").write_text(json.dumps(moves), encoding="utf-8")
    meta = {"manifest": man, "fold_half": half, "positions": int(len(keys)), "rows": int(len(h)),
            "moves": len(moves), "built": time.strftime("%Y-%m-%d %H:%M:%S"),
            "seconds": round(time.time() - t0, 1)}
    tmpm = d / "meta.json.tmp"
    tmpm.write_text(json.dumps(meta, indent=1), encoding="utf-8")
    tmpm.replace(d / "meta.json")
    log(f"{root}: {len(keys):,} positions, {len(h):,} moves ({meta['seconds']:,}s)")
    return d


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("roots", nargs="+")
    ap.add_argument("--mem", default="16GB")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--tmp-dir", default=None)
    ap.add_argument("--fold-half", type=int, choices=[0, 1], default=None,
                    help="index only the games in folds with fold %% 2 == H")
    a = ap.parse_args()
    for r in a.roots:
        build(r, a.mem, a.threads, a.tmp_dir, half=a.fold_half)
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent))
    sys.exit(main())
