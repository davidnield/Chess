"""Eval lookup with ChessDB step 1 (docs/move-menus-plan-2026-11-21.md, David's OK 2026-11-21).

A NEW lookup generation built from the published one (eval_lookup128.py) and the ChessDB move-menu
import (scratch/chessdb_import/import_menus.py). Same file layout and row dtype, so every consumer
reads it unchanged; the published lookup is never modified. Selecting it is a separate, default-off
decision tested through the sweep.

Step-1 policy, per position (full 128-bit key):
  - no score in the base lookup, ChessDB scored it            -> ChessDB         (source 4)
  - base score is pre-2021 fishnet only (tier classical/early)
    and ChessDB scored it                                       -> ChessDB         (source 5)
  - everything else (cloud, 2021+ fishnet, terminal mates)      -> unchanged
ChessDB's best score is side-to-move; it becomes White's view, then the eval DB's scale: cp clamped
to +-2000, a mate or tablebase-win claim (|score| >= 15000) -> sign * 2000. Terminal mates (source 3)
are never replaced.

Usage:
  python eval_lookup_cdb.py --base D:/chess/eval-lookup-p51-20261115 --eval-db D:/chess/eval-db-p51-20261115
      --menus E:/chess/chessdb-menus-20260702 --out D:/chess/eval-lookup-p51-cdb1-20261121 [--buckets ...]
A partial --buckets build is a test artifact: lookup.json is written only when all 512 are done.
--available builds every bucket whose ChessDB import is finished (rerun it as the import advances).
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pyarrow.compute as pc
import pyarrow.parquet as pq

from eval_lookup128 import FISHNET, ROW, SCHEMA, TERMINAL, keys
from month_archive import read_json, sha, write_json

CHESSDB_GAP, CHESSDB_OVER_LEGACY = 4, 5
VARIANT = "chessdb-step1-gap-and-legacy-fishnet-v1"
DECISIVE, CAP = 15000, 2000


def chessdb_white_cp(best: np.ndarray, white_to_move: np.ndarray) -> np.ndarray:
    """ChessDB best score (side to move, backprop scale) -> White's view on the eval DB's scale."""
    s = best.astype(np.int32)
    s = np.where(white_to_move, s, -s)
    s = np.where(np.abs(s) >= DECISIVE, np.sign(s) * CAP, np.clip(s, -CAP, CAP))
    return s.astype(np.int16)


def legacy_keys(db_bucket: Path) -> np.ndarray:
    t = pq.read_table(db_bucket, columns=["position_hash", "position_hash_hi", "source", "fishnet_tier"])
    legacy = pc.and_(pc.equal(t["source"], "fishnet"),
                     pc.is_in(t["fishnet_tier"], value_set=pc.cast(np.array(["classical", "early"]), "string")))
    legacy = pc.fill_null(legacy, False).to_numpy(zero_copy_only=False)
    k = keys(t["position_hash"].to_numpy()[legacy], t["position_hash_hi"].to_numpy()[legacy])
    k = k[np.lexsort((k["hi"], k["lo"]))]
    return k


def strictly_sorted(k: np.ndarray) -> bool:
    """Rows in strictly increasing (lo, hi) order (numpy cannot compare structured rows with >)."""
    lo, hi = k["lo"], k["hi"]
    return bool(((lo[1:] > lo[:-1]) | ((lo[1:] == lo[:-1]) & (hi[1:] > hi[:-1]))).all())


def member(sorted_keys: np.ndarray, q: np.ndarray) -> np.ndarray:
    if len(sorted_keys) == 0:
        return np.zeros(len(q), bool)
    i = np.searchsorted(sorted_keys, q)
    ok = i < len(sorted_keys)
    ok[ok] = sorted_keys[i[ok]] == q[ok]
    return ok


def make_bucket(base_npy: Path, db_bucket: Path, menus_bucket: Path, b: int) -> tuple[np.ndarray, dict]:
    base = np.load(base_npy, allow_pickle=False)
    if base.dtype != ROW:
        raise ValueError(f"unexpected layout in {base_npy}")
    bk = base[["lo", "hi"]]
    if not strictly_sorted(base):
        raise ValueError(f"{base_npy} is not sorted by unique full key")
    t = pq.read_table(menus_bucket, columns=["lo", "hi", "white_to_move", "found", "best_score"])
    # found with no moves (e.g. checkmate or stalemate) carries no score: only scored rows count
    f = t.filter(pc.and_(t["found"], pc.is_valid(t["best_score"])))
    lo, hi = f["lo"].to_numpy(), f["hi"].to_numpy()
    if ((lo % 512) != b).any():
        raise ValueError(f"misplaced ChessDB row in {menus_bucket}")
    best = f["best_score"].to_numpy()
    cq = keys(lo, hi)
    cp = chessdb_white_cp(best, f["white_to_move"].to_numpy(zero_copy_only=False))
    order = np.lexsort((cq["hi"], cq["lo"]))
    cq, cp = cq[order], cp[order]
    if not strictly_sorted(cq):
        raise ValueError(f"duplicate key in {menus_bucket}")
    # positions already in the base lookup
    pos = np.searchsorted(bk, cq)
    inbase = pos < len(base)
    inbase[inbase] = bk[pos[inbase]] == cq[inbase]
    out = base.copy()
    legacy = member(legacy_keys(db_bucket), cq)
    # the base lookup already overrode some legacy rows (terminal mates): those stay
    replace = inbase & legacy
    replace[replace] = out["source"][pos[replace]] == FISHNET
    rp = pos[replace]
    out["cp"][rp], out["source"][rp] = cp[replace], CHESSDB_OVER_LEGACY
    add = np.empty(np.count_nonzero(~inbase), dtype=ROW)
    add["lo"], add["hi"], add["cp"], add["source"] = cq["lo"][~inbase], cq["hi"][~inbase], cp[~inbase], CHESSDB_GAP
    out = np.concatenate([out, add])
    out.sort(order=["lo", "hi"])
    if not strictly_sorted(out):
        raise ValueError(f"bucket {b}: duplicate full key after the merge")
    stats = dict(base_rows=len(base), chessdb_found=len(cq), gap_added=int(len(add)),
                 legacy_replaced=int(replace.sum()), rows=len(out),
                 untouched_terminal=int((out["source"] == TERMINAL).sum()))
    return out, stats


def build(base: Path, db: Path, menus: Path, out: Path, buckets: list[int] | None = None) -> dict:
    """Compatibility entry point; v2 always verifies input bytes on resume."""
    from chessdb_lookup_run import run
    return run(base, db, menus, out, buckets, workers=1)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    for n in ("base", "eval-db", "menus", "out"):
        ap.add_argument("--" + n, required=True, type=Path)
    ap.add_argument("--buckets", type=int, nargs="*")
    ap.add_argument("--available", action="store_true", help="every bucket whose ChessDB import is done")
    a = ap.parse_args()
    buckets = a.buckets
    if a.available:
        buckets = [b for b in range(512) if (a.menus / "_done" / f"bkt{b:03d}.json").exists()]
    r = build(a.base, a.eval_db, a.menus, a.out, buckets)
    tot = {k: sum(v[k] for v in r.values()) for k in ("base_rows", "gap_added", "legacy_replaced", "rows")}
    print(tot)
