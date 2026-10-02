"""Stage 3's eval loader: a parquet --eval-db and the eval arrays built from it are interchangeable.

load_evals takes either the legacy (position_hash, eval_cp) parquet or an eval-arrays directory
(python/eval_arrays.py; e.g. the arrays of D:/chess/eval_full). Built from the SAME source, the two
must be indistinguishable to run_backwards_induction:
  1. eval_lookup: same keys, values bit-identical (one Polars sigmoid, a table computed through it);
  2. engine augmentation: the same (hit, expected score) for present and absent hashes, from the
     in-RAM sorted arrays vs the memory-mapped ones;
  3. the augmentation scenario of _test_stage3_augment.py picks the same rescue either way;
  4. an eval DB directory is refused (build its arrays first), a missing path only WARNs, and arrays
     whose cp range reaches --eval-mate-cp are refused (the arrays path cannot drop those rows).

Real data (skipped when absent): E:/chess/eval_arrays was built from E:/chess/unified_eval_db.parquet,
so on a sample of a pooled-stats DAG the two loaders must agree bit for bit.

Usage:  python _test_stage3_load_evals.py
"""
from __future__ import annotations

import contextlib
import io
import shutil
import sys
import tempfile
from pathlib import Path

import chess
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from eval_arrays import build_eval_arrays  # noqa: E402
from stage3_backwards_induction import load_evals, run_backwards_induction, zobrist_int64  # noqa: E402

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def child(parent: chess.Board, san: str) -> int:
    b = parent.copy()
    b.push_san(san)
    return zobrist_int64(b)


def quiet(fn, *a, **k):
    with contextlib.redirect_stdout(io.StringIO()) as out:
        r = fn(*a, **k)
    return r, out.getvalue()


def aug_pairs(fh, fe, keys: np.ndarray) -> list:
    """What augmented_candidates sees: (hit, float(es)) per key."""
    idx = np.clip(np.searchsorted(fh, keys), 0, len(fh) - 1)
    hit = fh[idx] == keys
    return [(bool(ok), float(fe[i]) if ok else None) for i, ok in zip(idx, hit)]


def synthetic(tmp: Path) -> None:
    start = chess.Board()
    sh = zobrist_int64(start)
    bad, good, meh = child(start, "h3"), child(start, "e4"), child(start, "d4")
    # Int32 eval_cp, like the old DB; cp values cover both signs, the +-2000 cap, and odd values.
    db = tmp / "db.parquet"
    pl.DataFrame({"position_hash": [bad, good, meh, sh, 12345, -98765],
                  "eval_cp": pl.Series([-600, 80, -230, 33, 2000, -1999], dtype=pl.Int32)}).write_parquet(db)
    pool = tmp / "pool.parquet"
    pl.DataFrame({"parent_hash": [sh], "child_hash": [bad]}).write_parquet(pool)
    adir = tmp / "arrays"
    quiet(build_eval_arrays, db, adir)

    (lk_p, fh_p, fe_p), _ = quiet(load_evals, db, pool, eval_mate_cp=3000, augment=True, eval_weight=1.0)
    (lk_a, fh_a, fe_a), log = quiet(load_evals, adir, pool, eval_mate_cp=3000, augment=True, eval_weight=1.0)
    check(set(lk_p) == set(lk_a) == {sh, bad}, f"eval_lookup holds the DAG's positions either way ({len(lk_a)})")
    check(all(float.hex(lk_p[k]) == float.hex(lk_a[k]) for k in lk_p),
          "eval_lookup values are bit-identical (parquet vs arrays)")
    keys = np.array([bad, good, meh, sh, 12345, -98765, 7, -7], dtype=np.int64)
    check(aug_pairs(fh_p, fe_p, keys) == aug_pairs(fh_a, fe_a, keys),
          "augmentation sees the same (hit, es) for present and absent hashes")
    check(isinstance(fh_a, np.memmap) and "memory-mapped" in log, "the arrays path augments from the memmap in place")

    # The augment scenario: only h3 is recorded and its child is engine-lost; e4 (best) and d4 are
    # unplayed rescues known only to the full DB.
    edges = [{"parent_hash": sh, "child_hash": bad, "move_san": "h3", "parent_epd": start.epd(),
              "white_score_avg": 0.50, "total": 1000, "draws": 0}]
    common = dict(prior_strength=0.0, min_move_games=0, eval_weight=1.0, require_eval=True,
                  robustness_floor=0.25, gate_metric="eval", augment_engine=True)
    (_, bm_p, *_), _ = quiet(run_backwards_induction, edges, "white", eval_lookup=lk_p,
                             full_eval_hashes=fh_p, full_eval_es=fe_p, **common)
    (_, bm_a, *_), _ = quiet(run_backwards_induction, edges, "white", eval_lookup=lk_a,
                             full_eval_hashes=fh_a, full_eval_es=fe_a, **common)
    check(bm_p.get(sh) == bm_a.get(sh) == "e4",
          f"the rescue scenario picks e4 either way (parquet {bm_p.get(sh)!r}, arrays {bm_a.get(sh)!r})")

    # Guards.
    (lk_m, fh_m, _), log = quiet(load_evals, tmp / "nope.parquet", pool, eval_mate_cp=3000, augment=True,
                                 eval_weight=1.0)
    check(lk_m == {} and fh_m is None and "WARNING" in log, "a missing --eval-db WARNs and loads nothing")
    ddir = tmp / "evaldb"
    ddir.mkdir()
    pl.DataFrame({"bucket": [0], "rows": [0]}).write_parquet(ddir / "_manifest.parquet")
    try:
        quiet(load_evals, ddir, pool, eval_mate_cp=3000, augment=True, eval_weight=1.0)
        refused = False
    except SystemExit as e:
        refused = "eval DB directory" in str(e)
    check(refused, "an eval DB directory is refused with the build command")
    try:
        quiet(load_evals, adir, pool, eval_mate_cp=1500, augment=True, eval_weight=1.0)
        refused = False
    except SystemExit as e:
        refused = "--eval-mate-cp" in str(e)
    check(refused, "arrays reaching --eval-mate-cp are refused")
    (lk_f, _, _), _ = quiet(load_evals, db, pool, eval_mate_cp=500, augment=False, eval_weight=1.0)
    check(set(lk_f) == {sh}, "the parquet path still drops |cp| >= --eval-mate-cp (legacy behaviour)")


def real_data() -> None:
    db, adir = Path("E:/chess/unified_eval_db.parquet"), Path("E:/chess/eval_arrays")
    pool = Path("E:/chess/position-stats/position_stats_pooled_ge1800_2013_2026_brc.parquet")
    if not (db.exists() and (adir / "eval_hash.npy").exists() and pool.exists()):
        print("\n  SKIP: real old DB / arrays / pool not all present")
        return
    tmp = Path(tempfile.mkdtemp(prefix="s3_load_real_"))
    try:
        # A 200K-edge sample of the canonical pool keeps this to seconds.
        sample = tmp / "pool_sample.parquet"
        pl.scan_parquet(pool).select("parent_hash", "child_hash").head(200_000).collect().write_parquet(sample)
        (lk_p, _, _), _ = quiet(load_evals, db, sample, eval_mate_cp=3000, augment=False, eval_weight=1.0)
        (lk_a, _, _), _ = quiet(load_evals, adir, sample, eval_mate_cp=3000, augment=False, eval_weight=1.0)
        same = set(lk_p) == set(lk_a) and all(float.hex(lk_p[k]) == float.hex(lk_a[k]) for k in lk_p)
        check(same, f"real data: unified_eval_db vs its arrays on a 200K-edge pool sample ({len(lk_p):,} evals)")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="s3_load_evals_"))
    try:
        print("Synthetic:")
        synthetic(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    real_data()
    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
