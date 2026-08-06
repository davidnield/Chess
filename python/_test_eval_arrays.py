"""The mmap'd eval lookup returns what the parquet says, and is fast enough.

Everything in the fused extract keys off this primitive: the winpos crossing
detection and the other-moves bucket's aggregate eval both come from it. A wrong
answer here is not a crash, it is a repertoire built on the wrong evaluations —
so it is checked against the source of truth rather than against itself.

Also measures throughput, because the design claim ("sort the batch first so the
binary searches sweep instead of jumping") is the reason per-ply lookups are
affordable at all, and an unmeasured performance claim is how the last plan went
wrong.

Usage:  python _test_eval_arrays.py [--sample N]
"""
from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from eval_arrays import (DEFAULT_ARRAY_DIR, DEFAULT_EVAL_DB, META_NAME, MISSING,
                         build_eval_arrays, lookup_evals, open_eval_arrays,
                         verify_eval_arrays)

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def raises(fn, exc) -> bool:
    try:
        fn()
    except exc:
        return True
    except Exception:                                          # noqa: BLE001
        return False
    return False


def check_staleness() -> None:
    """The arrays are a DERIVED copy — rebuild the eval DB and they go stale.

    Nothing about that is visible at read time: a stale array is the right shape,
    sorted, and answers every query. It just answers with the previous DB's
    evaluations, in both the winpos crossings and the other-moves bucket. So the
    guard is tested here on throwaway arrays rather than trusted.
    """
    tmp = Path(tempfile.mkdtemp(prefix="eval_arrays_stale_"))
    try:
        db = tmp / "src.parquet"
        adir = tmp / "arrays"
        pl.DataFrame({"position_hash": [5, 1, 9, 3],
                      "eval_cp": [10, -20, 30, -40]}).write_parquet(db)
        build_eval_arrays(db, adir)
        check((adir / META_NAME).exists(),
              "build writes a fingerprint sidecar")
        check("verified against" in verify_eval_arrays(adir, db),
              "freshly built arrays verify against their source")

        # Rebuild the source with different content. This is exactly the
        # build_fishnet_eval_db.py path that has already happened once.
        pl.DataFrame({"position_hash": [5, 1, 9, 3, 7],
                      "eval_cp": [10, -20, 30, -40, 50]}).write_parquet(db)
        check(raises(lambda: verify_eval_arrays(adir, db), ValueError),
              "a CHANGED source is detected as stale (this is the bug being fixed)")

        # build_eval_arrays is the one caller that can fix staleness, so it
        # rebuilds instead of raising.
        build_eval_arrays(db, adir)
        check("verified against" in verify_eval_arrays(adir, db),
              "build_eval_arrays rebuilds a stale pair rather than raising")
        check(int(np.load(adir / "eval_hash.npy", mmap_mode="r").shape[0]) == 5,
              "the rebuilt arrays carry the new row count")

        # Legacy arrays (built before the sidecar existed) must not force a
        # needless 400M-row rebuild when they are demonstrably current.
        (adir / META_NAME).unlink()
        check("adopted legacy arrays" in verify_eval_arrays(adir, db),
              "meta-less arrays matching row count + mtime are adopted")
        check((adir / META_NAME).exists(),
              "adoption records the fingerprint so later checks are exact")

        # ...but a meta-less pair whose row count disagrees is a hard failure,
        # which is the case adoption must never wave through.
        (adir / META_NAME).unlink()
        pl.DataFrame({"position_hash": [5, 1], "eval_cp": [10, -20]}).write_parquet(db)
        check(raises(lambda: verify_eval_arrays(adir, db), ValueError),
              "meta-less arrays with a MISMATCHED row count are rejected")

        check(raises(lambda: verify_eval_arrays(tmp / "nope", db), FileNotFoundError),
              "absent arrays raise FileNotFoundError with a build hint")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    # 1.25M ~= one read batch (50K games x ~25 plies), the size the extract will
    # actually issue. Measuring on a small batch understates the amortisation.
    ap.add_argument("--sample", type=int, default=1_250_000)
    a = ap.parse_args()

    # Runs on throwaway arrays, so it works with no E: data present.
    print("Staleness guard:")
    check_staleness()

    if not (DEFAULT_ARRAY_DIR / "eval_hash.npy").exists():
        print(f"\n  SKIP: eval arrays not built at {DEFAULT_ARRAY_DIR}")
        n_fail = sum(1 for ok, _ in _checks if not ok)
        print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} "
              f"({len(_checks)} checks — real arrays unavailable)")
        sys.exit(0 if n_fail == 0 else 1)
    print()

    h, e = open_eval_arrays()
    print(f"Arrays: {h.shape[0]:,} entries\n")

    check(bool(np.all(np.asarray(h[:1_000_000])[:-1] <= np.asarray(h[:1_000_000])[1:])),
          "hash array is sorted (prefix check — searchsorted requires it)")

    # Ground truth straight from the parquet, not from the arrays under test.
    src = pl.read_parquet(DEFAULT_EVAL_DB, columns=["position_hash", "eval_cp"]).head(a.sample)
    keys = src["position_hash"].to_numpy().astype(np.int64)
    want = src["eval_cp"].to_numpy().astype(np.int16)

    got = lookup_evals(keys, h, e)
    bad = int((got != want).sum())
    check(bad == 0, f"present keys return the parquet's eval_cp "
                    f"({len(keys):,} sampled, {bad} wrong)")
    if bad:
        i = int(np.nonzero(got != want)[0][0])
        print(f"        first: hash={keys[i]} got={got[i]} want={want[i]}")

    # Absent keys must report MISSING, not a neighbour's evaluation — the failure
    # mode that would quietly assign a random position's score.
    rng = np.random.default_rng(20260804)
    probe = rng.integers(np.iinfo(np.int64).min, np.iinfo(np.int64).max,
                         size=20_000, dtype=np.int64)
    absent = probe[~np.isin(probe, keys)]
    ga = lookup_evals(absent, h, e)
    # A random int64 landing in a 400M-entry table is ~2e-11 likely; any hit is
    # real, so compare against the table rather than asserting all-missing.
    idx = np.clip(np.searchsorted(h, absent), 0, h.shape[0] - 1)
    truly_absent = np.asarray(h[idx]) != absent
    check(bool(np.all(ga[truly_absent] == MISSING)),
          f"absent keys return MISSING, never a neighbour "
          f"({int(truly_absent.sum()):,} probed)")

    check(int(lookup_evals(np.array([], dtype=np.int64), h, e).shape[0]) == 0,
          "empty query returns empty (no crash on an all-filtered batch)")

    # Order independence: the sort/scatter must not permute results.
    perm = rng.permutation(len(keys))
    gp = lookup_evals(keys[perm], h, e)
    check(bool(np.array_equal(gp, got[perm])),
          "results follow the query order, not the sorted order")

    # Throughput. The bar that matters is not a round lookups/s number — it is
    # whether the lookups are cheap RELATIVE to the replay they ride along with.
    # A full 2013-2026 rebuild is ~2.6B ply-lookups against a projected ~62 h
    # extract on 6 workers = ~370 core-hours; the lookups must disappear into
    # that, not merely be "fast".
    PLY_LOOKUPS = 2.6e9
    EXTRACT_CORE_HOURS = 370.0
    BUDGET_FRAC = 0.05

    batch = keys[:min(len(keys), 1_250_000)]
    lookup_evals(batch[:1000], h, e)                      # warm the pages
    t0 = time.time()
    lookup_evals(batch, h, e)
    dt = time.time() - t0
    rate = len(batch) / dt
    core_hours = PLY_LOOKUPS / rate / 3600
    frac = core_hours / EXTRACT_CORE_HOURS
    print(f"\n  info  {len(batch):,} lookups in {dt:.2f}s = {rate/1e6:.2f}M/s")
    print(f"  info  {PLY_LOOKUPS/1e9:.1f}B ply-lookups -> {core_hours:.1f} core-hours "
          f"= {100*frac:.1f}% of a {EXTRACT_CORE_HOURS:.0f} core-hour extract")
    check(frac < BUDGET_FRAC,
          f"lookup cost under {100*BUDGET_FRAC:.0f}% of the extract "
          f"({100*frac:.1f}%, {rate/1e6:.2f}M/s)")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
