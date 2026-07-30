"""V1 — the linchpin: the optimized extract must produce IDENTICAL partials.

build_pooled_stats.extract_file gained two speed optimizations (2026-07): an
IncrementalZobrist replacing the per-ply 64-square hash rescan, and a per-batch
hash -> EPD memo replacing a board.epd() call per ply. Both are supposed to be
pure speedups with zero observable effect, so the bar is exact equality — not
"close", not "same row count".

This runs the SAME real source file through extract_file twice, with
optimize=False (the original code path) and optimize=True, and asserts the ps
and crush partials are frame-equal on every column at tolerance 0.

Why this one test covers both optimizations end-to-end:
  - a wrong hash delta changes parent_hash, which regroups every aggregate;
  - a wrong EPD memo changes parent_epd. _agg_ps keeps
    pl.col("parent_epd").first() per (parent_hash, move_san) group, so if
    hash -> EPD were ever NOT a function the memoized run would keep a
    different representative EPD than the unmemoized one and this fails.

Usage:  python _test_extract_equivalence.py [--games N] [--file PATH]
"""
from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
import time
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from build_pooled_stats import extract_file

SRC_DIRS = [
    Path("D:/data/chess/standard-chess-games-compressed/year=2019/month=3/event=Blitz"),
    Path("D:/data/chess/standard-chess-games-compressed/year=2023/month=1/event=Blitz"),
]

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def compare(a: Path, b: Path, keys: list[str], kind: str) -> None:
    da, db = pl.read_parquet(a), pl.read_parquet(b)
    if da.columns != db.columns:
        check(False, f"{kind}: column mismatch {da.columns} vs {db.columns}")
        return
    if da.height != db.height:
        check(False, f"{kind}: row count {da.height:,} vs {db.height:,}")
        return
    da = da.sort(keys)
    db = db.sort(keys)
    if da.equals(db):
        check(True, f"{kind}: {da.height:,} rows identical across all {len(da.columns)} columns")
        return
    # Localize the first differing column so a failure is actionable.
    bad = [c for c in da.columns if not da[c].equals(db[c])]
    check(False, f"{kind}: {len(bad)} column(s) differ: {bad}")
    for c in bad[:2]:
        diff = da.with_row_index().filter(da[c] != db[c])
        if diff.height:
            i = diff["index"][0]
            print(f"    first {c} diff at row {i}: "
                  f"baseline={da[c][i]!r}  optimized={db[c][i]!r}")
            print(f"    row (baseline): {da.row(i, named=True)}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", type=int, default=8000,
                    help="Games per run; both runs see the same prefix. The default "
                         "keeps run_tests.py fast — rerun wider with --games 25000 "
                         "(verified identical there) before trusting a change to the "
                         "replay path.")
    ap.add_argument("--file", default=None, help="Source parquet to replay.")
    args = ap.parse_args()

    if args.file:
        src = Path(args.file)
    else:
        src = None
        for d in SRC_DIRS:
            if d.is_dir():
                files = sorted(d.glob("*.parquet"))
                if files:
                    src = files[0]
                    break
    if src is None or not src.exists():
        print(f"  SKIP: no source parquet found under {[str(d) for d in SRC_DIRS]}")
        print("\nALL PASS (0 checks — source data unavailable)")
        sys.exit(0)

    print(f"Source: {src}  ({src.stat().st_size/1e6:.0f} MB)")
    print(f"Games:  {args.games:,} per run\n")

    tmp = Path(tempfile.mkdtemp(prefix="extract_equiv_"))
    try:
        out = {}
        for tag, opt in (("baseline", False), ("optimized", True)):
            ps = tmp / f"{tag}.ps.parquet"
            cr = tmp / f"{tag}.crush.parquet"
            t0 = time.time()
            r = extract_file(src, ps, cr, min_elo=1800, max_ply=30, tiers=None,
                             limit_games=args.games, optimize=opt)
            el = time.time() - t0
            out[tag] = (ps, cr, el, r)
            print(f"  {tag:<10} {r['kept']:,}/{r['games']:,} kept  "
                  f"ps={r['ps_rows']:,} crush={r['crush_rows']:,}  {el:.1f}s "
                  f"({r['kept']/el:,.0f} kept-games/s)")

        b_el, o_el = out["baseline"][2], out["optimized"][2]
        print(f"\n  speedup: {b_el/o_el:.2f}x  ({100*(1-o_el/b_el):.0f}% faster)\n")

        compare(out["baseline"][0], out["optimized"][0],
                ["parent_hash", "move_san"], "position-stats partial")
        compare(out["baseline"][1], out["optimized"][1],
                ["parent_hash", "move_san", "move_bucket"], "crush partial")

        # On-disk byte-identity is reported for information only, never asserted:
        # zstd/parquet writes are not required to be reproducible byte-for-byte
        # (metadata, compression-block boundaries), so a mismatch here is not a
        # defect. Frame equality above is the binding check.
        for kind, i in (("ps", 0), ("crush", 1)):
            a, b = out["baseline"][i], out["optimized"][i]
            same = a.read_bytes() == b.read_bytes()
            print(f"  info  {kind} partial bytes identical on disk: {same} "
                  f"(not asserted — parquet writes need not be reproducible)")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
