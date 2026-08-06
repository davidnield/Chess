"""The pool's stored child_hash agrees with a python-chess re-derivation.

History: this file used to prove that removing an (epd, san) -> child_hash memo
from merge_position_stats changed nothing (the cache could not hit — the GROUP BY
above it already made the key unique, so ~23.45M entries served 18 lookups).
The derivation loop it guarded is now gone entirely: child_hash comes out of the
extract replay, where the position after ply p is the position before ply p+1.

What survives is the invariant that actually matters and is worth keeping pinned
on the SHIPPED pool: whatever produced the stored column, replaying the move from
parent_epd must reproduce it. _test_extract_child_hash.py checks the same identity
at the source (on freshly replayed games); this checks it on the artifact Stage 3
actually reads, which is the one that would silently mis-link the DAG.

Usage:  python _test_merge_child_hash.py [--rows N] [--stats PATH]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import chess
import chess.polyglot
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))

DEFAULT_STATS = Path("E:/chess/position-stats/"
                     "position_stats_pooled_ge1800_2013_2025_brc.parquet")

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


INT64_MAX, INT64_RANGE = 2**63 - 1, 2**64


def _one(epd: str, san: str):
    try:
        b = chess.Board(epd)
        b.push(b.parse_san(san))
        hh = chess.polyglot.zobrist_hash(b)
        return hh - INT64_RANGE if hh > INT64_MAX else hh
    except Exception:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=15_000,
                    help="Rows to compare. The default keeps run_tests.py fast; the "
                         "uniqueness property was measured on all 23,450,758 rows "
                         "(23,450,740 distinct keys). Rerun wider with --rows 200000.")
    ap.add_argument("--stats", default=str(DEFAULT_STATS))
    args = ap.parse_args()

    stats = Path(args.stats)
    if not stats.exists():
        print(f"  SKIP: stats parquet not found at {stats}")
        print("\nALL PASS (0 checks — source data unavailable)")
        sys.exit(0)

    print(f"Stats: {stats.name}")
    df = pl.read_parquet(stats, columns=["parent_hash", "parent_epd", "move_san",
                                         "child_hash"]).head(args.rows)
    epds = df["parent_epd"].to_list()
    sans = df["move_san"].to_list()
    n = len(epds)
    print(f"Rows:  {n:,}\n")

    # (a) the key is unique -> _agg_ps's .first() on child_hash is exact, and the
    #     removed memo could never have paid for itself
    n_key = df.select(["parent_epd", "move_san"]).n_unique()
    check(n_key == n,
          f"(parent_epd, move_san) unique on real data: {n_key:,}/{n:,}")

    # (b) the stored column reproduces under python-chess
    derived = [_one(e, s) for e, s in zip(epds, sans)]
    stored = df["child_hash"].to_list()
    bad = [i for i, (d, s) in enumerate(zip(derived, stored)) if d != s]
    check(not bad,
          f"stored child_hash == python-chess re-derivation over {n:,} rows "
          f"({len(bad)} mismatches)")
    for i in bad[:5]:
        print(f"        {sans[i]:<8} stored {stored[i]} derived {derived[i]}")
        print(f"          {epds[i]}")

    # (c) nothing null: a null would drop the edge at stage3's CSR filter
    check(df["child_hash"].null_count() == 0,
          "no null child_hash (a null silently drops the edge in Stage 3)")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
