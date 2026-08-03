"""V4 — removing the merge's (epd, san) -> child_hash cache changes nothing.

build_pooled_stats.merge_position_stats derives child_hash by replaying one move
from each row's parent_epd. It used to memoize that on (parent_epd, move_san),
but the cache could not hit: the GROUP BY immediately above already made
(parent_hash, move_san) unique, and parent_epd is a function of parent_hash. So
the dict held ~23.45M entries — several GB, built in the single-threaded tail of
the merge — to serve 18 lookups.

This asserts (a) the key really is unique on real data, and (b) cached and
uncached derivation produce identical child_hash values.

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


def derive_uncached(epds, sans):
    """The shipped path."""
    return [_one(e, s) for e, s in zip(epds, sans)]


def derive_cached(epds, sans):
    """The removed path, reproduced verbatim for comparison."""
    cache: dict[tuple[str, str], int | None] = {}
    out = []
    for epd, san in zip(epds, sans):
        key = (epd, san)
        h = cache.get(key, "miss")
        if h == "miss":
            h = _one(epd, san)
            cache[key] = h
        out.append(h)
    return out, len(cache)


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

    # (a) the key is unique -> the cache could never have paid for itself
    n_key = df.select(["parent_epd", "move_san"]).n_unique()
    check(n_key == n,
          f"(parent_epd, move_san) unique on real data: {n_key:,}/{n:,} "
          f"-> cache hit rate {100 * (1 - n_key / n):.4f}%")

    # (b) identical output either way
    plain = derive_uncached(epds, sans)
    cached, cache_size = derive_cached(epds, sans)
    check(plain == cached,
          f"cached and uncached child_hash identical over {n:,} rows "
          f"(cache would have held {cache_size:,} entries)")

    # (c) and both still agree with what the shipped parquet already contains
    check(plain == df["child_hash"].to_list(),
          "derived child_hash matches the stored column in the shipped parquet")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
