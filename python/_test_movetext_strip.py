"""The M1 movetext strip must remove exactly what iter_san_moves already discards.

This is the one step CLAUDE.md flags as LOSSY: canonical D: movetext has its brace
comments ({[%clk ...]}, {[%eval ...]}) removed at ingest and they survive only in
the F: raw archive. The whole justification for doing that is a claim of no
downstream effect, and that claim has TWO parts, both asserted here:

  1. the SAN token stream is unchanged  -> every position-stats edge is unchanged
  2. move_count is unchanged            -> every crush move_bucket is unchanged
     (move_count counts white "N. " tokens via \\d+\\.\\s, computed pre-strip)

It rests on two INDEPENDENT implementations agreeing: the polars regex chain in
process_pgn_parquets.movetext_strip_expr, and the tokenizer in
stage1_extract_positions.iter_san_moves. Nothing forces them to stay in step, and
they are not interchangeable in general -- `1...e5` with no space strips to `e5`
but tokenizes as the unparseable `1...e5`. The invariant survives only because
Lichess always writes the space, so check 4 asserts that PRECONDITION on real
data: if the upstream format ever changes, this fires instead of the strip
silently eating a move.

The production expression is exercised directly (not a copy of its patterns),
which is why movetext_strip_expr exists as a function.

Fixture: 600 real 2025-10 games, sampled across the file (599 comment-bearing,
54 with [%eval]). Point --source at a raw F: parquet for a wider sweep.

Usage:  python _test_movetext_strip.py [--source PATH] [--games N]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from process_pgn_parquets import movetext_strip_expr
from stage1_extract_positions import iter_san_moves

FIXTURE = Path(__file__).parent / "_test_fixtures/movetext_strip/raw_movetext.parquet"

# The shape where the two implementations diverge: move-number dots NOT followed
# by whitespace. Lichess never emits it; if it ever does, the strip stops being
# equivalent to the tokenizer and this test says so.
DIVERGENT = re.compile(r"\d+\.\.\.\S")
# move_count, as computed in process_pgn_parquets (pre-strip).
MOVE_NUM = re.compile(r"\d+\.\s")

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=None,
                    help="Raw parquet with comment-bearing movetext (default: fixture).")
    ap.add_argument("--games", type=int, default=0, help="Cap games (0 = all).")
    a = ap.parse_args()

    src = Path(a.source) if a.source else FIXTURE
    if not src.exists():
        print(f"  SKIP: no movetext source at {src}")
        print("\nALL PASS (0 checks — source unavailable)")
        sys.exit(0)

    df = pl.read_parquet(src, columns=["movetext"]).filter(
        pl.col("movetext").is_not_null())
    if a.games:
        df = df.head(a.games)
    raw = df["movetext"].to_list()
    # THE PRODUCTION EXPRESSION, not a reimplementation of it.
    stripped = df.select(movetext_strip_expr())["movetext"].to_list()
    print(f"Source: {src.name}  ({len(raw):,} games)\n")

    # A pass proves nothing if the corpus has nothing to strip.
    n_comment = sum(1 for m in raw if "{" in m)
    check(n_comment >= 0.5 * len(raw),
          f"corpus actually exercises the strip ({n_comment:,}/{len(raw):,} "
          f"games carry brace comments)")

    bad_tok = bad_cnt = 0
    first_tok = first_cnt = None
    for r, s in zip(raw, stripped):
        if list(iter_san_moves(r)) != list(iter_san_moves(s)):
            bad_tok += 1
            if first_tok is None:
                first_tok = (r, s)
        if len(MOVE_NUM.findall(r)) != len(MOVE_NUM.findall(s)):
            bad_cnt += 1
            if first_cnt is None:
                first_cnt = (r, s)

    check(bad_tok == 0,
          f"SAN token stream identical raw vs stripped ({len(raw):,} games, "
          f"{bad_tok} mismatched)")
    if first_tok:
        r, s = first_tok
        print(f"        raw     : {list(iter_san_moves(r))[:12]}")
        print(f"        stripped: {list(iter_san_moves(s))[:12]}")

    check(bad_cnt == 0,
          f"move_count (\\d+\\.\\s) identical raw vs stripped ({bad_cnt} mismatched)")
    if first_cnt:
        r, s = first_cnt
        print(f"        raw={len(MOVE_NUM.findall(r))} stripped={len(MOVE_NUM.findall(s))}")
        print(f"        raw text: {r[:160]!r}")

    n_div = sum(1 for m in raw if DIVERGENT.search(m))
    check(n_div == 0,
          f"precondition: no game writes move-number dots without a following "
          f"space ({n_div} found — this is what makes the strip safe)")

    # Why that precondition is load-bearing. Pinned so that if someone teaches
    # iter_san_moves to handle the bare form, this states the relationship changed.
    probe = "1. e4 1...e5 2. Nf3 1-0"
    probe_s = pl.DataFrame({"movetext": [probe]}).select(
        movetext_strip_expr())["movetext"][0]
    diverges = list(iter_san_moves(probe)) != list(iter_san_moves(probe_s))
    check(diverges,
          "the documented divergence is real: '1...e5' strips to 'e5' but "
          "tokenizes as '1...e5' (so the precondition above is load-bearing)")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
