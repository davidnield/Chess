"""child_hash emitted by the extract replay == python-chess re-derivation.

The extract now carries child_hash out of the replay (the position after ply p is
the position before ply p+1), which deleted the merge's single-threaded
re-derivation loop. That loop was the previous source of truth, so this test is
what keeps the two answers pinned together: a wrong child_hash does not crash
anything, it silently mis-links the DAG and every value propagates along the
wrong edges.

Deliberately run against a REAL source file, not fixtures. The failure modes that
matter here — castling, en passant, promotion, the depth-tier truncation, games
that fail SAN parsing partway — are ones synthetic positions are exactly the
least likely to reproduce in combination.

Checks:
  1. every emitted child_hash equals chess.Board(parent_epd) + push(move_san)
  2. child_hash is a FUNCTION of (parent_hash, move_san)  [what _agg_ps assumes]
  3. the chain closes: an edge's child_hash is the next ply's parent_hash
  4. no nulls
"""
from __future__ import annotations

import sys
from pathlib import Path

import chess
import chess.polyglot
import polars as pl
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
from build_pooled_stats import SOURCE_ROOT, _new_buf, _walk_game, _BUF_SCHEMA
from zobrist import IncrementalZobrist

GAMES = 3000
INT64_MAX, INT64_RANGE = 2**63 - 1, 2**64

failures = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global failures
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        failures += 1


def find_source() -> Path | None:
    for year in sorted((p for p in SOURCE_ROOT.glob("year=*")), reverse=True):
        for mon in sorted(year.glob("month=*")):
            for ev in sorted(mon.glob("event=*")):
                fs = sorted(ev.glob("*.parquet"))
                if fs:
                    return fs[0]
    return None


def replay(src: Path, optimize: bool) -> pl.DataFrame:
    """Same call shape extract_file uses, minus the aggregation."""
    buf = _new_buf()
    hasher = IncrementalZobrist(chess.Board()) if optimize else None
    memo: dict[int, str] | None = {} if optimize else None
    n = 0
    pf = pq.ParquetFile(src)
    cols = ["movetext", "white_score", "termination", "move_count", "mean_elo"]
    for batch in pf.iter_batches(batch_size=5000, columns=cols):
        for rec in batch.to_pylist():
            if n >= GAMES:
                break
            me, ws = rec["mean_elo"], rec["white_score"]
            if me is None or me < 1800 or ws is None:
                continue
            n += 1
            normal = rec["termination"] == "Normal"
            _walk_game(buf, rec["movetext"], ws, normal and ws == 1.0,
                       normal and ws == 0.0, rec["move_count"], None, 30,
                       hasher, memo)
        if memo is not None:
            memo.clear()
        if n >= GAMES:
            break
    return pl.DataFrame(buf, schema=_BUF_SCHEMA)


def main() -> None:
    print("=" * 72)
    print("EXTRACT child_hash == python-chess re-derivation")
    print("=" * 72)

    src = find_source()
    if src is None:
        print(f"  SKIP: no source parquet under {SOURCE_ROOT}")
        sys.exit(0)
    print(f"  source: {src}")

    df = replay(src, optimize=True)
    print(f"  replayed {df.height:,} plies\n")
    if not df.height:
        check("produced rows", False, "empty replay")
        sys.exit(1)

    # 1. against python-chess, on the distinct edges (the expensive check)
    #
    # Restricted to edges that CARRY an EPD. build_pooled_stats now populates
    # parent_epd only to EPD_MAX_PLY (16) -- past the opening it is null, because
    # it measured 50.3% of partial bytes and nothing renders a ply-40 position.
    # This check reconstructs the board FROM the EPD, so it can only run where
    # one exists; a null becomes chess.Board("8/8/8/8/8/8/8/8") and reports every
    # SAN as illegal.
    #
    # Depth is still covered, by other checks rather than by this one:
    #   * check 3 below walks the chain across EVERY ply at any depth, and a
    #     wrong deep hash shows up there as a broken parent/child link;
    #   * _test_zobrist_incremental.py differentially tests the hasher against
    #     python-chess over millions of plies with no depth limit.
    edges = (df.select(["parent_epd", "move_san", "parent_hash", "child_hash"])
               .filter(pl.col("parent_epd").is_not_null())
               .unique())
    n_deep = df.filter(pl.col("parent_epd").is_null()).height
    if n_deep:
        print(f"  EPD-backed check covers ply <= EPD_MAX_PLY; "
              f"{n_deep:,} deeper plies rely on checks 2-4")
    bad = []
    for epd, san, ph, ch in edges.iter_rows():
        b = chess.Board(epd)
        try:
            b.push(b.parse_san(san))
        except Exception as e:                       # noqa: BLE001
            bad.append((epd, san, f"unparseable: {e}"))
            continue
        hh = chess.polyglot.zobrist_hash(b)
        want = hh - INT64_RANGE if hh > INT64_MAX else hh
        if want != ch:
            bad.append((epd, san, f"got {ch} want {want}"))
    check("child_hash matches python-chess on every distinct edge",
          not bad, f"{len(edges):,} edges, {len(bad)} mismatches")
    for epd, san, why in bad[:5]:
        print(f"        {san:<8} {why}\n          {epd}")

    # 2. functional dependency — what _agg_ps's .first() relies on
    fd = (df.group_by("parent_hash", "move_san")
            .agg(pl.col("child_hash").n_unique().alias("n")))
    viol = int((fd["n"] > 1).sum())
    check("child_hash is a function of (parent_hash, move_san)", viol == 0,
          f"{fd.height:,} keys, {viol} with >1 distinct child")

    # 3. the chain closes within each game: consecutive plies link up.
    #    Rows are appended in replay order, so a ply-p row is immediately followed
    #    by its ply-(p+1) row whenever the game continued.
    ph = df["parent_hash"].to_list()
    ch = df["child_hash"].to_list()
    ply = df["ply"].to_list()
    breaks = sum(1 for i in range(len(ply) - 1)
                 if ply[i + 1] == ply[i] + 1 and ch[i] != ph[i + 1])
    links = sum(1 for i in range(len(ply) - 1) if ply[i + 1] == ply[i] + 1)
    check("edge's child_hash == next ply's parent_hash", breaks == 0,
          f"{links:,} consecutive pairs, {breaks} broken")

    # 4. no nulls
    check("no null child_hash", df["child_hash"].null_count() == 0)

    # 5. the unoptimized path (no incremental hasher / memo) agrees
    df2 = replay(src, optimize=False)
    same = df.equals(df2)
    check("optimized and unoptimized replays are frame-identical", same,
          f"{df.height:,} vs {df2.height:,} rows")

    print()
    print(f"{'ALL CHECKS PASSED' if failures == 0 else f'{failures} CHECK(S) FAILED'}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
