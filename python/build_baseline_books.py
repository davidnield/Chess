"""Baseline repertoires to compare the sharp book against (task #114).

"Our book scores 0.55 out of sample" means nothing on its own. It needs a floor:
what would a book built by an obvious rule have scored, at the same size? Without
that, every sweep result is unanchored — which is the gap the Fable-5 review
named, and the reason #114 exists.

Three baselines, each a rule for picking OUR move at a node:

  popular         the most-played reply. What a person copying the explorer's
                  top line would produce. Beats surprisingly much.
  engine-top-1    the move whose resulting position has the best engine eval for
                  us. The "just play the computer move" book — strong positions,
                  no regard for whether anyone reaches them.
  greedy-winrate  the move with the best empirical score for our side, subject to
                  a games floor. The naive data-mining book, and the one whose
                  failure mode is instructive: it chases small-sample flukes.

EQUAL FOOTPRINT IS THE WHOLE POINT
----------------------------------
A bigger book covers more games, so comparing books of different sizes measures
size, not quality. Footprint here is the number of OUR-TURN decisions — the
things a human memorises — and every baseline is grown to the same count as the
book under test (--budget, or --like to read it off an existing repertoire).

Growth is best-first by REACH PROBABILITY, not breadth-first: at an opponent node
each reply carries prob = total/sum(total), at our node the chosen move carries 1,
so reach(child) = reach(parent) x p. Spending the budget on the most likely
positions is what any sane repertoire does, and doing it any other way would
hand the baselines a handicap the comparison would then mistake for our book
being good.

A NOTE ON `value`
-----------------
These books carry a `value` column because downstream readers key membership off
it, but it is the node's EMPIRICAL score from position_stats — a descriptive
statistic, not Stage 3's propagated prediction. Coverage and realized score are
comparable across baseline and sharp books; CALIBRATION IS NOT. Do not read a
baseline's calibration table as if it meant the same thing.

Usage:
    .venv/Scripts/python.exe python/build_baseline_books.py \\
        --stats E:/chess/position-stats/position_stats_pooled_ge1800_2013_2026_brc.parquet \\
        --perspective white --rule popular \\
        --like E:/chess/repertoire/repertoire_pooled_white_sharp.parquet \\
        --out E:/chess/repertoire/baseline_white_popular.parquet
"""
from __future__ import annotations

import argparse
import heapq
import sys
import time
from pathlib import Path

import duckdb
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))

RULES = ("popular", "engine-top-1", "greedy-winrate", "from-book")
# "from-book" is not a baseline — it re-grows an EXISTING repertoire through this
# same traversal, which is how the sharp book gets cut to a comparable footprint.
# Pruning it with separate code would risk the two sides differing in exactly the
# way the comparison is supposed to rule out.
# Opponent replies below this share of a node's mass are not expanded. They are
# still counted as mass we fail to cover, so dropping them costs coverage rather
# than hiding it.
MIN_REPLY_SHARE = 0.01
# greedy-winrate without a floor picks whatever 3-game line happens to be 100%.
# The floor is what makes it a baseline rather than a noise generator.
GREEDY_MIN_GAMES = 50


def edges_for(con, hashes: list[int], stats: str, min_games: int) -> pl.DataFrame:
    """All outgoing edges of a frontier, one query per BFS level.

    Level-synchronous on purpose: the pooled stats file is far too large to hold
    in memory, and per-node queries would be thousands of round trips. This is
    ~30 queries for a ply-30 book.
    """
    con.execute("CREATE OR REPLACE TEMP TABLE frontier (h BIGINT)")
    if hashes:
        con.executemany("INSERT INTO frontier VALUES (?)", [(h,) for h in hashes])
    return con.execute(f"""
        SELECT s.parent_hash, s.move_san, s.child_hash, s.parent_epd, s.ply,
               s.total, s.white_wins, s.draws, s.black_wins
        FROM read_parquet('{stats}') s
        JOIN frontier f ON f.h = s.parent_hash
        WHERE s.total >= {min_games}
    """).pl()


def attach_evals(df: pl.DataFrame, mm_hash, mm_cp) -> pl.DataFrame:
    """child_hash -> engine eval. The pooled stats carries no eval column, so
    engine-top-1 has to go through the same mmap'd arrays the extract uses —
    reading it from the stats file (the first attempt here) is not an option."""
    import numpy as np

    from eval_arrays import MISSING, lookup_evals
    keys = df["child_hash"].fill_null(0).to_numpy().astype(np.int64)
    ev = lookup_evals(keys, mm_hash, mm_cp).astype(np.int32)
    return df.with_columns(
        pl.when(pl.Series(ev) == int(MISSING)).then(None)
          .otherwise(pl.Series(ev)).alias("child_eval"))


def our_score(w: int, d: int, b: int, n: int, white: bool) -> float:
    if not n:
        return 0.5
    ws = (w + 0.5 * d) / n
    return ws if white else 1.0 - ws


def child_of(epd: str, san: str) -> int | None:
    """Hash after playing `san` from `epd`, for moves absent from the pool.

    Stage 3 can select an ENGINE-AUGMENTED move that no pooled game ever played,
    so the sharp book's best_move is not always one of the stats edges. Dropping
    those nodes would silently prune the book exactly where it is most original.
    """
    import chess

    from zobrist import IncrementalZobrist, zobrist_int64
    try:
        b = chess.Board(epd + " 0 1") if len(epd.split()) == 4 else chess.Board(epd)
        h = IncrementalZobrist(b)
        h.push_move(b, b.parse_san(san))
        return h.current(b)
    except Exception:                                             # noqa: BLE001
        return None


def pick(rule: str, grp: pl.DataFrame, white: bool,
         book: dict | None = None, ph: int | None = None
         ) -> tuple[str, int] | None:
    """Apply a rule to one node's candidate edges. None = stop here."""
    if rule == "from-book":
        san = (book or {}).get(ph)
        if not san:
            return None                      # a leaf of the source book
        hit = grp.filter(pl.col("move_san") == san)
        if hit.height:
            return san, hit.row(0, named=True)["child_hash"]
        return san, child_of(grp.row(0, named=True)["parent_epd"], san)
    if rule == "popular":
        r = grp.sort("total", descending=True).row(0, named=True)
    elif rule == "greedy-winrate":
        elig = grp.filter(pl.col("total") >= GREEDY_MIN_GAMES)
        elig = elig if elig.height else grp
        scored = [(our_score(r["white_wins"], r["draws"], r["black_wins"],
                             r["total"], white), r["total"], r)
                  for r in elig.iter_rows(named=True)]
        # total breaks ties so the rule is deterministic across runs.
        r = max(scored, key=lambda x: (x[0], x[1]))[2]
    else:  # engine-top-1
        ev = grp.filter(pl.col("child_eval").is_not_null())
        if not ev.height:
            r = grp.sort("total", descending=True).row(0, named=True)
        else:
            # child_eval is centipawns from WHITE's view at the child position.
            r = ev.sort("child_eval", descending=white).row(0, named=True)
    return r["move_san"], r["child_hash"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--stats", required=True)
    ap.add_argument("--perspective", choices=["white", "black"], required=True)
    ap.add_argument("--rule", choices=RULES, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--budget", type=int, default=0,
                    help="Our-turn decisions to spend. 0 with --like reads it "
                         "from that repertoire.")
    ap.add_argument("--like", default=None,
                    help="Repertoire to match the footprint of.")
    ap.add_argument("--book", default=None,
                    help="Source repertoire for --rule from-book: re-grow it "
                         "through this traversal, cut to --budget decisions.")
    ap.add_argument("--min-games", type=int, default=50,
                    help="Edge floor, matching the pool's own floor.")
    ap.add_argument("--max-ply", type=int, default=30)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--mem", default="8GB")
    a = ap.parse_args()

    white = a.perspective == "white"
    budget = a.budget
    if a.like and not budget:
        lf = pl.read_parquet(a.like, columns=["side_to_move", "best_move"])
        budget = lf.filter((pl.col("side_to_move") == a.perspective)
                           & pl.col("best_move").is_not_null()).height
        print(f"footprint from {Path(a.like).name}: {budget:,} our-turn decisions")
    if not budget:
        print("FATAL: give --budget or --like")
        return 1

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.mem}'")
    con.execute(f"SET threads={a.threads}")
    stats = a.stats.replace("\\", "/")

    import chess
    from zobrist import zobrist_int64
    root = zobrist_int64(chess.Board())

    src_moves: dict[int, str] = {}
    src_values: dict[int, float] = {}
    if a.rule == "from-book":
        if not a.book:
            print("FATAL: --rule from-book needs --book")
            return 1
        bf = pl.read_parquet(a.book, columns=["position_hash", "side_to_move",
                                              "value", "best_move"])
        for h, stm, v, mv in bf.iter_rows():
            if v is not None:
                src_values[h] = v
            if stm == a.perspective and mv:
                src_moves[h] = mv
        print(f"source book: {len(src_moves):,} our-turn decisions available, "
              f"cutting to the {budget:,} most reachable")

    mm = None
    if a.rule == "engine-top-1":
        from eval_arrays import open_eval_arrays
        mm = open_eval_arrays()
        print("eval arrays mmapped for engine-top-1")

    # (-reach, tie, hash, ply) — heapq is a min-heap, so negate to pop the most
    # reachable node first.
    heap: list[tuple[float, int, int, int]] = [(-1.0, 0, root, 0)]
    seen: set[int] = {root}
    rows: dict[int, dict] = {}
    spent = 0
    tie = 1
    t0 = time.time()

    while heap and spent < budget:
        # Drain one "level" worth: everything currently on the heap, so the
        # expensive stats query is amortised over many nodes.
        batch = []
        while heap and len(batch) < 20000:
            batch.append(heapq.heappop(heap))
        by_hash = {h: (-nr, ply) for nr, _t, h, ply in batch}
        df = edges_for(con, list(by_hash), stats, a.min_games)
        if df.is_empty():
            continue
        if mm is not None:
            df = attach_evals(df, *mm)
        for ph, grp in df.group_by("parent_hash"):
            ph = ph[0] if isinstance(ph, tuple) else ph
            reach, ply = by_hash.get(ph, (0.0, 0))
            if ply >= a.max_ply:
                continue
            first = grp.row(0, named=True)
            # ply parity decides whose turn it is: even ply = White to move.
            us = (ply % 2 == 0) == white
            if us:
                if spent >= budget:
                    continue
                got = pick(a.rule, grp, white, src_moves, ph)
                if got is None:
                    continue                 # source book has no move here
                san, ch = got
                tot = int(grp["total"].sum())
                # from-book keeps the source's Stage-3 value, so a pruned sharp
                # book stays calibration-comparable to its unpruned self. The
                # rule-based baselines fall back to the empirical score, which
                # is descriptive only.
                emp = our_score(int(grp["white_wins"].sum()), int(grp["draws"].sum()),
                                int(grp["black_wins"].sum()), tot, True)
                rows[ph] = dict(
                    position_hash=ph, position_epd=first["parent_epd"],
                    side_to_move="white" if ply % 2 == 0 else "black",
                    value=src_values.get(ph, emp) if a.rule == "from-book" else emp,
                    best_move=san)
                spent += 1
                if ch is not None and ch not in seen:
                    seen.add(ch)
                    heapq.heappush(heap, (-reach, tie, ch, ply + 1))
                    tie += 1
            else:
                tot = int(grp["total"].sum())
                emp = our_score(int(grp["white_wins"].sum()), int(grp["draws"].sum()),
                                int(grp["black_wins"].sum()), tot, True)
                rows[ph] = dict(
                    position_hash=ph, position_epd=first["parent_epd"],
                    side_to_move="white" if ply % 2 == 0 else "black",
                    value=src_values.get(ph, emp) if a.rule == "from-book" else emp,
                    best_move=None)
                for r in grp.iter_rows(named=True):
                    p = r["total"] / tot if tot else 0.0
                    if p < MIN_REPLY_SHARE or r["child_hash"] is None:
                        continue
                    if r["child_hash"] in seen:
                        continue
                    seen.add(r["child_hash"])
                    heapq.heappush(heap, (-(reach * p), tie, r["child_hash"], ply + 1))
                    tie += 1
        print(f"  spent {spent:,}/{budget:,} decisions, {len(rows):,} nodes, "
              f"heap {len(heap):,}  ({time.time()-t0:,.0f}s)", flush=True)

    out = Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(list(rows.values())).write_parquet(out, compression="zstd")
    print(f"\n{a.rule} / {a.perspective}: {len(rows):,} nodes, "
          f"{spent:,} our-turn decisions -> {out}")
    if a.rule == "from-book":
        print(f"NOTE: `value` carried over from the source book for "
              f"{sum(1 for h in rows if h in src_values):,} nodes, so this "
              f"pruned book stays calibration-comparable to its unpruned self.")
    else:
        print("NOTE: `value` here is the node's EMPIRICAL score, not a Stage-3 "
              "prediction. Coverage and realized score are comparable across "
              "books; calibration is not.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
