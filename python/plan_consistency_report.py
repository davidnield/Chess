"""
Plan-consistency report — measure the LEARNABILITY of a built repertoire:
how consistent are the IDEAS (piece destinations, pawn breaks, castling)
across its variations, weighted by the % OF GAMES in which each idea occurs.

Why % of games and not % of variations: rare deviations (e.g. punishing an
opponent blunder) live on low-reach paths and barely move a game-weighted
number — they should NOT count against a repertoire's coherence. What should
count is needing DIFFERENT plans against openings you actually face, or
needing precise nuance against rare lines (the "memorize the King's Gambit
move orders" problem). This report is the measurement half of the learnability
project (step 1): it changes NO selection, it just makes today's consistency
visible so a plan-prior can later be fed back into Stage 3.

Method
------
1. IDEA TOKENS. Each of our SAN moves is normalized to the idea it embodies:
     - piece moves  -> piece + destination square  (Nbd2 / Nxd2 -> "Nd2":
       the idea is WHERE the piece goes, not how it got there)
     - pawn pushes  -> destination square          ("c5" = the c5 break)
     - pawn captures keep the source file          ("cxd5" != "exd5")
     - castling     -> "O-O" / "O-O-O"
2. REACH-WEIGHTED PATH WALK. A depth-first walk of the book tree from the
   start position: at our turn follow best_move; at the opponent's turn
   branch over their recorded replies weighted by empirical frequency
   (same data Stage 3 propagates with). The walk is PATH-exact (no
   transposition merging) because "played at least once this game" is a
   path property. Paths are pruned below --epsilon weight (leaked mass is
   reported). The walk needs no board replay: it follows (parent_hash,
   move_san) -> child_hash through the stats edge table. An our-move with
   no recorded edge (engine-augmented rescue) is counted, then the path
   ends (its continuation is outside the recorded DAG).
3. AGGREGATION. game_mass[token] += path weight at the FIRST occurrence of
   the token on a path — every game flowing through that node plays the
   idea, including games where the opponent leaves book later. Dividing by
   the context's total mass gives exactly "% of games in which we go for
   this idea". Tokens are split by CONTEXT = the opponent's first move
   (vs 1.e4 / vs 1.d4 ... for black; vs 1...c5 / 1...e5 ... for white).

Headline numbers per context:
   - habit index: reach-weighted mean over our moves of "% of this
     context's games in which that move's idea appears" — how habitual the
     average move you play is (100% = you always do the same things).
   - ideas for 90% of moves: how many distinct tokens cover 90% of your
     reach-weighted move mass — the working vocabulary size.

Usage:
    .venv/Scripts/python.exe python/plan_consistency_report.py \\
        --repertoire E:/chess/repertoire/repertoire_pooled_black_sharp.parquet \\
        --perspective black \\
        --stats E:/chess/position-stats/position_stats_pooled_ge1800_2019_2025_brc.parquet \\
        [--max-our-moves 12] [--epsilon 1e-6] [--min-pct 2.0] [--top 30] \\
        [--output report.txt] [--export-prefix E:/chess/repertoire/_plan/black]

--export-prefix additionally writes <prefix>_prior.parquet (ctx, token,
game_freq) and <prefix>_reach.parquet (position_hash, ctx, reach) — the inputs
Stage 3's learnability tiebreak consumes via --plan-prior / --plan-reach.
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from pathlib import Path

import chess
import chess.polyglot
import numpy as np
import polars as pl

# Single source of truth for the idea vocabulary: selection (stage 3's
# learnability tiebreak) and measurement (this report) MUST tokenize alike.
from stage3_backwards_induction import LEARN_GLOBAL_CTX as GLOBAL_CTX
from stage3_backwards_induction import idea_token  # noqa: F401  (re-export for tests)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

INT64_MAX   = 2**63 - 1
INT64_RANGE = 2**64


def zobrist_int64(b: chess.Board) -> int:
    h = chess.polyglot.zobrist_hash(b)
    return h - INT64_RANGE if h > INT64_MAX else h


# ── Walk ───────────────────────────────────────────────────────────────────

def analyze_slice(
    rep_slice: pl.DataFrame,
    stats_slice: pl.DataFrame,
    perspective: str,
    max_our_moves: int,
    epsilon: float,
) -> dict:
    """Path-exact reach-weighted DFS. Returns raw tallies:
       game_mass  (ctx, token) -> mass of games where token occurs >= once
       move_mass  (ctx, token) -> reach-weighted count of our-move occurrences
       ctx_mass   ctx -> game mass entering our book under that context
       pruned     mass dropped below epsilon (error bound on deep tokens)
    """
    white = perspective == "white"
    our_parity = 0 if white else 1          # depth % 2 == our_parity -> our turn

    # Our book moves: sorted arrays for searchsorted lookup.
    ours = (rep_slice
            .filter((pl.col("side_to_move") == perspective)
                    & pl.col("best_move").is_not_null())
            .select(["position_hash", "best_move"])
            .sort("position_hash"))
    our_hashes = ours["position_hash"].to_numpy()
    our_sans   = ours["best_move"].to_list()

    # Stats edges: sorted by parent for contiguous-range lookup.
    edges = (stats_slice
             .select(["parent_hash", "move_san", "total", "child_hash"])
             .sort("parent_hash"))
    e_parent = edges["parent_hash"].to_numpy()
    e_san    = edges["move_san"].to_list()
    e_total  = edges["total"].to_numpy().astype(np.float64)
    e_cnull  = edges["child_hash"].is_null().to_numpy()
    e_child  = edges["child_hash"].fill_null(0).to_numpy()

    def our_san(ph: int) -> str | None:
        i = np.searchsorted(our_hashes, ph)
        if i < len(our_hashes) and our_hashes[i] == ph:
            return our_sans[i]
        return None

    def edge_range(ph: int) -> tuple[int, int]:
        lo = int(np.searchsorted(e_parent, ph, side="left"))
        hi = int(np.searchsorted(e_parent, ph, side="right"))
        return lo, hi

    game_mass: dict[tuple[str, str], float] = defaultdict(float)
    move_mass: dict[tuple[str, str], float] = defaultdict(float)
    ctx_mass:  dict[str, float]             = defaultdict(float)
    node_reach: dict[int, float]            = defaultdict(float)  # our-turn nodes
    node_ctx:  dict[tuple[int, str], float] = defaultdict(float)  # dominant-ctx votes
    node_depth: dict[int, int]              = {}                  # min our-moves before node
    tallies = {"pruned": 0.0}

    played: set[str] = set()                 # tokens on the current path
    pending: list[tuple[str, bool]] = []     # (token, is_first) before ctx is fixed

    def record(tok: str, first: bool, ctx: str, w: float) -> None:
        move_mass[(ctx, tok)] += w
        move_mass[(GLOBAL_CTX, tok)] += w
        if first:
            game_mass[(ctx, tok)] += w
            game_mass[(GLOBAL_CTX, tok)] += w

    sys.setrecursionlimit(10_000)

    def dfs(ph: int, depth: int, w: float, ctx: str | None, n_our: int) -> None:
        if depth % 2 == our_parity:
            san = our_san(ph)
            if san is None:                  # out of book / eval frontier
                return
            node_reach[ph] += w
            if ctx is not None:
                node_ctx[(ph, ctx)] += w
            d = node_depth.get(ph)
            if d is None or n_our < d:
                node_depth[ph] = n_our
            tok = idea_token(san)
            first = tok not in played
            if first:
                played.add(tok)
            if ctx is None:
                pending.append((tok, first))
            else:
                record(tok, first, ctx, w)
            if n_our + 1 < max_our_moves:
                # follow our move through the recorded edge; augmented rescues
                # have no edge -> counted above, path ends here.
                lo, hi = edge_range(ph)
                for i in range(lo, hi):
                    if e_san[i] == san and not e_cnull[i]:
                        dfs(int(e_child[i]), depth + 1, w, ctx, n_our + 1)
                        break
            if ctx is None:
                pending.pop()
            if first:
                played.discard(tok)
        else:
            lo, hi = edge_range(ph)
            if lo == hi:
                return
            tot = float(e_total[lo:hi].sum())
            if tot <= 0:
                return
            fixing = ctx is None             # this opponent move fixes the context
            for i in range(lo, hi):
                if e_cnull[i]:
                    continue
                wb = w * (e_total[i] / tot)
                if wb < epsilon:
                    tallies["pruned"] += wb
                    continue
                ctx2 = ctx
                if fixing:
                    ctx2 = e_san[i]
                    ctx_mass[ctx2] += wb
                    ctx_mass[GLOBAL_CTX] += wb
                    for tok, first in pending:
                        record(tok, first, ctx2, wb)
                dfs(int(e_child[i]), depth + 1, wb, ctx2, n_our)

    start_hash = zobrist_int64(chess.Board())
    dfs(start_hash, 0, 1.0, None, 0)

    return {"game_mass": dict(game_mass), "move_mass": dict(move_mass),
            "ctx_mass": dict(ctx_mass), "pruned": tallies["pruned"],
            "node_reach": dict(node_reach), "node_ctx": dict(node_ctx),
            "node_depth": dict(node_depth)}


def export_plan(res: dict, prefix: Path) -> tuple[Path, Path]:
    """Write the two Stage-3 learnability inputs (atomic, repo convention):
       <prefix>_prior.parquet  (ctx, token, game_freq, ctx_share) — incl. global rows
       <prefix>_reach.parquet  (position_hash, ctx, reach) — dominant ctx per node
    ctx_share (fraction of all games under that context) drives Stage 3's shrinkage
    of rare-context habits toward the GLOBAL habits — without it a rare context's
    own pass-1 oddities (e.g. a precise knight maneuver vs 1.b3) self-reinforce.
    Consumed by stage3_backwards_induction.py --plan-prior / --plan-reach."""
    ctx_mass = res["ctx_mass"]
    total = ctx_mass.get(GLOBAL_CTX, 0.0) or 1.0
    rows = [(c, t, gm / ctx_mass[c], ctx_mass[c] / total)
            for (c, t), gm in res["game_mass"].items() if ctx_mass.get(c, 0) > 0]
    prior = pl.DataFrame({"ctx":       [r[0] for r in rows],
                          "token":     [r[1] for r in rows],
                          "game_freq": [r[2] for r in rows],
                          "ctx_share": [r[3] for r in rows]})
    dom: dict[int, tuple[str, float]] = {}
    for (ph, c), m in res["node_ctx"].items():
        if ph not in dom or m > dom[ph][1]:
            dom[ph] = (c, m)
    phs = list(res["node_reach"].keys())
    reach = pl.DataFrame({"position_hash": pl.Series(phs, dtype=pl.Int64),
                          "ctx":   [dom.get(ph, (GLOBAL_CTX, 0.0))[0] for ph in phs],
                          "reach": [res["node_reach"][ph] for ph in phs],
                          "min_our_depth": pl.Series(
                              [res["node_depth"].get(ph, 0) for ph in phs],
                              dtype=pl.Int32)})
    prefix.parent.mkdir(parents=True, exist_ok=True)
    out_p = prefix.parent / (prefix.name + "_prior.parquet")
    out_r = prefix.parent / (prefix.name + "_reach.parquet")
    for df, out in ((prior, out_p), (reach, out_r)):
        tmp = out.with_suffix(out.suffix + ".tmp")
        df.write_parquet(tmp)
        tmp.replace(out)
    return out_p, out_r


# ── Reporting ──────────────────────────────────────────────────────────────

def render(res: dict, perspective: str, max_our_moves: int, epsilon: float,
           min_pct: float, top: int, min_ctx_pct: float) -> str:
    white = perspective == "white"
    game_mass, move_mass, ctx_mass = (res["game_mass"], res["move_mass"],
                                      res["ctx_mass"])
    total = ctx_mass.get(GLOBAL_CTX, 0.0)
    L: list[str] = []
    L.append(f"Horizon: our first {max_our_moves} moves | epsilon {epsilon:g} "
             f"(pruned mass {res['pruned'] * 100:.2f}%) | "
             f"book-entered mass {total * 100:.1f}%")

    def ctx_label(ctx: str) -> str:
        if ctx == GLOBAL_CTX:
            return ctx
        return f"vs 1...{ctx}" if white else f"vs 1.{ctx}"

    ordered = [GLOBAL_CTX] + sorted(
        (c for c in ctx_mass if c != GLOBAL_CTX),
        key=lambda c: -ctx_mass[c])

    for ctx in ordered:
        cmass = ctx_mass[ctx]
        share = cmass / total if total else 0.0
        if ctx != GLOBAL_CTX and share * 100 < min_ctx_pct:
            continue
        toks = [(t, gm / cmass) for (c, t), gm in game_mass.items() if c == ctx]
        toks.sort(key=lambda kv: -kv[1])
        mm = {t: m for (c, t), m in move_mass.items() if c == ctx}
        mtot = sum(mm.values())
        habit = (sum(m * (game_mass.get((ctx, t), 0.0) / cmass)
                     for t, m in mm.items()) / mtot) if mtot else 0.0
        # working vocabulary: tokens covering 90% of our-move mass
        acc, vocab = 0.0, 0
        for _t, m in sorted(mm.items(), key=lambda kv: -kv[1]):
            acc += m
            vocab += 1
            if acc >= 0.9 * mtot:
                break
        L.append("")
        L.append(f"{'=' * 66}")
        L.append(f"{ctx_label(ctx)}   ({share * 100:.1f}% of games, "
                 f"{mtot / cmass:.1f} booked moves/game)")
        L.append(f"  habit index {habit * 100:.0f}%   |   "
                 f"ideas covering 90% of moves: {vocab}")
        L.append(f"  {'idea':<10} % of games")
        shown = 0
        for t, pct in toks:
            if pct * 100 < min_pct or shown >= top:
                break
            L.append(f"  {t:<10} {pct * 100:8.1f}%")
            shown += 1
    return "\n".join(L)


# ── Main ───────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repertoire", required=True, help="Stage 3 repertoire parquet.")
    ap.add_argument("--perspective", choices=["white", "black"], required=True)
    ap.add_argument("--stats", required=True,
                    help="Matching stats parquet (needs child_hash) — supplies both "
                         "opponent reply frequencies and our-move edges.")
    ap.add_argument("--max-our-moves", type=int, default=12,
                    help="Horizon: only our first N moves per game count (default 12).")
    ap.add_argument("--epsilon", type=float, default=1e-6,
                    help="Prune paths below this reach weight (default 1e-6).")
    ap.add_argument("--min-pct", type=float, default=2.0,
                    help="Hide ideas below this %% of games (default 2).")
    ap.add_argument("--top", type=int, default=30,
                    help="Max ideas listed per context (default 30).")
    ap.add_argument("--min-ctx-pct", type=float, default=1.0,
                    help="Hide contexts below this %% of games (default 1).")
    ap.add_argument("--output", default=None, help="Also write the report here.")
    ap.add_argument("--export-prefix", default=None,
                    help="Write <prefix>_prior.parquet + <prefix>_reach.parquet — the "
                         "Stage-3 learnability inputs (--plan-prior / --plan-reach). "
                         "With multiple slices, the slice is appended to the prefix.")
    args = ap.parse_args()

    rep_path = Path(args.repertoire)
    print(f"Repertoire:    {rep_path}")
    print(f"Perspective:   {args.perspective}")
    print(f"Stats:         {args.stats}\n")

    rep = pl.read_parquet(rep_path,
                          columns=["event", "elo_band", "position_hash",
                                   "side_to_move", "best_move"])
    stats = pl.read_parquet(args.stats,
                            columns=["event", "elo_band", "parent_hash",
                                     "move_san", "total", "child_hash"])

    reports: list[str] = []
    slices = rep.select(["event", "elo_band"]).unique().sort(["event", "elo_band"])
    multi = len(slices) > 1
    for sr in slices.iter_rows(named=True):
        ev, eb = sr["event"], sr["elo_band"]
        t0 = time.time()
        res = analyze_slice(
            rep.filter((pl.col("event") == ev) & (pl.col("elo_band") == eb)),
            stats.filter((pl.col("event") == ev) & (pl.col("elo_band") == eb)),
            args.perspective, args.max_our_moves, args.epsilon)
        hdr = (f"PLAN-CONSISTENCY: {rep_path.name} ({args.perspective})  "
               f"slice {ev}/elo{eb}  [walk {time.time() - t0:.1f}s]")
        reports.append(hdr + "\n" + render(
            res, args.perspective, args.max_our_moves, args.epsilon,
            args.min_pct, args.top, args.min_ctx_pct))
        if args.export_prefix:
            prefix = Path(args.export_prefix + (f"_{ev}_{eb}" if multi else ""))
            out_p, out_r = export_plan(res, prefix)
            print(f"Exported plan prior: {out_p}\n"
                  f"         node reach: {out_r}  ({len(res['node_reach']):,} nodes)")

    report = "\n\n".join(reports)
    print(report)
    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(report, encoding="utf-8")
        print(f"\nReport saved to: {out}")


if __name__ == "__main__":
    main()
