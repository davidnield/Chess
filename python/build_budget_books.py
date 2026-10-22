"""Build budget-constrained repertoire books from an unconstrained Stage-3 rep.

Chooses, under a hard budget of our-turn decisions ("moves to learn", counted
per build_baseline_books.py's footprint definition), which positions to book
AND which move to book at each — the budget is inside the selection, so a
400-move book may play different variations (early payoff) than a 40,000-move
book (which can afford deep traps). Method: per-node value-vs-budget curves
(budget_core.py, where the algorithm and its documented approximations live).

The four comparable methods Phase D evaluates:
  reach-truncation   build_baseline_books.py --rule from-book   (existing)
  greedy             --method greedy: myopic one-step gains, no chain atoms
  fixed-policy DP    --method dp --fixed-policy: optimal stopping, source moves
  full DP            --method dp: joint stopping + policy choice

Outputs one book per --budget from a single curve build. Books are
replay_holdout/score_repertoire-compatible; `value` on booked rows is the
BUDGETED prediction (V at the node's allocation), the source's unconstrained
value is preserved in `value_unconstrained`. A .meta.json sidecar records
budgets requested vs realized (path-sum and distinct), method, epsilon, the
root value-vs-budget curve, and the source rep's own meta digest.

The extraction argmax selects on value (a frozen crush bonus used to enter it;
crush was removed 2026-10-02).

Usage:
    .venv/Scripts/python.exe python/build_budget_books.py \\
        --rep E:/chess/repertoire/_sweep/baseline_white.parquet \\
        --stats E:/chess/position-stats/position_stats_pooled_ge1800_2013_2024_brc.parquet \\
        --perspective white --out-prefix E:/chess/repertoire/_budget/dp_white \\
        --budget 400 --budget 4000 --budget 40000 --method dp

Defaults for --aux-stats / --reply-shrink are read from the source rep's
.meta.json sidecar; explicit flags override.
reply_shrink > 0 without aux is NOT implemented (every current book is
aux + reply_shrink 0) — the build refuses rather than silently diverging.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import budget_core as bc                                    # noqa: E402
from stage3_backwards_induction import cp_to_expected_score  # noqa: E402
from zobrist import zobrist_int64                            # noqa: E402


def log(msg: str) -> None:
    print(msg, flush=True)


def peak_commit_gb() -> float | None:
    """Peak COMMIT (private bytes) for this process, the metric that actually
    binds on this box -- the ceiling is the commit limit, not physical RAM
    (CLAUDE.md). Matches what trip_chain.py reports for Stage 3, so budget runs
    and sweep runs are directly comparable. Returns None if psutil is absent."""
    try:
        import psutil
        return round(psutil.Process().memory_info().peak_pagefile / 1024 ** 3, 2)
    except Exception:
        return None


class GateAdmit:
    """--gated-reach for the collector: which of an our-node's recorded moves the
    gates admit (budget_core.gate_candidates, the definition build_graph uses).
    Evals come from the mmap'd arrays, prefetched a level at a time; the cache is
    the eval_ws the graph is built from afterwards."""

    def __init__(self, rep_moves, our_white: bool, slice_prior: float,
                 gates: bool = True, max_eval_loss: float | None = None):
        from eval_arrays import MISSING, lookup_evals, open_eval_arrays
        self._lookup, self._missing = lookup_evals, MISSING
        self._mm = open_eval_arrays()
        self.rep_moves, self.our_white = rep_moves, our_white
        self.slice_prior, self.gates = slice_prior, gates
        self.max_eval_loss = max_eval_loss
        self.eval_ws: dict[int, float] = {}
        self._seen: set[int] = set()

    def prefetch(self, hashes) -> None:
        new = np.array([h for h in set(hashes) if h not in self._seen], dtype=np.int64)
        if not len(new):
            return
        self._seen.update(new.tolist())
        for h, c in zip(new.tolist(), self._lookup(new, *self._mm).tolist()):
            if c != self._missing:
                self.eval_ws[h] = cp_to_expected_score(int(c))

    def __call__(self, ph: int, rows: list[dict]) -> set[int]:
        src = self.rep_moves.get(ph)
        return {e["child_hash"] for e, _ in bc.gate_candidates(
            ph, rows, src, self.eval_ws, self.our_white, self.slice_prior,
            require_eval=self.gates, gates=self.gates, max_eval_loss=self.max_eval_loss)
            if e.get("child_hash") is not None}


def load_allowed_moves(path: str, perspective: str, min_score: float = 0.0,
                       courses: list[str] | None = None,
                       book_only: bool = False) -> dict[int, set[str]]:
    """--allowed-moves: position -> the SANs (check marks stripped) our book may
    play there. A course_books course_moves.parquet (parent_hash, move_san, score,
    course_id, rep_side) keeps the repertoire side's moves scoring above
    `min_score` (0 = any recommendation; a move the course marks ? scores 0),
    optionally from some courses only, and with `book_only` only each course
    book's own choice there (in_book) rather than every move its analysis
    recommends. Any other parquet needs position_hash or parent_hash, and move_san."""
    d = pl.read_parquet(path)
    if "parent_hash" not in d.columns:
        d = d.rename({"position_hash": "parent_hash"})
    if "rep_side" in d.columns:
        d = d.filter(pl.col("rep_side") == perspective)
    if "score" in d.columns:
        d = d.filter(pl.col("score") > min_score)
    if courses:
        d = d.filter(pl.col("course_id").is_in(courses))
    if book_only and "in_book" in d.columns:
        d = d.filter(pl.col("in_book"))
    out: dict[int, set[str]] = {}
    for h, m in d.select("parent_hash", "move_san").unique().iter_rows():
        out.setdefault(h, set()).add(m.rstrip("+#"))
    return out


class AllowedAdmit:
    """--allowed-moves for the collector: at our nodes expand only the allowed
    moves (and, under --gated-reach, only those the gates also admit)."""

    def __init__(self, allowed: dict[int, set[str]], inner: GateAdmit | None = None):
        self.allowed, self.inner = allowed, inner

    def prefetch(self, hashes) -> None:
        if self.inner is not None:
            self.inner.prefetch(hashes)

    def __call__(self, ph: int, rows: list[dict]) -> set[int]:
        ok = self.allowed.get(ph)
        if not ok:
            return set()
        kids = {r["child_hash"] for r in rows if r.get("child_hash") is not None
                and r["move_san"].rstrip("+#") in ok}
        return kids & self.inner(ph, rows) if (kids and self.inner is not None) else kids


def restrict_candidates(g: "bc.Graph", allowed: dict[int, set[str]]) -> dict:
    """Drop every our-node candidate --allowed-moves does not list. A node left with
    none can only stop (its stop value), so the book ends there."""
    dropped = emptied = 0
    for h, n in g.our.items():
        ok = allowed.get(h, ())
        keep = [c for c in n.cands if c[0].rstrip("+#") in ok]
        dropped += len(n.cands) - len(keep)
        if n.cands and not keep:
            emptied += 1
        n.cands = keep
    return {"cands_dropped": dropped, "nodes_emptied": emptied}


def collect_subgraph_edges(con, stats: str, root: int, our_white: bool,
                           eps: float, max_ply: int, share_floor: float,
                           min_games: int, admit=None) -> pl.DataFrame:
    """Iterative frontier expansion (pattern: build_baseline_books.edges_for),
    keeping every edge of every node whose OPTIMISTIC reach >= eps. Reach decays
    only at opponent nodes, by reply share; our moves carry it unchanged.

    EDGES FETCHED AND REACH SETTLED ARE SEPARATE (fixed 2026-09-04). The guard
    used to be `if cr < eps or ch in fetched: continue`, which skipped the reach
    update below for anything already visited. A transposition first reached
    through a short unlikely path and later through a likelier one therefore
    KEPT the lower reach, and — the part that actually loses positions — its
    children had already been expanded at that lower reach, so any child then
    under eps was dropped and never revisited. Nothing downstream can recover
    them: build_graph only sees the rows this returns, and recomputing reach
    correctly over an already-truncated edge set still has no rows for the
    pruned cone.

    So a node is re-enqueued whenever its reach IMPROVES, and its children are
    re-expanded from the better value. Its adjacency is re-read rather than
    cached in Python (the rows are already in `frames`; a dict-of-dicts copy of
    a multi-million-edge subgraph is not worth the peak), and only genuinely new
    parents append to `frames`, so the returned edge set stays duplicate-free
    and memory is unchanged.

    Termination: reach only ever increases, is bounded by 1, and the update is
    strict (`>`), so a cycle whose shares multiply to exactly 1 cannot re-fire.
    Every chess cycle alternates turns, so it contains an opponent node whose
    share is < 1 and the product strictly decays toward the eps cutoff.

    admit (--gated-reach): at our nodes, follow only the moves the gates admit.
    Every recorded move of the node is still RETURNED (the stop value and the
    gates read them all); only the expansion is restricted. None = expand every
    recorded move, the policy-free default.

    NOTE: optimistic reach is a MAX over paths, not the total probability of
    arriving — those differ when several opponent move orders converge on one
    position, and the max is not an upper bound on the sum. It is used here only
    as a pruning heuristic; see the plan's note on policy-free reach.
    """
    frames: list[pl.DataFrame] = []
    reach: dict[int, float] = {root: 1.0}
    ply: dict[int, int] = {root: 0}
    frontier = [root]
    fetched: set[int] = set()
    lvl = 0
    reopened = 0
    while frontier:
        con.execute("CREATE OR REPLACE TEMP TABLE frontier (h BIGINT)")
        con.executemany("INSERT INTO frontier VALUES (?)",
                        [(h,) for h in frontier])
        df = con.execute(f"""
            SELECT s.parent_hash, s.move_san, s.child_hash, s.parent_epd,
                   s.total, s.white_wins, s.draws, s.black_wins,
                   s.white_score_avg
            FROM read_parquet('{stats}') s
            JOIN frontier f ON f.h = s.parent_hash
            WHERE s.total >= {min_games}
        """).pl()
        fresh = {h for h in frontier if h not in fetched}
        if df.height and fresh:
            keep = df.filter(pl.col("parent_hash").is_in(list(fresh)))
            if keep.height:
                frames.append(keep)
        fetched.update(frontier)
        nxt: dict[int, float] = {}
        if df.height and admit is not None:
            # parents and children of this level, for the gates' eval lookups
            admit.prefetch(df["parent_hash"].to_list()
                           + df["child_hash"].drop_nulls().to_list())
        if df.height:
            for ph, grp in df.group_by("parent_hash"):
                ph = ph[0] if isinstance(ph, tuple) else ph
                r = reach.get(ph, 0.0)
                p_ply = ply.get(ph, 0)
                if p_ply >= max_ply:
                    continue
                tot = int(grp["total"].sum())
                our_turn = (p_ply % 2 == 0) == our_white
                rows = list(grp.iter_rows(named=True))
                allowed = admit(ph, rows) if (our_turn and admit is not None) else None
                for row in rows:
                    ch = row["child_hash"]
                    if ch is None:
                        continue
                    if our_turn:
                        if allowed is not None and ch not in allowed:
                            continue
                        cr = r
                    else:
                        share = row["total"] / tot if tot else 0.0
                        if share < share_floor:
                            continue
                        cr = r * share
                    if cr < eps:
                        continue
                    if cr > reach.get(ch, 0.0):
                        if ch in fetched:
                            reopened += 1
                        reach[ch] = cr
                        # A position's side to move is fixed, so every path to it
                        # has the same parity and only the DEPTH can differ. Keep
                        # the shallowest, which is what level-order discovery gave
                        # before re-propagation existed.
                        ply[ch] = min(ply.get(ch, p_ply + 1), p_ply + 1)
                        nxt[ch] = cr
        frontier = list(nxt)
        lvl += 1
        log(f"  level {lvl}: fetched {len(fetched):,} nodes, "
            f"frontier {len(frontier):,}, edges so far "
            f"{sum(f.height for f in frames):,}"
            + (f", reopened {reopened:,}" if reopened else ""))
    if reopened:
        log(f"  reach re-propagation reopened {reopened:,} node-visits "
            f"(each one a subtree the old collector would have pruned)")
    return pl.concat([f for f in frames if f.height]) if frames else pl.DataFrame()


def evaluate_books(g, specs: list[str], perspective: str, log, what: str = "") -> None:
    """In-model value of FOLLOWING each LABEL=PATH book on graph g
    (budget_core.policy_value), then, for two or more books, where they differ: every
    shallow position (ply <= 6) some book reaches in >= 1% of games, with each book's
    reach and value there."""
    order, _cut = bc.topo_up(g)
    log(f"evaluating {len(specs)} book(s) on this graph ({what})")
    evals = {}
    for spec in specs:
        label, path = spec.split("=", 1)
        bd = pl.read_parquet(path, columns=["position_hash", "side_to_move", "best_move"])
        mv = {h: m for h, sd, m in bd.iter_rows() if sd == perspective and m}
        r = bc.policy_value(g, mv, order, detail=True)
        evals[label] = r
        log(f"  {label:<28} {len(mv):>7,} moves  value {r['value']:.5f} (our side)  "
            f"played {r['played']:,}  not candidates {r['missing']:,}")
    if len(evals) < 2:
        return
    from repertoire_stats import opening_names
    names = opening_names()
    labels = list(evals)
    cand = [h for h in set().union(*(e["W"] for e in evals.values()))
            if g.ply.get(h, 99) <= 6
            and max(e["W"].get(h, 0.0) for e in evals.values()) >= 0.01]
    cand.sort(key=lambda h: -max(e["W"].get(h, 0.0) for e in evals.values()))
    log("  breakdown (ply <= 6, reach >= 1%): reach / value per book")
    log("    " + f"{'ply':>3} " + "".join(f"{lb[:17]:>18}" for lb in labels) + "  opening")
    for h in cand[:40]:
        cells = ""
        for lb in labels:
            w, t = evals[lb]["W"].get(h, 0.0), evals[lb]["T"].get(h)
            cells += f"{w * 100:7.2f}% {t:.4f} " if (w > 0 and t is not None) \
                else f"{'':>18}"
        nm = names.get(h, ("", ""))[1][:48]
        log(f"    {g.ply.get(h, -1):>3} {cells} {nm or g.epd.get(h, '')[:48]}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rep", required=True, help="Unconstrained source rep parquet.")
    ap.add_argument("--stats", required=True)
    ap.add_argument("--perspective", choices=["white", "black"], required=True)
    ap.add_argument("--out-prefix", required=True,
                    help="Books land at <prefix>_b<N>.parquet (+ .meta.json).")
    ap.add_argument("--budget", type=int, action="append", required=True)
    ap.add_argument("--method", choices=["greedy", "dp", "price"], default="dp",
                    help="dp: the curve engine. price: price per move "
                         "(budget_core.price_search) -- exact for its own book size "
                         "on a tree, no curves, so --grid-cap/--match-*/--leftover do "
                         "not apply; the book lands on the reachable size nearest "
                         "--budget and the meta records the bound on the rest.")
    ap.add_argument("--fixed-policy", action="store_true",
                    help="Restrict candidates to the source rep's best_move "
                         "(optimal stopping; the B-prime comparator).")
    ap.add_argument("--epsilon", type=float, default=None,
                    help="Optimistic-reach floor. Default min(1e-3, 8/max(budget)).")
    ap.add_argument("--max-ply", type=int, default=40)
    ap.add_argument("--share-floor", type=float, default=0.002)
    ap.add_argument("--min-games", type=int, default=50)
    ap.add_argument("--max-cands", type=int, default=0,
                    help="Cap on candidates per our-node. DEFAULT 0 (uncapped) "
                         "as of 2026-08-29: the gates are the pruner. This "
                         "narrows the CHOICE SET only -- it does NOT shrink the "
                         "subgraph, because collect_subgraph_edges expands "
                         "before build_graph caps, so a cap buys no memory and "
                         "costs value. Measured: gates alone leave a mean of "
                         "2.66 candidates/node (62%% of our-nodes have exactly "
                         "1); --max-cands 3 binds on 19.5%% of nodes and lost "
                         "0.0043 at b=6 on the <=2024 white pool. When set, "
                         "keeps the source book's move, the top-K by child "
                         "eval, and the single most forcing candidate.")
    ap.add_argument("--match-distinct", action="store_true",
                    help="Spend a LARGER path-charged budget until the book "
                         "holds --budget DISTINCT decisions. The curves charge "
                         "a transposed position once per path but you only "
                         "memorise it once, so without this a b=400 DP book "
                         "holds 305 moves while the truncation baseline holds "
                         "400 -- an unequal-footprint comparison biased against "
                         "the DP. Costs extra extractions, not a rebuild.")
    ap.add_argument("--match-inflate", type=float, default=1.6,
                    help="Curve headroom when --match-distinct is on: curves "
                         "are built to ceil(max budget * this) so the search "
                         "has room above the target. Measured gaps are 15-24%%, "
                         "so 1.6 brackets them with margin. Above --grid-cap "
                         "atoms this costs no extra memory.")
    ap.add_argument("--grid-cap", type=int, default=256)
    ap.add_argument("--aux-stats", default=None,
                    help="Default: read from --rep's .meta.json.")
    ap.add_argument("--slice-prior", type=float, default=None,
                    help="Default: computed from the stats root edges.")
    ap.add_argument("--eval-weight", type=float, default=None,
                    help="Engine-eval share of every leaf/stop value. Default: the "
                         "SOURCE rep's eval_weight from its .meta.json (0.75 for the "
                         "2026-10-17 canonical pair), else 0.5. Mirroring matters: a "
                         "budget book valued at another weight optimises a different "
                         "objective from the book it is cut from. 0 = purely empirical value: "
                         "every number becomes a smoothed observed score. Note "
                         "the eval is still used for GATING which candidates "
                         "exist — that is a separate mechanism from value.")
    ap.add_argument("--prior-strength", type=float, default=bc.PRIOR_STRENGTH,
                    help="Beta-binomial pseudocount pulling thin empirical "
                         "scores toward the slice prior (default 500). 0 = raw "
                         "observed scores, unsmoothed.")
    ap.add_argument("--stop-model", choices=["pool", "weak"], default="pool",
                    help="How a position out of book is valued. 'pool' (default): the "
                         "pool's own results from there (we play on like the pool). "
                         "'weak': we play one move like --weak-pool's population, the "
                         "opponent replies like the pool, then the engine "
                         "(python/weak_stop.py); applied to every out-of-book position "
                         "so the DP compares like with like.")
    ap.add_argument("--weak-pool", default=None,
                    help="Pool parquet whose move frequencies stand for OUR play out of "
                         "book under --stop-model weak (e.g. the 1000-1800 pool).")
    ap.add_argument("--weak-min-games", type=float, default=20.0,
                    help="Below this many weak-pool games at a position, our move "
                         "frequencies fall back to the main pool's (default 20).")
    ap.add_argument("--price-rounds", type=int, default=2,
                    help="--method price: reach re-pricing rounds (default 2).")
    ap.add_argument("--price-select", choices=["closest", "best"], default="closest",
                    help="--method price: the last round's book nearest the target "
                         "(default), or the best realized value of every solve within "
                         "1%% of the target.")
    ap.add_argument("--price-seed", choices=["optimistic", "source"], default="optimistic",
                    help="--method price: price with the optimistic reach (default) or, "
                         "in every round, with max(optimistic, the source rep's policy reach).")
    ap.add_argument("--evaluate-after", action="append", default=[], metavar="LABEL=PATH",
                    help="After building the books, evaluate them in-model on the same graph "
                         "next to these books (as --evaluate does, without a second graph build).")
    ap.add_argument("--price-reprice", choices=["replace", "max"], default="replace",
                    help="--method price: after round 1, price a node at its book's reach "
                         "(replace, default) or at max(book reach, optimistic reach).")
    ap.add_argument("--price-diagnose", action="store_true",
                    help="DIAGNOSTIC: run a fixed set of price-search variants on this "
                         "graph and log each one's size and in-model value (plus any "
                         "--evaluate books); writes no books.")
    ap.add_argument("--allowed-moves", default=None, metavar="PATH",
                    help="Mix and match: our book may play only the moves this parquet "
                         "lists for a position (course_books' course_moves.parquet, or "
                         "any position_hash/parent_hash + move_san table). Elsewhere the "
                         "book can only stop. Default off.")
    ap.add_argument("--allowed-min-score", type=float, default=0.0,
                    help="course_moves only: keep moves scoring above this "
                         "(default 0: any recommendation, never a '?' move).")
    ap.add_argument("--allowed-book-only", action="store_true",
                    help="course_moves only: keep each course book's own choice at a "
                         "position (in_book), not every move its analysis recommends.")
    ap.add_argument("--allowed-courses", default=None,
                    help="course_moves only: comma-separated course_ids to draw from "
                         "(default: every course of the book's side).")
    ap.add_argument("--weak-eval-curve", default=None, metavar="PATH",
                    help="Under --stop-model weak, turn evals into expected scores with "
                         "the curves fit_eval_curve.py fitted per rating pairing "
                         "(eval_curves_pooled.parquet) instead of the uniform Lichess "
                         "curve. Default off.")
    ap.add_argument("--weak-curve-cells",
                    default="stm1000-1799_vs_opp1800+,stm1800+_vs_opp1000-1799",
                    help="US_CELL,THEM_CELL of --weak-eval-curve: the curve where we are "
                         "to move, and where they are.")
    ap.add_argument("--weak-min-share", type=float, default=0.01,
                    help="Drop moves/replies under this share and renormalise.")
    ap.add_argument("--evaluate", action="append", default=[], metavar="LABEL=PATH",
                    help="DIAGNOSTIC: build the graph exactly as for a book, then print the "
                         "in-model value of FOLLOWING each given book on it "
                         "(budget_core.policy_value) and exit without writing books.")
    ap.add_argument("--leftover", action="store_true",
                    help="Extraction: give an opponent node's unspendable budget to the "
                         "closed child with the densest blocking atom instead of "
                         "dropping it (budget_core.extract_book). Default off.")
    ap.add_argument("--gated-reach", action="store_true",
                    help="Expand the subgraph (and compute optimistic reach) only "
                         "through moves the gates admit at our nodes, instead of "
                         "every recorded move. The gates leave ~2.7 candidates per "
                         "node out of many more recorded moves, so this is the "
                         "subgraph-size lever for larger budgets. A position reachable "
                         "only through a refuted move gets no reach. Default off.")
    ap.add_argument("--max-eval-loss", type=float, default=None, metavar="CP",
                    help="Soundness gate: at our nodes book only moves whose engine eval "
                         "is within CP centipawns of the best play there (the best "
                         "evaluated candidate, or the position's own eval if higher), "
                         "the source book's move included (budget_core.eval_loss_keep). "
                         "The Stage-3 gates work in expected score and admit e.g. the "
                         "Blackmar-Diemer (-0.64) at the root. Default off.")
    ap.add_argument("--no-gates", action="store_true",
                    help="Drop require-eval and both refutation gates, so every "
                         "recorded move is a candidate. Diagnostic only.")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--mem", default="24GB")
    ap.add_argument("--tmp-dir", default="D:/chess_duckdb_tmp")
    ap.add_argument("--scan-distinct", default="",
                    help="DIAGNOSTIC 'LO:HI'. Build the curves, then print the "
                         "distinct decision count for every charged budget in "
                         "[LO, HI] and exit WITHOUT writing books. This is the "
                         "measurement that says whether a footprint target is "
                         "reachable at all or whether the distinct count skips "
                         "it, and the one that sizes match_distinct's scan_cap.")
    ap.add_argument("--force", action="store_true")
    a = ap.parse_args()

    t0 = time.time()
    our_white = a.perspective == "white"
    budgets = sorted(set(a.budget))
    bmax = max(budgets)
    # --match-distinct spends ABOVE the requested budget, so the curves need
    # headroom above it or the search has nowhere to go.
    curve_bmax = (int(math.ceil(bmax * a.match_inflate)) if a.match_distinct
                  else bmax)
    eps = a.epsilon if a.epsilon is not None else min(1e-3, 8.0 / bmax)

    # source meta drives the defaults that must MIRROR the source build
    meta_path = Path(a.rep + ".meta.json")
    src_meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    aux_path = a.aux_stats or src_meta.get("aux_stats")
    reply_shrink = float(src_meta.get("reply_shrink", 0.0) or 0.0)
    if reply_shrink > 0 and not aux_path:
        log("FATAL: source rep used reply_shrink>0 without aux; the budget "
            "DP does not implement that blend (see header). Refusing.")
        return 1
    # The other-moves bucket's coverage rule is part of the objective, so it must
    # match the Stage-3 build that produced the source rep (aux_opp_mix, shared).
    other_cov_blend = bool(src_meta.get("other_cov_blend", False))
    # Same reasoning as other_cov_blend: mirror the source build's eval weight unless
    # overridden. Before 2026-10-17 this defaulted to a constant 0.5, which matched the
    # recipe until the canonical pair moved to 0.75 and would then have silently valued
    # every budget book at a different weight from its source.
    if a.eval_weight is None:
        a.eval_weight = float(src_meta.get("eval_weight", bc.EVAL_WEIGHT))
    log(f"eval weight {a.eval_weight:g} ({'source meta' if 'eval_weight' in src_meta else 'default'})")
    if other_cov_blend:
        log("other-moves bucket: coverage-scaled engine blend (source rep's "
            "--other-cov-blend)")

    outs = {b: Path(f"{a.out_prefix}_b{b}.parquet") for b in budgets}
    todo = {b: p for b, p in outs.items() if a.force or not p.exists()}
    if not todo:
        log("all outputs exist; --force to rebuild")
        return 0
    outs[budgets[0]].parent.mkdir(parents=True, exist_ok=True)

    import chess
    root = zobrist_int64(chess.Board())
    stats = a.stats.replace("\\", "/")
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.mem}'; SET threads={a.threads};")

    slice_prior = a.slice_prior
    if slice_prior is None:
        slice_prior = con.execute(f"""
            SELECT SUM(white_score_avg * total) / SUM(total)
            FROM read_parquet('{stats}') WHERE parent_hash = {root}
        """).fetchone()[0] or 0.5
    log(f"slice prior {slice_prior:.4f}; eps {eps:g}; budgets {budgets}; "
        f"method {a.method}{' fixed-policy' if a.fixed_policy else ''}")

    admit = None
    if a.gated_reach:
        if a.no_gates:
            log("FATAL: --gated-reach with --no-gates is a no-op; refusing")
            return 1
        from stage3_backwards_induction import load_incumbent
        _sl0 = (pl.scan_parquet(a.rep).select(["event", "elo_band"]).unique().collect())
        src_all = load_incumbent(a.rep, a.perspective, _sl0["event"][0],
                                 _sl0["elo_band"][0])
        log(f"gated reach: {len(src_all):,} source decisions loaded for the gates")
        admit = GateAdmit(src_all, our_white, slice_prior, max_eval_loss=a.max_eval_loss)
    allowed = None
    if a.allowed_moves:
        allowed = load_allowed_moves(
            a.allowed_moves, a.perspective, a.allowed_min_score,
            a.allowed_courses.split(",") if a.allowed_courses else None,
            a.allowed_book_only)
        log(f"allowed moves: {sum(map(len, allowed.values())):,} moves at "
            f"{len(allowed):,} positions from {a.allowed_moves}")
        admit = AllowedAdmit(allowed, admit)
    log("collecting subgraph edges...")
    edf = collect_subgraph_edges(con, stats, root, our_white, eps,
                                 a.max_ply, a.share_floor, a.min_games, admit)
    if not edf.height:
        log("FATAL: empty subgraph")
        return 1
    parents = edf["parent_hash"].unique().to_list()
    log(f"  {edf.height:,} edges, {len(parents):,} parent nodes "
        f"({time.time()-t0:,.0f}s)")

    log("loading source rep (subgraph slice)...")
    want = set(parents) | set(edf["child_hash"].drop_nulls().to_list())
    # Slice identity travels from the source rep. A rep spans exactly one
    # (event, elo_band) in every current build; if that ever stops being true
    # the book would need per-row slices, so fail rather than pick one.
    _sl = (pl.scan_parquet(a.rep).select(["event", "elo_band"])
           .unique().collect())
    if _sl.height != 1:
        log(f"FATAL: --rep spans {_sl.height} (event, elo_band) slices; "
            f"budget books carry a single slice. Slices: {_sl.to_dicts()}")
        return 1
    src_event = _sl["event"][0]
    src_elo_band = _sl["elo_band"][0]
    log(f"slice: event={src_event!r} elo_band={src_elo_band}")

    rep = (pl.scan_parquet(a.rep)
           .filter(pl.col("position_hash").is_in(list(want)))
           .select(["position_hash", "side_to_move", "value", "best_move",
                    "value_robust", "value_worst", "eval_score"])
           .collect())
    rep_moves, rep_rows = {}, {}
    for r in rep.iter_rows(named=True):
        h = r["position_hash"]
        rep_rows[h] = r
        if r["side_to_move"] == a.perspective and r["best_move"]:
            rep_moves[h] = r["best_move"]
    log(f"  {len(rep_moves):,} source decisions in slice")

    log("evals (mmap)...")
    from eval_arrays import MISSING, lookup_evals, open_eval_arrays
    mm_hash, mm_cp = open_eval_arrays()
    keys = np.array(sorted(want), dtype=np.int64)
    cps = lookup_evals(keys, mm_hash, mm_cp)
    eval_ws = {int(h): cp_to_expected_score(int(c))
               for h, c in zip(keys, cps) if c != MISSING}
    log(f"  {len(eval_ws):,} of {len(want):,} positions eval-covered")

    aux_rows = {}
    if aux_path:
        aux_rows = bc.load_aux_rows(aux_path, parents)
        log(f"  aux rows for {len(aux_rows):,} subgraph nodes")

    # Only the fields build_graph reads, and the EPD on a parent's first row only:
    # a full row dict per edge was most of the peak at a few million edges.
    by_parent: dict[int, list[dict]] = {}
    for ph, ms, ch, tot, ws, epd in edf.select(
            ["parent_hash", "move_san", "child_hash", "total", "white_score_avg",
             "parent_epd"]).iter_rows():
        lst = by_parent.get(ph)
        if lst is None:
            by_parent[ph] = [{"move_san": ms, "child_hash": ch, "total": tot,
                              "white_score_avg": ws, "parent_epd": epd}]
        else:
            lst.append({"move_san": ms, "child_hash": ch, "total": tot,
                        "white_score_avg": ws})
    del edf

    log("building graph...")
    g = bc.build_graph(by_parent, root, our_white, rep_moves=rep_moves,
                       eval_ws=eval_ws,
                       aux_rows=aux_rows, slice_prior=slice_prior,
                       eps=eps, max_ply=a.max_ply, share_floor=a.share_floor,
                       max_cands=a.max_cands, eval_weight=a.eval_weight,
                       prior_strength=a.prior_strength,
                       require_eval=not a.no_gates, gates=not a.no_gates,
                       other_cov_blend=other_cov_blend,
                       gated_reach=a.gated_reach,
                       max_eval_loss=a.max_eval_loss)
    log(f"  {len(g.our):,} our nodes, {len(g.opp):,} opponent nodes")
    allowed_counts = None
    if allowed is not None:
        allowed_counts = restrict_candidates(g, allowed)
        log(f"  allowed moves: {allowed_counts}")
        del allowed
    # nothing below reads the edge lists, the eval cache or the gate's source moves
    del by_parent, eval_ws, want, admit
    import gc
    gc.collect()
    stop_counts = None
    if a.stop_model == "weak":
        if not a.weak_pool or not Path(a.weak_pool).exists():
            log(f"FATAL: --stop-model weak needs --weak-pool (got {a.weak_pool})")
            return 1
        from weak_stop import EvalSource, load_curves, weak_stop_values
        curves = None
        if a.weak_eval_curve:
            curves = load_curves(a.weak_eval_curve, *a.weak_curve_cells.split(","))
            log(f"  weak stop model: fitted eval curves {curves}")
        xs, ys = bc.stop_model_positions(g)
        W, R, wc = weak_stop_values(con, xs, ys, pool=stats, weak_pool=a.weak_pool,
                                    evals=EvalSource(our_white, curves=curves),
                                    min_weak=a.weak_min_games,
                                    min_share=a.weak_min_share, log=log)
        ac = bc.apply_stop_values(g, W, R)
        stop_counts = {**dict(wc), **{f"applied_{k}": v for k, v in ac.items()}}
        log(f"  weak stop model: {stop_counts}  ({time.time()-t0:,.0f}s)")
        del W, R

    if a.price_diagnose:
        order, _cut = bc.topo_up(g)
        src_reach = dict(g.reach)
        src_reach.update({h: max(r, src_reach.get(h, 0.0))
                          for h, r in bc.policy_reach(g, rep_moves, order).items() if r > 0})
        target = budgets[0]
        log(f"price diagnose at {target}: after each round, the closest book and the "
            f"best within 1% of the target so far")
        for seed, rp in (("optimistic", "replace"), ("source", "replace"),
                         ("optimistic", "max")):
            t1 = time.time()

            def show(rnd, cl, best, seed=f"{seed}/{rp}", t1=t1):
                b_ = (f"{best['spent_distinct']:,} at {best['root_value_realized']:.5f}"
                      if best else "none")
                log(f"  seed {seed:<18} round {rnd}: closest {cl['spent_distinct']:,} at "
                    f"{cl['root_value_realized']:.5f}; best {b_}  ({time.time() - t1:,.0f}s)")
            bc.price_search(g, target, log=lambda *_: None, rounds=4, select="best",
                            seed_reach=src_reach if seed == "source" else None,
                            on_round=show, reprice=rp)
    if a.evaluate:
        evaluate_books(g, a.evaluate, a.perspective, log,
                       f"stop model {a.stop_model}, eps {eps:g}"
                       f"{', gated' if a.gated_reach else ''}")
    if a.evaluate or a.price_diagnose:
        return 0
    fixed = rep_moves if (a.fixed_policy or a.method == "greedy") else None
    if a.method == "price":
        curves, diag = {}, {"stranded_cycle_nodes": None}
    else:
        log("building curves...")
        curves, diag = bc.build_curves(g, bmax=bmax, k_atoms=a.grid_cap,
                                       fixed_policy=fixed)
        log(f"  stranded cycle nodes {diag['stranded_cycle_nodes']:,}  "
            f"({time.time()-t0:,.0f}s)")

    # SECOND curve set, for --match-distinct probes ONLY.
    #
    # curve_bmax used to be the single build cap, which made the flag change
    # answers at budgets far BELOW it and broke its own no-op contract
    # (2026-08-31: dp_black_b2 went 9 -> 24 rows with charged=2, i.e. via the
    # early exit, having never probed). _node_curve truncates its point set at
    # `b > bmax` and only then takes a CONCAVE MAJORANT, and a hull is global:
    # admitting one far high point makes the merge cascade fuse every increment
    # before it into one expensive atom, which the extraction cannot buy at all.
    # Demonstrated on the pure functions in scratch/python/match_distinct_repro.py
    # -- 14 unit atoms collapse to a single cost-24 atom and every budget 1..20
    # drops to base.
    #
    # Inflating can only ever coarsen a hull, so it is weakly HARMFUL below the
    # cap and must not be imposed on books that never needed the headroom. Keep
    # the plain curves authoritative -- extraction at the requested budget is
    # then bit-identical with and without the flag, which is what the
    # verification asserts -- and reach for the inflated set only when the
    # search genuinely has to probe higher, where the book differs anyway.
    probe_curves = None
    if a.match_distinct and curve_bmax > bmax and a.method != "price":
        log(f"building probe curves (bmax {curve_bmax}) for --match-distinct...")
        probe_curves, _ = bc.build_curves(g, bmax=curve_bmax,
                                          k_atoms=a.grid_cap,
                                          fixed_policy=fixed)
        log(f"  probe curves ready  ({time.time()-t0:,.0f}s)")

    # Diagnostics read the WIDEST set: both are trimmed at their own bmax, so
    # taking capacity or a value-at-charged off the plain curves would silently
    # report the cap rather than the quantity.
    widest = probe_curves if probe_curves is not None else curves
    cap = widest[g.root].capacity if g.root in widest else 0
    if a.method != "price":
        log(f"  root capacity {cap:,} (path-sum)")

    # Diagnostic: the distinct-vs-charged curve itself. --match-distinct's
    # search assumes it can find a charged budget landing exactly on a target,
    # and when it cannot the honest question is whether the count SKIPS that
    # target or the search merely failed to look. Guessing at that from the
    # outside cost two wrong hypotheses on 2026-09-08; this prints the answer.
    # Writes nothing and returns before the book loop.
    if a.scan_distinct:
        lo_s, hi_s = (int(x) for x in a.scan_distinct.split(":"))
        log(f"scan-distinct {lo_s}..{hi_s} on {'plain' if hi_s <= bmax else 'plain+probe'} curves")
        log(f"  {'charged':>9}{'distinct':>10}{'paths':>8}{'root value':>14}  curves")
        prev = None
        for bb in range(lo_s, hi_s + 1):
            cs = curves if bb <= bmax else widest
            r = bc.extract_book(g, cs, bb, fixed_policy=fixed, leftover=a.leftover)
            d = r["spent_distinct"]
            mark = "" if prev is None or d == prev else f"   <- +{d - prev}"
            log(f"  {bb:>9}{d:>10}{r['spent_paths']:>8}"
                f"{r['root_value_realized']:>14.6f}  "
                f"{'plain' if bb <= bmax else 'probe'}{mark}")
            prev = d
        return 0

    for b in budgets:
        out = outs[b]
        if b not in todo:
            log(f"skip b={b} (exists)")
            continue
        price_info = None
        if a.method == "price":
            seed = None
            if a.price_seed == "source":
                # never dearer than the optimistic reach; the source book's policy reach
                # (summed over its paths) where that is larger
                seed = dict(g.reach)
                seed.update({h: max(r, seed.get(h, 0.0)) for h, r in bc.policy_reach(
                    g, rep_moves, bc.topo_up(g)[0]).items() if r > 0})
            res, price_info = bc.price_search(g, b, fixed_policy=fixed, log=log,
                                              rounds=a.price_rounds,
                                              select=a.price_select, seed_reach=seed,
                                              reprice=a.price_reprice)
            charged, matched = b, res["spent_distinct"] >= b
            log(f"  ({time.time()-t0:,.0f}s)")
        elif a.method == "greedy":
            # Myopic one-step greedy picks the booked SET; the masked curves
            # then supply the VALUES only -- force_booked stops the density
            # allocator re-deciding the set. Without it (pre-2026-08-29) greedy
            # selected 6 nodes of positive one-step gain and only 3 were booked,
            # every miss from receiving alloc 0 and none from a value test: a
            # 14-move book answering a 20-move budget, which measures size
            # rather than method and voids the equal-footprint comparison.
            booked, _spent = bc.greedy_stopping(g, rep_moves, b)
            mask = {h: rep_moves[h] for h in booked}
            gcurves, _ = bc.build_curves(g, bmax=b, k_atoms=a.grid_cap,
                                         fixed_policy=mask)
            res = bc.extract_book(g, gcurves, b, fixed_policy=mask,
                                  force_booked=booked)
            charged, matched = b, res["spent_distinct"] >= b
        elif a.match_distinct:
            res, charged, matched, spent = bc.match_distinct(
                g, curves, b, curve_bmax, fixed_policy=fixed,
                probe_curves=probe_curves, plain_bmax=bmax, leftover=a.leftover)
            # matched is `spent >= b`; the equal-footprint comparison needs
            # `spent == b`, and the two came apart in the wild (fixdp_white_b20
            # booked 21 for a target of 20 and reported success). Say which.
            exact = spent == b
            log(f"  match-distinct b={b}: charged {charged} for "
                f"{spent} distinct"
                + ("" if matched else "  (TARGET NOT REACHED — capacity)")
                + ("" if exact or not matched
                   else f"  (OVERSHOT by {spent - b}: the distinct count skips "
                        f"{b}; footprint audits at tol 0 will reject this)"))
        else:
            res = bc.extract_book(g, curves, b, fixed_policy=fixed,
                                  leftover=a.leftover)
            charged, matched = b, res["spent_distinct"] >= b

        rows = []
        flip = (lambda v: v) if our_white else (lambda v: None if v is None
                                                else 1.0 - v)
        for h in (res["visited"] or []):
            # side_to_move is absolute: the root is always White to move, so
            # even ply = white regardless of which book is being built.
            side = "white" if (g.ply[h] % 2 == 0) else "black"
            src = rep_rows.get(h, {})
            rows.append({
                "position_hash": h,
                "position_epd": g.epd.get(h) or src.get("position_epd"),
                # The slice these books belong to, inherited from --rep. Every
                # downstream reader keys on it: repertoire_explorer refuses to
                # load a rep without it (and its whole per-slice view cache is
                # built on it), and score_repertoire selects by it. Omitting
                # these silently produced books the explorer could not open at
                # all, which is not the "replay/score-compatible" this script's
                # header claims.
                "event": src_event,
                "elo_band": src_elo_band,
                "side_to_move": side,
                "value": flip(res["realized"].get(h)),
                "best_move": res["booked"].get(h),
                "value_robust": src.get("value_robust"),
                "value_worst": src.get("value_worst"),
                "eval_score": src.get("eval_score"),
                "value_unconstrained": src.get("value"),
                "alloc_budget": res["alloc"].get(h, 0),
            })
        tmp = out.with_suffix(".parquet.tmp")
        pl.DataFrame(rows).write_parquet(tmp, compression="zstd")
        tmp.replace(out)
        meta = {
            "budget_requested": b,
            "spent_paths": res["spent_paths"],
            "spent_distinct": res["spent_distinct"],
            "method": a.method + ("+fixed" if a.fixed_policy else ""),
            "epsilon": eps, "max_ply": a.max_ply,
            "share_floor": a.share_floor, "grid_cap": a.grid_cap,
            "match_distinct": bool(a.match_distinct),
            "budget_charged": charged,
            # target_met is `spent >= budget`; exact is `spent == budget`. Only
            # the latter licenses an equal-footprint comparison, and they came
            # apart in the wild -- fixdp_white_b20 booked 21 for a target of 20
            # and recorded target_met=True, which a reader would take as matched.
            "distinct_target_met": bool(matched),
            "distinct_exact": bool(res["spent_distinct"] == b),
            "root_value_curve": None if g.root not in widest
            else widest[g.root].eval(charged),
            "root_value_realized": res["root_value_realized"],
            "root_capacity_paths": cap,
            "stranded_cycle_nodes": diag["stranded_cycle_nodes"],
            "slice_prior": slice_prior, "eval_weight": a.eval_weight,
            "prior_strength": a.prior_strength, "gates": not a.no_gates,
            "max_cands": a.max_cands, "gated_reach": a.gated_reach,
            "max_eval_loss": a.max_eval_loss,
            "leftover": a.leftover,
            "stop_model": a.stop_model,
            **({"price_rounds": a.price_rounds, "price_select": a.price_select,
                "price_seed": a.price_seed, "price_reprice": a.price_reprice}
               if a.method == "price" else {}),
            **({"allowed_moves": a.allowed_moves, "allowed_min_score": a.allowed_min_score,
                "allowed_courses": a.allowed_courses,
                "allowed_book_only": a.allowed_book_only, "allowed_counts": allowed_counts}
               if a.allowed_moves else {}),
            **({"price": price_info} if price_info is not None else {}),
            **({"weak_pool": a.weak_pool, "weak_min_games": a.weak_min_games,
                "weak_min_share": a.weak_min_share, "weak_counts": stop_counts,
                **({"weak_eval_curve": a.weak_eval_curve,
                    "weak_curve_cells": a.weak_curve_cells,
                    "weak_curves": curves}
                   if a.weak_eval_curve else {})}
               if a.stop_model == "weak" else {}),
            "source_rep": str(a.rep), "source_meta": src_meta,
            "stats": str(a.stats), "aux_stats": aux_path,
            "subgraph_nodes": len(g.our) + len(g.opp),
            "subgraph_our_nodes": len(g.our),
            "peak_commit_gb": peak_commit_gb(),
            "built": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        Path(str(out) + ".meta.json").write_text(json.dumps(meta, indent=2))
        rvr, rvc = res["root_value_realized"], meta["root_value_curve"]
        log(f"b={b}: booked {res['spent_distinct']:,} distinct "
            f"({res['spent_paths']:,} path-charged), root value "
            f"{rvr:.4f}" if rvr is not None else
            f"b={b}: booked {res['spent_distinct']:,} distinct (root value n/a)"
            f" -> {out.name}")
        if rvr is not None and rvc is not None:
            log(f"       curve {rvc:.4f} -> {out.name}")
        if res["spent_distinct"] == 0:
            log("       WARNING: nothing booked — stopping beat every "
                "candidate at every node (check L_node vs continuation)")

    if a.evaluate_after:
        evaluate_books(g, [f"this_b{b}={outs[b]}" for b in budgets if outs[b].exists()]
                       + a.evaluate_after, a.perspective, log,
                       f"stop model {a.stop_model}, eps {eps:g}"
                       f"{', gated' if a.gated_reach else ''}")
    pk = peak_commit_gb()
    log(f"\ndone in {time.time()-t0:,.0f}s"
        + (f"; peak commit {pk:,.1f} GB" if pk is not None else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
