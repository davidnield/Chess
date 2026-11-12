"""Build budget-constrained repertoire books from an unconstrained Stage-3 rep.

Chooses, under a hard budget of our-turn decisions ("moves to learn", counted
per build_baseline_books.py's footprint definition), which positions to book
AND which move to book at each — the budget is inside the selection, so a
400-move book may play different variations (early payoff) than a 40,000-move
book (which can afford deep traps). Method: per-node value-vs-budget curves
(budget_core.py, where the algorithm and its documented approximations live).

Two optimisers (2026-11-12; the greedy, fixed-policy and price-variant paths were retired and
live in tag archive-2026-11-11):
  --method price     price search, one repricing round at max(book, optimistic) reach from the
                     source book's seed, best solve, then DAG polish (every White recipe)
  --method dp        the curve engine, joint stopping + policy choice; Black recipes add
                     --match-distinct --match-inflate 1.5 --polish
Out-of-book positions are always valued by the weak-player stop model (--weak-pool), optionally
replaced by sampled tables (--sampled-stop / --sampled-other); strict source and crack at parents
are always on.

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
        --weak-pool E:/chess/position-stats/weak_edges_b1000_1800_on_ge1800.parquet \
        --budget 2000 --method price

Defaults for --aux-stats / --reply-shrink are read from the source rep's
.meta.json sidecar; explicit flags override.
reply_shrink > 0 without aux is NOT implemented (every current book is
aux + reply_shrink 0) — the build refuses rather than silently diverging.
"""
from __future__ import annotations

import argparse
import hashlib
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
from duck_compat import duck_pl                             # noqa: E402
from stage3_backwards_induction import cp_to_expected_score  # noqa: E402
from zobrist import zobrist_int64                            # noqa: E402


# Fixed since 2026-11-12 (options retired; tag archive-2026-11-11 keeps the old paths): the price
# search as every price recipe ran it, and the weak stop model's minimum move share.
PRICE = {"rounds": 1, "select": "best", "reprice": "max", "dag_mode": "polish"}
WEAK_MIN_SHARE = 0.01


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


def book_frame(rows: list[dict]) -> pl.DataFrame:
    """The book's rows as a frame. Every row counts for the schema: loss_exception is null on
    nearly all of them, and a label past polars' default 100-row inference raises, at the very
    end of a long build. It stays a string column in a book without exceptions."""
    df = pl.DataFrame(rows, infer_schema_length=None)
    return df.with_columns(pl.col("loss_exception").cast(pl.Utf8)) if "loss_exception" in df.columns else df


def _pool_min_games(stats: str) -> int | None:
    """The pool's own min_games floor, from its <stats>.meta.json (None if absent)."""
    mp = Path(str(stats) + ".meta.json")
    if not mp.exists():
        return None
    v = json.loads(mp.read_text()).get("min_games")
    return int(v) if v is not None else None


class GateAdmit:
    """--gated-reach for the collector: which of an our-node's recorded moves the
    gates admit (budget_core.gate_candidates, the definition build_graph uses).
    Evals come from the mmap'd arrays, prefetched a level at a time; the cache is
    the eval_ws the graph is built from afterwards."""

    def __init__(self, rep_moves, our_white: bool, slice_prior: float,
                 gates: bool = True, max_eval_loss: float | None = None, **gate_settings):
        from eval_arrays import MISSING, lookup_evals, open_eval_arrays
        self._lookup, self._missing = lookup_evals, MISSING
        self._mm = open_eval_arrays()
        self.rep_moves, self.our_white = rep_moves, our_white
        self.slice_prior, self.gates = slice_prior, gates
        self.max_eval_loss = max_eval_loss
        self.gate_settings = gate_settings
        self.eval_ws: dict[int, float] = {}
        self._seen: set[int] = set()
        self.excluded = {}
        self.rare_floor, self.verified = None, {}

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
        if ph in self.excluded:
            rows = [r for r in rows if r["move_san"].rstrip("+#") not in self.excluded[ph]]
        return {e["child_hash"] for e, _ in bc.gate_candidates(
            ph, rows, src, self.eval_ws, self.our_white, self.slice_prior,
            require_eval=self.gates, gates=self.gates, max_eval_loss=self.max_eval_loss,
            rare_floor=self.rare_floor, verified=self.verified,
            **self.gate_settings)
            if e.get("child_hash") is not None}


def load_incumbent_moves(path: str, perspective: str) -> dict[int, str]:
    """--incumbent: position -> the previous book's move at our turn (check marks stripped)."""
    d = pl.read_parquet(path, columns=["position_hash", "side_to_move", "best_move"])
    return {h: m.rstrip("+#") for h, s, m in d.iter_rows() if s == perspective and m}


def incumbent_churn(booked: dict[int, str], incumbent: dict[int, str],
                    reach: dict[int, float]) -> dict:
    """How far the new book moved from the incumbent: decisions kept, changed (same position,
    different move), dropped (the incumbent books it, the new book does not) and added, by
    count and by optimistic reach."""
    new = {h: m.rstrip("+#") for h, m in booked.items()}
    both = new.keys() & incumbent.keys()
    changed = [h for h in both if new[h] != incumbent[h]]
    dropped = incumbent.keys() - new.keys()
    added = new.keys() - incumbent.keys()

    def r(hs):
        return float(sum(reach.get(h, 0.0) for h in hs))
    return {"kept": len(both) - len(changed), "changed": len(changed),
            "dropped": len(dropped), "added": len(added),
            "reach_kept": r(set(both) - set(changed)), "reach_changed": r(changed),
            "reach_dropped": r(dropped), "reach_added": r(added)}


def collect_subgraph_edges(con, stats: str, root: int, our_white: bool,
                           eps: float, max_ply: int, share_floor: float,
                           min_games: int, admit=None, engine_moves=None, reply_overlay=None,
                           uncut_own: bool = False) -> pl.DataFrame:
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
    floor = f"s.total >= {int(min_games)}"
    if uncut_own:
        our_turn = "w" if our_white else "b"
        floor += f" OR split_part(s.parent_epd, ' ', 2) = '{our_turn}'"
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
        df = duck_pl(con, f"""
            SELECT s.parent_hash, s.move_san, s.child_hash, s.parent_epd,
                   s.total, s.white_wins, s.draws, s.black_wins,
                   s.white_score_avg
            FROM read_parquet('{stats}') s
            JOIN frontier f ON f.h = s.parent_hash
            WHERE {floor}
        """)
        # A fixed order (2026-10-29): DuckDB's join order and polars' group_by order
        # vary run to run, and the walk below is order-dependent -- a transposition's
        # depth is set by whichever parent first gives it its best reach, and depth
        # drives the max_ply cut and the turn parity -- so two builds of one input
        # differed by a few positions. Rows also reach build_graph in this order.
        if reply_overlay is not None:
            df = reply_overlay.attach(df, frontier)
        df = df.sort("parent_hash", "move_san", "child_hash", nulls_last=True)
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
            for ph, grp in df.group_by("parent_hash", maintain_order=True):
                ph = ph[0] if isinstance(ph, tuple) else ph
                r = reach.get(ph, 0.0)
                p_ply = ply.get(ph, 0)
                if p_ply >= max_ply:
                    continue
                tot = int(grp["total"].sum())
                our_turn = (p_ply % 2 == 0) == our_white
                rows = list(grp.iter_rows(named=True))
                if our_turn and engine_moves and ph in engine_moves:
                    existing = {r["child_hash"] for r in rows}
                    rows.extend({"move_san": san, "child_hash": ch, "total": 0, "engine": True}
                                for san, ch in engine_moves[ph] if ch not in existing)
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
                        share = bc.reply_share(row,tot)
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rep", required=True, help="Unconstrained source rep parquet.")
    ap.add_argument("--stats", required=True)
    ap.add_argument("--opponent-prior", help="Frozen named replies and Maia probabilities at opponent positions")
    ap.add_argument("--opponent-pseudogames", type=float, default=0.0)
    ap.add_argument("--perspective", choices=["white", "black"], required=True)
    ap.add_argument("--out-prefix", required=True,
                    help="Books land at <prefix>_b<N>.parquet (+ .meta.json).")
    ap.add_argument("--budget", type=int, action="append", required=True)
    ap.add_argument("--method", choices=["dp", "price"], default="dp",
                    help="dp: the curve engine. price: price per move "
                         "(budget_core.price_search) -- exact for its own book size "
                         "on a tree, no curves, so --grid-cap/--match-*/--leftover do "
                         "not apply; the book lands on the reachable size nearest "
                         "--budget and the meta records the bound on the rest.")
    ap.add_argument("--epsilon", type=float, default=None,
                    help="Optimistic-reach floor. Default min(1e-3, 8/max(budget)).")
    ap.add_argument("--max-ply", type=int, default=40)
    ap.add_argument("--share-floor", type=float, default=0.002)
    ap.add_argument("--min-games", type=int, default=None,
                    help="Game floor for the edges the subgraph collector reads. Default "
                         "(2026-10-31): the pool's own floor from <stats>.meta.json, else 50. "
                         "A floor ABOVE the pool's drops those edges from the reply mix AND "
                         "from the aux sidecar (whose OTHER bucket holds only replies under "
                         "the pool's floor), so the survivors are renormalised over a smaller "
                         "denominator: the Black lifetime book lost its 20-49-game replies "
                         "that way. Warned about, recorded in the meta.")
    ap.add_argument("--dump-stop-positions", default=None, metavar="PARQUET",
                    help="write every stop position of the graph (ply, reach, weak-stop value) "
                         "and stop: the input of sampled_stop.py (2026-10-31)")
    ap.add_argument("--sampled-stop", default=None, metavar="PARQUET",
                    help="replace the weak-stop values by the referee's, sampled per (position, "
                         "ply) by sampled_stop.py and calibrated/shrunk there (owner's decision on "
                         "the follow-up review's R4, 2026-10-31). Needs --stop-model weak.")
    ap.add_argument("--dump-other-positions", default=None, metavar="PARQUET",
                    help="write every opponent node's other bucket (share, value, pool children) "
                         "and stop: the input of sampled_other.py (2026-11-02)")
    ap.add_argument("--sampled-other", default=None, metavar="PARQUET",
                    help="put the other bucket on the referee's scale, sampled and calibrated by "
                         "sampled_other.py (2026-11-02). Default off.")
    ap.add_argument("--dump-engine-targets", metavar="PARQUET",
                    help="export reachable unsound, empty or unevaluated decisions for "
                         "engine_candidates.py before spending the learning budget")
    ap.add_argument("--engine-candidates", default=None, metavar="PARQUET",
                    help="engine-proposed moves to judge alongside the recorded ones "
                         "(engine_candidates.py; follow-up review R5). Default off.")
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
    ap.add_argument("--weak-pool", required=True,
                    help="Pool parquet whose move frequencies stand for OUR play out of "
                         "book under --stop-model weak (e.g. the 1000-1800 pool).")
    ap.add_argument("--weak-min-games", type=float, default=20.0,
                    help="Below this many weak-pool games at a position, our move "
                         "frequencies fall back to the main pool's (default 20).")
    ap.add_argument("--polish", action="store_true",
                    help="--method dp: polish the curve engine's book to the same budget "
                         "(budget_core.polish_book, the coordinate ascent of --price-dag-mode "
                         "polish, started from the DP book): never below the DP book in-model, "
                         "never over the budget. Default off.")
    ap.add_argument("--price-engine", choices=["auto", "compiled", "legacy"], default="auto",
                    help="--method price: how the search computes (budget_core.price_search "
                         "engine). auto (default): array sweeps on a graph compiled once per "
                         "run (budget_compiled.py) from budget_core.PRICE_COMPILED_MIN_NODES "
                         "nodes, the dict walks below; legacy: the dict walks throughout, the "
                         "reference. Every engine writes the same books bit for bit "
                         "(_test_budget_speed.py); legacy exists to check that on real graphs.")
    ap.add_argument("--gated-reach", action="store_true",
                    help="Expand the subgraph (and compute optimistic reach) only "
                         "through moves the gates admit at our nodes, instead of "
                         "every recorded move. The gates leave ~2.7 candidates per "
                         "node out of many more recorded moves, so this is the "
                         "subgraph-size lever for larger budgets. A position reachable "
                         "only through a refuted move gets no reach. Default off.")
    ap.add_argument("--incumbent", default=None, metavar="BOOK",
                    help="Rebuild stickiness: a PREVIOUS book of this colour (e.g. the one being "
                         "learned). At each position its move is kept unless another candidate is "
                         "better by more than --stick-margin (budget_core.incumbent_move); the meta "
                         "records the churn against it. Default off.")
    ap.add_argument("--stick-margin", type=float, default=None,
                    help="Expected-score advantage a new move needs to replace the incumbent's "
                         "(default 0.01 with --incumbent; 0 = no stickiness, churn report only).")
    ap.add_argument("--robustness-floor", type=float, default=bc.ROBUSTNESS_FLOOR,
                    help="Absolute gate tolerance in expected-score units")
    ap.add_argument("--gate-rel-floor", type=float, default=bc.GATE_REL_FLOOR,
                    help="Relative gate tolerance in expected-score units")
    ap.add_argument("--gate-rel-own-margin", type=float, default=bc.GATE_REL_OWN_MARGIN,
                    help="Discount on the position's own eval for the relative gate")
    ap.add_argument("--max-eval-loss", type=float, default=None, metavar="CP",
                    help="Soundness gate: at our nodes book only moves whose engine eval "
                         "is within CP centipawns of the best play there (the best "
                         "evaluated candidate, or the position's own eval if higher), "
                         "the source book's move included (budget_core.eval_loss_keep). "
                         "The Stage-3 gates work in expected score and admit e.g. the "
                         "Blackmar-Diemer (-0.64) at the root. Default off.")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--mem", default="24GB")
    ap.add_argument("--tmp-dir", default="D:/chess_duckdb_tmp")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--rare-verification", action="store_true",
                    help="With --uncut-own-moves: an own move with fewer than --min-games "
                         "observations and no verified score (rare_verify.py rows in "
                         "--engine-candidates) cannot set a gate reference. It stays a "
                         "candidate. Writes <book>.rare.json: the decisions where such a move "
                         "would have excluded a rival. Default off = the old rule exactly.")
    ap.add_argument("--uncut-own-moves", type=Path, metavar="CACHE",
                    help="Use a complete native own-move lookup prepared by uncut_own_moves.py. "
                         "Give --stats and --aux-stats for its original population.")
    a = ap.parse_args()
    gate_settings = {k: getattr(a, k) for k in ("robustness_floor", "gate_rel_floor", "gate_rel_own_margin")}
    if any(not 0 <= v <= 1 for v in gate_settings.values()):
        ap.error("gate tolerances must be between 0 and 1 expected-score units")
    # sampled tables record this; gates and strict_source are fixed True since 2026-11-12
    gate_recipe = dict(**gate_settings, max_eval_loss=a.max_eval_loss, gates=True,
                       gated_reach=a.gated_reach, strict_source=True)
    if a.dump_engine_targets and a.max_eval_loss is None:
        ap.error("--dump-engine-targets requires --max-eval-loss")
    if a.stick_margin is not None and not a.incumbent:
        ap.error("--stick-margin needs --incumbent")
    if a.polish and a.method != "dp":
        ap.error("--polish is for --method dp (--method price always polishes)")
    if a.incumbent and a.stick_margin is None:
        a.stick_margin = 0.01

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
    uncut = None
    if a.uncut_own_moves:
        from uncut_own_moves import load
        if not aux_path:
            ap.error("--uncut-own-moves requires the native aux sidecar")
        uncut = load(a.uncut_own_moves, a.stats, aux_path, a.perspective, a.opponent_prior)
    uncut_recipe = uncut["recipe_sha256"] if uncut else None
    if a.rare_verification and not uncut:
        ap.error("--rare-verification needs --uncut-own-moves")
    for dest in outs.values():
        if dest.exists() and not a.force:
            mp = Path(str(dest) + ".meta.json")
            old = json.loads(mp.read_text()).get("uncut_own_recipe") if mp.exists() else None
            if old != uncut_recipe:
                ap.error("Existing output uses another move lookup; choose a fresh output prefix")
    todo = {b: p for b, p in outs.items() if a.force or not p.exists()}
    if not todo and not a.dump_engine_targets:
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
        f"method {a.method}")

    admit = None
    if a.gated_reach:
        from stage3_backwards_induction import load_incumbent
        _sl0 = (pl.scan_parquet(a.rep).select(["event", "elo_band"]).unique().collect())
        src_all = load_incumbent(a.rep, a.perspective, _sl0["event"][0],
                                 _sl0["elo_band"][0])
        log(f"gated reach: {len(src_all):,} source decisions loaded for the gates")
        admit = GateAdmit(src_all, our_white, slice_prior, max_eval_loss=a.max_eval_loss, **gate_settings)
    pool_floor = _pool_min_games(stats)
    if a.min_games is None:
        a.min_games = pool_floor if pool_floor is not None else 50
        log(f"min-games {a.min_games} ({'the pool' + chr(39) + 's floor' if pool_floor is not None else 'default, pool meta has none'})")
    elif pool_floor is not None and a.min_games > pool_floor:
        log(f"WARNING: --min-games {a.min_games} is above the pool's floor {pool_floor}: "
            f"edges with {pool_floor}-{a.min_games - 1} games leave both the reply mix "
            f"and the aux sidecar")
    log("collecting subgraph edges...")
    proposal_moves = None
    proposal_sha = None
    proposal_input = None
    verified_at = {}
    excluded = {}
    verified_cp = {}
    if a.engine_candidates:
        # A resumed engine job can replace its parquet while the collector runs.
        # Pin both graph passes and the output provenance to exactly one snapshot.
        proposal_input = Path(a.engine_candidates).read_bytes()
        proposal_sha = hashlib.sha256(proposal_input).hexdigest()
        fresh = {}
        excluded = bc.verified_exclusions(proposal_input, a.max_eval_loss)
        verified_cp = bc.verified_cp_overrides(pl.read_parquet(proposal_input))
        verified_at = bc.verified_moves(pl.read_parquet(proposal_input))
        proposal_moves, _ = bc.load_engine_candidates(proposal_input, fresh)
        gate = admit
        if gate is not None:
            gate.excluded = excluded
            gate.eval_ws.update(fresh)
            gate._seen.update(fresh)
    rare_floor = a.min_games if a.rare_verification else None
    if rare_floor is not None:
        gate = admit
        if gate is not None:
            gate.rare_floor, gate.verified = rare_floor, verified_at
    reply_overlay = None
    if a.opponent_prior:
        from opponent_prior import ReplyOverlay
        reply_overlay = ReplyOverlay(a.opponent_prior,a.opponent_pseudogames,stats,aux_path,
                                     a.perspective,a.min_games)
    elif a.opponent_pseudogames != 0:
        ap.error("--opponent-pseudogames requires --opponent-prior")
    graph_stats = (a.uncut_own_moves / "graph.parquet").as_posix() if uncut else stats
    graph_aux = str(a.uncut_own_moves / "aux.parquet") if uncut else aux_path
    if uncut:
        log(f"uncut own moves: {a.perspective}, recipe {uncut['recipe_sha256']}")
    edf = collect_subgraph_edges(con, graph_stats, root, our_white, eps,
                                 a.max_ply, a.share_floor, a.min_games, admit, proposal_moves,
                                 reply_overlay, uncut_own=bool(uncut))
    if not edf.height:
        log("FATAL: empty subgraph")
        return 1
    parents = edf["parent_hash"].unique(maintain_order=True).to_list()
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
        aux_rows = bc.load_aux_rows(graph_aux, parents)
        if reply_overlay is not None:
            for h in parents:
                if h in reply_overlay.extra_aux:
                    if h in aux_rows:
                        raise ValueError(f"Duplicate termination data for expanded parent {h}")
                    aux_rows[h] = reply_overlay.extra_aux[h]
        log(f"  aux rows for {len(aux_rows):,} subgraph nodes")

    # Only the fields build_graph reads, and the EPD on a parent's first row only:
    # a full row dict per edge was most of the peak at a few million edges.
    by_parent: dict[int, list[dict]] = {}
    cols = ["parent_hash","move_san","child_hash","total","white_score_avg","parent_epd"]
    if reply_overlay is not None:
        cols.extend(["reply_mass","reach_share","additional_reply"])
    for ph, ms, ch, tot, ws, epd, *weights in edf.select(cols).iter_rows():
        lst = by_parent.get(ph)
        if lst is None:
            by_parent[ph] = [{"move_san": ms, "child_hash": ch, "total": tot,
                              "white_score_avg": ws, "parent_epd": epd}]
        else:
            lst.append({"move_san": ms, "child_hash": ch, "total": tot,
                        "white_score_avg": ws})
        if weights and weights[0] is not None:
            by_parent[ph][-1].update(reply_mass=weights[0], reach_share=weights[1], additional_reply=weights[2])
    del edf

    engine_moves, engine_conflicts = None, []
    if proposal_input is not None:
        engine_moves, engine_conflicts = bc.load_engine_candidates(proposal_input, eval_ws)
        log(f"  engine candidates at {len(engine_moves):,} positions; {len(engine_conflicts):,} "
            f"differ from the cached eval by more than 25 cp (the fresh eval is used)")

    log("building graph...")
    g = bc.build_graph(by_parent, root, our_white, rep_moves=rep_moves,
                       eval_ws=eval_ws,
                       aux_rows=aux_rows, slice_prior=slice_prior,
                       eps=eps, max_ply=a.max_ply, share_floor=a.share_floor,
                       eval_weight=a.eval_weight,
                       prior_strength=a.prior_strength,
                       other_cov_blend=other_cov_blend,
                       gated_reach=a.gated_reach,
                       max_eval_loss=a.max_eval_loss, **gate_settings,
                       strict_source=True,
                       crack_at_parents=True,
                       engine_moves=engine_moves, excluded=excluded,
                       rare_floor=rare_floor, verified=verified_at,
                       track_route_ply=bool(a.dump_stop_positions or a.sampled_stop
                                            or a.dump_other_positions or a.sampled_other),
                       track_other=bool(a.dump_other_positions or a.sampled_other))
    log(f"  {len(g.our):,} our nodes, {len(g.opp):,} opponent nodes")
    if a.dump_engine_targets:
        from engine_candidates import target_rows
        targets = target_rows(g, a.perspective)
        target_path = Path(a.dump_engine_targets)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = Path(str(target_path) + ".tmp")
        targets.write_parquet(tmp)
        tmp.replace(target_path)
        log(f"{targets.height:,} pre-selection targets -> {target_path}")
        return 0
    other_report = None
    if a.dump_other_positions or a.sampled_other:
        import sampled_other as so
        from sampled_stop import recipe_key
        other_key = recipe_key(a.perspective, a.max_ply + 1, stats, a.min_games, a.share_floor,
                               aux_stats=aux_path, eval_weight=a.eval_weight,
                               other_cov_blend=other_cov_blend)
        other_key["engine_candidates_sha256"] = proposal_sha
        other_key["uncut_own_recipe"] = uncut_recipe
        other_key["gate_policy"] = gate_recipe
        if reply_overlay is not None:
            other_key["opponent_prior"] = reply_overlay.recipe
        if not g.other:
            log("FATAL: no other buckets on this graph (--aux-stats?)")
            return 1
    if a.dump_other_positions:
        dump = so.other_positions(g)
        tmp = Path(a.dump_other_positions + ".tmp")
        dump.write_parquet(tmp)
        tmp.replace(a.dump_other_positions)
        Path(a.dump_other_positions + ".meta.json").write_text(json.dumps(other_key, indent=1),
                                                               encoding="utf-8")
        log(f"other buckets: {dump.height:,} nodes -> {a.dump_other_positions}; stopping (sample "
            f"them with sampled_other.py)")
        return 0
    terminal = None
    if a.dump_stop_positions or a.sampled_stop:
        import sampled_stop as ss
        terminal = ss.terminal_outcomes(by_parent, our_white)
        log(f"  {len(terminal):,} recorded moves end the game")
    incumbent = None
    if a.incumbent:
        incumbent = load_incumbent_moves(a.incumbent, a.perspective)
        g.meta["stick"] = (incumbent, a.stick_margin)
        on_graph = sum(1 for h in incumbent if h in g.our)
        log(f"  incumbent: {len(incumbent):,} moves ({on_graph:,} at positions of this graph), "
            f"stick margin {a.stick_margin}")
    # nothing below reads the edge lists, the eval cache or the gate's source moves
    del by_parent, eval_ws, want, admit
    import gc
    gc.collect()
    stop_counts = None
    sampled_fit = None
    if True:     # the weak-player stop model (the only stop model since 2026-11-12)
        if not Path(a.weak_pool).exists():
            log(f"FATAL: --weak-pool {a.weak_pool} does not exist")
            return 1
        from weak_stop import EvalSource, weak_stop_values
        xs, ys = bc.stop_model_positions(g)
        W, R, wc = weak_stop_values(con, xs, ys, pool=stats, weak_pool=a.weak_pool,
                                    evals=EvalSource(our_white, overrides=verified_cp),
                                    min_weak=a.weak_min_games,
                                    min_share=WEAK_MIN_SHARE, log=log)
        if a.dump_stop_positions or a.sampled_stop:
            info = ss.stop_positions(g)
            dump_key = ss.recipe_key(a.perspective, a.max_ply + 1, stats, a.min_games, a.share_floor,
                                     weak_pool=a.weak_pool, weak_min_games=a.weak_min_games,
                                     weak_min_share=WEAK_MIN_SHARE, stop_model="weak")
            dump_key["engine_candidates_sha256"] = proposal_sha
            dump_key["uncut_own_recipe"] = uncut_recipe
            dump_key["gate_policy"] = gate_recipe
            if reply_overlay is not None:
                dump_key["opponent_prior"] = reply_overlay.recipe
        if a.dump_stop_positions:
            dump = info.with_columns(
                pl.struct("side", "hash").map_elements(
                    lambda r: (W if r["side"] == "x" else R).get(r["hash"]),
                    return_dtype=pl.Float64).alias("stop"),
                pl.col("hash").map_elements(lambda h: terminal.get(h), return_dtype=pl.Float64)
                .alias("exact"))
            tmp = Path(a.dump_stop_positions + ".tmp")
            dump.write_parquet(tmp)
            tmp.replace(a.dump_stop_positions)
            Path(a.dump_stop_positions + ".meta.json").write_text(json.dumps(dump_key, indent=1),
                                                                  encoding="utf-8")
            log(f"stop positions: {dump.height:,} ({dump.filter(pl.col('side') == 'x').height:,} our "
                f"turn), horizon {a.max_ply + 1} -> {a.dump_stop_positions}; stopping (sample them "
                f"with sampled_stop.py)")
            return 0
        if a.sampled_stop:
            # the table must be the referee's at THIS book's colour and horizon, sampled from a
            # dump of this recipe (a horizon H is a graph capped at H - 1: see sampled_stop.py)
            smp = Path(a.sampled_stop + ".meta.json")
            sm = json.loads(smp.read_text(encoding="utf-8")) if smp.exists() else {}
            bad = ss.table_problems(sm, dump_key)
            if sm.get("max_ply") != a.max_ply + 1:
                bad["referee horizon"] = (sm.get("max_ply"), a.max_ply + 1)
            if bad:
                log(f"FATAL: --sampled-stop {a.sampled_stop} does not match this build (table, "
                    f"build): {bad}. A referee horizon H needs --max-ply H - 1.")
                return 1
            if a.sampled_other:
                om = Path(a.sampled_other + ".meta.json")
                diff = ss.simulation_mismatch(sm, json.loads(om.read_text(encoding="utf-8")) if om.exists() else {})
                if diff:
                    log(f"FATAL: the stop and other tables come from different simulations (stop, other): {diff}")
                    return 1
            bp = Path(a.sampled_stop + ".boundary.parquet")
            if not bp.exists():
                log(f"FATAL: {bp} is missing: the stored evals for horizon positions")
                return 1
            fills = dict(pl.read_parquet(bp).select("hash", "cp").iter_rows())
            W, R, sampled_report = ss.apply(W, R, info, pl.read_parquet(a.sampled_stop),
                                            horizon=a.max_ply + 1, terminal=terminal,
                                            evals=EvalSource(our_white, boundary=True, fills=fills,
                                                             overrides=verified_cp))
            sampled_fit = {k: sampled_report.get(k) for k in
                           ("model", "cross_ply_refused", "multi_ply_reach_share", "terminal",
                            "horizon", "horizon_no_eval")}
            log(f"  sampled stop values: {sampled_report['sampled']} sampled, "
                f"{sampled_report['predicted']} predicted, {sampled_report['horizon']} at the horizon "
                f"({sampled_report['horizon_no_eval']} without an eval), {sampled_report['terminal']} "
                f"finished, {sampled_report['cross_ply_refused']} refused (sampled at another ply); "
                f"fit {sampled_report['model']}; graph diagnostic, optimistic stop reach at "
                f"positions reachable at several plies {sampled_report.get('multi_ply_reach_share')}")
            wc = {**dict(wc), **{f"sampled_{k}": v for k, v in sampled_report["sampled"].items()},
                  **{f"predicted_{k}": v for k, v in sampled_report["predicted"].items()}}
            del info
        ac = bc.apply_stop_values(g, W, R)
        stop_counts = {**dict(wc), **{f"applied_{k}": v for k, v in ac.items()}}
        log(f"  weak stop model: {stop_counts}  ({time.time()-t0:,.0f}s)")
        del W, R
    if a.sampled_other:
        smp = Path(a.sampled_other + ".meta.json")
        sm = json.loads(smp.read_text(encoding="utf-8")) if smp.exists() else {}
        from sampled_stop import table_problems
        bad = table_problems(sm, other_key)
        if bad:
            log(f"FATAL: --sampled-other {a.sampled_other} does not match this build (table, "
                f"build): {bad}")
            return 1
        other_report = so.apply(g, pl.read_parquet(a.sampled_other))
        log(f"  sampled other buckets: {other_report}")
    g.other = {}

    if a.method == "price":
        curves, diag = {}, {"stranded_cycle_nodes": None}
    else:
        log("building curves...")
        curves, diag = bc.build_curves(g, bmax=bmax, k_atoms=a.grid_cap)
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
                                          k_atoms=a.grid_cap)
        log(f"  probe curves ready  ({time.time()-t0:,.0f}s)")

    # Diagnostics read the WIDEST set: both are trimmed at their own bmax, so
    # taking capacity or a value-at-charged off the plain curves would silently
    # report the cap rather than the quantity.
    widest = probe_curves if probe_curves is not None else curves
    cap = widest[g.root].capacity if g.root in widest else 0
    if a.method != "price":
        log(f"  root capacity {cap:,} (path-sum)")

    polish_order = None

    # --method price: compile the (final) graph once for every budget, and the source seed
    # once (it does not depend on the budget)
    price_cg = price_seed = None
    if a.method == "price" and todo:
        if a.price_engine == "compiled" or (
                a.price_engine == "auto"
                and len(g.our) + len(g.opp) >= bc.PRICE_COMPILED_MIN_NODES):
            import budget_compiled
            t1 = time.time()
            price_cg = budget_compiled.compile_graph(g)
            log(f"  graph compiled for the price search ({time.time()-t1:,.0f}s)")
        if True:
            # never dearer than the optimistic reach; the source book's policy reach
            # (summed over its paths) where that is larger
            order = (price_cg.order if price_cg is not None else
                     (bc._topo_up_dict if a.price_engine == "legacy" else bc.topo_up)(g)[0])
            price_seed = dict(g.reach)
            price_seed.update({h: max(r, price_seed.get(h, 0.0)) for h, r in bc.policy_reach(
                g, rep_moves, order, compiled=price_cg).items() if r > 0})
            del order

    for b in budgets:
        out = outs[b]
        if b not in todo:
            log(f"skip b={b} (exists)")
            continue
        price_info = None
        if a.method == "price":
            res, price_info = bc.price_search(g, b, log=log, rounds=PRICE["rounds"],
                                              select=PRICE["select"], seed_reach=price_seed,
                                              reprice=PRICE["reprice"],
                                              dag_mode=PRICE["dag_mode"],
                                              engine=a.price_engine, compiled=price_cg)
            charged, matched = b, res["spent_distinct"] >= b
            log(f"  ({time.time()-t0:,.0f}s)")
        elif a.match_distinct:
            res, charged, matched, spent = bc.match_distinct(
                g, curves, b, curve_bmax,
                probe_curves=probe_curves, plain_bmax=bmax)
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
            res = bc.extract_book(g, curves, b)
            charged, matched = b, res["spent_distinct"] >= b
        polish_info = None
        if a.polish:
            # the DP book is the start; the polished book's values are policy_value's
            # (book_result), not the curves', and its rows carry no allocation
            if polish_order is None:
                polish_order = bc.topo_up(g)[0]
            moves, polish_info = bc.polish_book(g, res["booked"], b, polish_order)
            res = bc.book_result(g, moves, polish_order)
            log(f"  dp polish: {polish_info['start_size']:,} -> {polish_info['size']:,} "
                f"positions, {polish_info['start_value']:.5f} -> {polish_info['value']:.5f} "
                f"({polish_info['accepted']} moves, {polish_info['seconds']:.1f}s)")

        rc = g.meta.get("root_crack")
        if rc:
            # --crack-at-parents moves an our-node's collapses onto its parents' edges; the
            # root has none, so its own share is applied here, to the reported value only
            # (an affine map of the root value: no decision depends on it)
            ra = rc[0] / (rc[0] + rc[1])
            rv = res["root_value_realized"]
            res = {**res, "root_value_realized": (1.0 - ra) + ra * rv,
                   "realized": {**res["realized"], g.root: (1.0 - ra) + ra * res["realized"][g.root]}
                   if g.root in res["realized"] else res["realized"]}
        rows = []
        lexc = g.meta.get("loss_exceptions")
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
                # The unrestricted SOURCE rep's soundness numbers, renamed 2026-10-31:
                # as value_robust/value_worst they read as this book's own, and
                # score_repertoire reported them as such. Nothing here recomputes them
                # for the budget policy, so they are labelled for what they are.
                "source_value_robust": src.get("value_robust"),
                "source_value_worst": src.get("value_worst"),
                "eval_score": src.get("eval_score"),
                "value_unconstrained": src.get("value"),
                "alloc_budget": res["alloc"].get(h, 0),
                # Booked decisions the loss limit could not bind (budget_core.
                # loss_exceptions): 'fallback' = least-losing move, 'unknown' = no eval.
                **({"loss_exception": lexc.get(h) if res["booked"].get(h) else None}
                   if lexc is not None else {}),
            })
        tmp = out.with_suffix(".parquet.tmp")
        book_frame(rows).write_parquet(tmp, compression="zstd")
        tmp.replace(out)
        meta = {
            "budget_requested": b,
            "spent_paths": res["spent_paths"],
            "spent_distinct": res["spent_distinct"],
            "method": a.method,
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
            "prior_strength": a.prior_strength, "gates": True,
            "max_cands": 0, "gated_reach": a.gated_reach,
            "max_eval_loss": a.max_eval_loss, **gate_settings,
            "min_games": a.min_games, "pool_min_games": pool_floor,
            **({"uncut_own_recipe": uncut["recipe_sha256"], "uncut_own_moves": str(a.uncut_own_moves)}
               if uncut else {}),
            "perspective": a.perspective,
            **({"rare_floor": rare_floor, "rare_disputes": len(g.meta.get("rare_disputes", {})),
                "rare_disputes_booked": sum(1 for h in g.meta.get("rare_disputes", {})
                                            if res["booked"].get(h))}
               if rare_floor is not None else {}),
            **({"opponent_prior":reply_overlay.recipe, "named_opponent_replies":g.meta.get("named_opponent_replies",{})}
               if reply_overlay is not None else {}),
            # recorded either way: a meta from before 2026-10-31 lacks both keys, which
            # means both off (the old defaults)
            "strict_source": True,
            "src_rejected": g.meta.get("src_rejected", 0),
            **({"loss_exceptions_booked": {
                k: sum(1 for h, s in lexc.items() if s == k and res["booked"].get(h))
                for k in ("fallback", "unknown")}} if lexc is not None else {}),
            "crack_at_parents": True,
            **({"engine_candidates": a.engine_candidates, "engine_candidates_sha256": proposal_sha,
                "engine_cands_added": g.meta.get("engine_cands", 0),
                "engine_eval_conflicts": engine_conflicts}
               if a.engine_candidates else {}),
            "crack_edges": g.meta.get("crack_edges", 0),
            **({"incumbent": str(a.incumbent), "stick_margin": a.stick_margin,
                "incumbent_churn": incumbent_churn(res["booked"], incumbent, g.reach)}
               if incumbent is not None else {}),
            "leftover": False,
            "stop_model": "weak",
            **({"sampled_stop": a.sampled_stop, "sampled_fit": sampled_fit,
                "sampled_config": json.loads(Path(a.sampled_stop + ".meta.json").read_text(encoding="utf-8"))
                if Path(a.sampled_stop + ".meta.json").exists() else None}
               if a.sampled_stop else {}),
            **({"sampled_other": a.sampled_other, "sampled_other_fit": other_report}
               if a.sampled_other else {}),
            **({"price_rounds": PRICE["rounds"], "price_select": PRICE["select"],
                "price_seed": "source", "price_reprice": PRICE["reprice"],
                "price_dag_mode": PRICE["dag_mode"], "price_engine": a.price_engine}
               if a.method == "price" else {}),
            **({"price": price_info} if price_info is not None else {}),
            **({"polish": polish_info} if polish_info is not None else {}),
            "weak_pool": a.weak_pool, "weak_min_games": a.weak_min_games,
            "weak_min_share": WEAK_MIN_SHARE, "weak_counts": stop_counts,
            "source_rep": str(a.rep), "source_meta": src_meta,
            "stats": str(a.stats), "aux_stats": aux_path,
            "subgraph_nodes": len(g.our) + len(g.opp),
            "subgraph_our_nodes": len(g.our),
            "peak_commit_gb": peak_commit_gb(),
            "built": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        Path(str(out) + ".meta.json").write_text(json.dumps(meta, indent=2))
        if rare_floor is not None:
            # every decision where an unverified rare move would have excluded a rival had it
            # bound, with the graph's optimistic reach: the verifier's work list
            Path(str(out) + ".rare.json").write_text(json.dumps(
                [{"position_hash": h, "reach": g.reach.get(h, 0.0), "ply": g.ply.get(h),
                  "booked": res["booked"].get(h), **d}
                 for h, d in sorted(g.meta.get("rare_disputes", {}).items())]))
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

    pk = peak_commit_gb()
    log(f"\ndone in {time.time()-t0:,.0f}s"
        + (f"; peak commit {pk:,.1f} GB" if pk is not None else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
