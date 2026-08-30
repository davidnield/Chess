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

Crush: the frozen unconstrained crush_potential (from --rep) enters the
extraction argmax as cw*(imm + (1-imm)*gamma^0.5*crush_pot[child]). The
per-edge imm term needs an edge-crush cache built from the crush histogram
(--crush-db, heavy, skip-gated); without a cache the imm=0 approximation is
used and noted in meta. --crush-weight 0 disables the term entirely.

Usage:
    .venv/Scripts/python.exe python/build_budget_books.py \\
        --rep E:/chess/repertoire/_sweep/baseline_white.parquet \\
        --stats E:/chess/position-stats/position_stats_pooled_ge1800_2013_2024_brc.parquet \\
        --perspective white --out-prefix E:/chess/repertoire/_budget/dp_white \\
        --budget 400 --budget 4000 --budget 40000 --method dp

Defaults for --aux-stats / --reply-shrink / --crush-weight / --crush-gamma are
read from the source rep's .meta.json sidecar; explicit flags override.
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


def collect_subgraph_edges(con, stats: str, root: int, our_white: bool,
                           eps: float, max_ply: int, share_floor: float,
                           min_games: int) -> pl.DataFrame:
    """Iterative frontier expansion (pattern: build_baseline_books.edges_for),
    keeping every edge of every node whose OPTIMISTIC reach >= eps. Reach decays
    only at opponent nodes, by reply share; our moves carry it unchanged."""
    frames: list[pl.DataFrame] = []
    reach: dict[int, float] = {root: 1.0}
    ply: dict[int, int] = {root: 0}
    frontier = [root]
    fetched: set[int] = set()
    lvl = 0
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
        fetched.update(frontier)
        frames.append(df)
        nxt: dict[int, float] = {}
        if df.height:
            for ph, grp in df.group_by("parent_hash"):
                ph = ph[0] if isinstance(ph, tuple) else ph
                r = reach.get(ph, 0.0)
                p_ply = ply.get(ph, 0)
                if p_ply >= max_ply:
                    continue
                tot = int(grp["total"].sum())
                our_turn = (p_ply % 2 == 0) == our_white
                for row in grp.iter_rows(named=True):
                    ch = row["child_hash"]
                    if ch is None:
                        continue
                    if our_turn:
                        cr = r
                    else:
                        share = row["total"] / tot if tot else 0.0
                        if share < share_floor:
                            continue
                        cr = r * share
                    if cr < eps or ch in fetched:
                        continue
                    if cr > reach.get(ch, 0.0):
                        reach[ch] = cr
                        ply[ch] = p_ply + 1
                        nxt[ch] = cr
        frontier = list(nxt)
        lvl += 1
        log(f"  level {lvl}: fetched {len(fetched):,} nodes, "
            f"frontier {len(frontier):,}, edges so far "
            f"{sum(f.height for f in frames):,}")
    return pl.concat([f for f in frames if f.height]) if frames else pl.DataFrame()


def load_edge_imm(cache: Path, our_white: bool) -> dict:
    df = pl.read_parquet(cache)
    col = "white_imm_sum" if our_white else "black_imm_sum"
    out = {}
    for r in df.iter_rows(named=True):
        cg = r.get("crush_games") or 0
        if cg:
            out[(r["parent_hash"], r["move_san"])] = (r.get(col) or 0.0) / cg
    return out


def build_edge_crush_cache(crush_db: str, parents: list[int], out: Path,
                           threads: int, mem: str, tmp: str | None) -> None:
    """Per-edge imm/dfull aggregate over the crush histogram, semi-joined to
    the subgraph's parents (stage3 main()'s SQL, restricted). Skip-gated."""
    if out.exists():
        log(f"  edge-crush cache exists: {out}")
        return
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{mem}'; SET threads={threads}; "
                "SET preserve_insertion_order=false;")
    if tmp:
        con.execute(f"SET temp_directory='{tmp}'")
    con.execute("CREATE TEMP TABLE keep (h BIGINT)")
    con.executemany("INSERT INTO keep VALUES (?)", [(h,) for h in parents])
    tmp_out = out.with_suffix(".parquet.tmp")
    con.execute(f"""
        COPY (
            SELECT c.parent_hash, c.move_san,
                   SUM(CASE WHEN move_bucket BETWEEN 1 AND 2
                       THEN white_wins ELSE 0 END)::DOUBLE AS white_imm_sum,
                   SUM(CASE WHEN move_bucket BETWEEN 1 AND 2
                       THEN black_wins ELSE 0 END)::DOUBLE AS black_imm_sum,
                   SUM(n)::BIGINT AS crush_games
            FROM read_parquet('{crush_db}') c
            JOIN keep k ON k.h = c.parent_hash
            GROUP BY c.parent_hash, c.move_san
        ) TO '{tmp_out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    tmp_out.replace(out)
    log(f"  built edge-crush cache: {out}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rep", required=True, help="Unconstrained source rep parquet.")
    ap.add_argument("--stats", required=True)
    ap.add_argument("--perspective", choices=["white", "black"], required=True)
    ap.add_argument("--out-prefix", required=True,
                    help="Books land at <prefix>_b<N>.parquet (+ .meta.json).")
    ap.add_argument("--budget", type=int, action="append", required=True)
    ap.add_argument("--method", choices=["greedy", "dp"], default="dp")
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
    ap.add_argument("--crush-weight", type=float, default=None)
    ap.add_argument("--crush-gamma", type=float, default=None)
    ap.add_argument("--crush-db", default=None)
    ap.add_argument("--edge-crush-cache", default=None)
    ap.add_argument("--slice-prior", type=float, default=None,
                    help="Default: computed from the stats root edges.")
    ap.add_argument("--eval-weight", type=float, default=bc.EVAL_WEIGHT,
                    help="Engine-eval share of every leaf/stop value (default "
                         "0.5, the blessed recipe). 0 = purely empirical value: "
                         "every number becomes a smoothed observed score. Note "
                         "the eval is still used for GATING which candidates "
                         "exist — that is a separate mechanism from value.")
    ap.add_argument("--prior-strength", type=float, default=bc.PRIOR_STRENGTH,
                    help="Beta-binomial pseudocount pulling thin empirical "
                         "scores toward the slice prior (default 500). 0 = raw "
                         "observed scores, unsmoothed.")
    ap.add_argument("--no-gates", action="store_true",
                    help="Drop require-eval and both refutation gates, so every "
                         "recorded move is a candidate. Diagnostic only.")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--mem", default="24GB")
    ap.add_argument("--tmp-dir", default="D:/chess_duckdb_tmp")
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
    cw = a.crush_weight if a.crush_weight is not None else \
        float(src_meta.get("crush_weight", 0.0) or 0.0)
    cg = a.crush_gamma if a.crush_gamma is not None else \
        float(src_meta.get("crush_gamma", 0.99) or 0.99)

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
        f"method {a.method}{' fixed-policy' if a.fixed_policy else ''}; "
        f"crush w={cw} gamma={cg}")

    log("collecting subgraph edges...")
    edf = collect_subgraph_edges(con, stats, root, our_white, eps,
                                 a.max_ply, a.share_floor, a.min_games)
    if not edf.height:
        log("FATAL: empty subgraph")
        return 1
    parents = edf["parent_hash"].unique().to_list()
    log(f"  {edf.height:,} edges, {len(parents):,} parent nodes "
        f"({time.time()-t0:,.0f}s)")

    log("loading source rep (subgraph slice)...")
    want = set(parents) | set(edf["child_hash"].drop_nulls().to_list())
    rep = (pl.scan_parquet(a.rep)
           .filter(pl.col("position_hash").is_in(list(want)))
           .select(["position_hash", "side_to_move", "value", "best_move",
                    "crush_potential", "value_robust", "value_worst",
                    "eval_score"])
           .collect())
    rep_moves, rep_crush, rep_rows = {}, {}, {}
    for r in rep.iter_rows(named=True):
        h = r["position_hash"]
        rep_rows[h] = r
        if r["side_to_move"] == a.perspective and r["best_move"]:
            rep_moves[h] = r["best_move"]
        if r["crush_potential"] is not None:
            rep_crush[h] = r["crush_potential"]
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
        adf = (pl.scan_parquet(aux_path)
               .filter(pl.col("position_hash").is_in(parents)).collect())
        aux_rows = {r["position_hash"]: r for r in adf.iter_rows(named=True)}
        log(f"  aux rows for {len(aux_rows):,} subgraph nodes")

    edge_imm: dict = {}
    imm_note = "imm=0 approximation (no edge-crush cache)"
    if cw > 0:
        cache = Path(a.edge_crush_cache) if a.edge_crush_cache else \
            outs[budgets[0]].parent / "edge_crush_cache.parquet"
        if a.crush_db:
            build_edge_crush_cache(a.crush_db.replace("\\", "/"), parents,
                                   cache, a.threads, a.mem, a.tmp_dir)
        if cache.exists():
            edge_imm = load_edge_imm(cache, our_white)
            imm_note = f"edge imm from {cache.name} ({len(edge_imm):,} edges)"
    log(f"  crush: {imm_note}")

    by_parent: dict[int, list[dict]] = {}
    for r in edf.iter_rows(named=True):
        by_parent.setdefault(r["parent_hash"], []).append(r)

    log("building graph...")
    g = bc.build_graph(by_parent, root, our_white, rep_moves=rep_moves,
                       rep_crush=rep_crush, eval_ws=eval_ws,
                       aux_rows=aux_rows, slice_prior=slice_prior,
                       eps=eps, max_ply=a.max_ply, share_floor=a.share_floor,
                       crush_weight=cw, crush_gamma=cg, edge_imm=edge_imm,
                       max_cands=a.max_cands, eval_weight=a.eval_weight,
                       prior_strength=a.prior_strength,
                       require_eval=not a.no_gates, gates=not a.no_gates)
    log(f"  {len(g.our):,} our nodes, {len(g.opp):,} opponent nodes")

    fixed = rep_moves if (a.fixed_policy or a.method == "greedy") else None
    log("building curves...")
    curves, diag = bc.build_curves(g, bmax=curve_bmax, k_atoms=a.grid_cap,
                                   fixed_policy=fixed)
    cap = curves[g.root].capacity if g.root in curves else 0
    log(f"  root capacity {cap:,} (path-sum); stranded cycle nodes "
        f"{diag['stranded_cycle_nodes']:,}  ({time.time()-t0:,.0f}s)")

    for b in budgets:
        out = outs[b]
        if b not in todo:
            log(f"skip b={b} (exists)")
            continue
        if a.method == "greedy":
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
            res, charged, matched = bc.match_distinct(g, curves, b, curve_bmax,
                                                      fixed_policy=fixed)
            log(f"  match-distinct b={b}: charged {charged} for "
                f"{res['spent_distinct']} distinct"
                + ("" if matched else "  (TARGET NOT REACHED — capacity)"))
        else:
            res = bc.extract_book(g, curves, b, fixed_policy=fixed)
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
                "side_to_move": side,
                "value": flip(res["realized"].get(h)),
                "best_move": res["booked"].get(h),
                "value_robust": src.get("value_robust"),
                "value_worst": src.get("value_worst"),
                "eval_score": src.get("eval_score"),
                "crush_potential": src.get("crush_potential"),
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
            "distinct_target_met": bool(matched),
            "root_value_curve": None if g.root not in curves
            else curves[g.root].eval(charged),
            "root_value_realized": res["root_value_realized"],
            "root_capacity_paths": cap,
            "stranded_cycle_nodes": diag["stranded_cycle_nodes"],
            "crush_weight": cw, "crush_gamma": cg, "crush_imm": imm_note,
            "slice_prior": slice_prior, "eval_weight": a.eval_weight,
            "prior_strength": a.prior_strength, "gates": not a.no_gates,
            "max_cands": a.max_cands,
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
        if rvr is not None:
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
