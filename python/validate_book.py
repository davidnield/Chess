"""Validate the exact book a person will learn (follow-up review R5, 2026-10-31).

For every decision of OUR colour in the file (base rows and sequence-extension rows alike):

  legal      the position parses, its side to move and hash match the row, the move is legal
  candidate  base rows of a budget book: is the move in the final candidate set its recipe
             allows -- budget_core.gate_candidates over the pool's edges at the meta's floor,
             plus the source book's move under the --strict-source rule? Status:
               in_set          one of the candidates, within --max-eval-loss
               fallback        every candidate failed the limit; the least-losing one (the
                               owner's rule, 2026-10-08)
               unknown         no candidate had an eval: the limit could not judge
               outside_set     NOT a permitted candidate (a defect)
               extension       a sequence-extension row (extend_book: engine-chosen)
  eval       parent and child evals from the eval arrays and the cached loss; rows whose loss
             exceeds the limit, or that lack an eval, are FLAGGED
  fresh      with --nodes N, every flagged row gets a free Stockfish search. If its best move IS
             the book's move there is nothing to compare: same_move, whatever the scores say
             (two searches disagreeing is uncertainty, not an error). Otherwise the best move and
             the book's move are each searched under the same restriction, budget and cleared
             hash: confirmed (the book's move is worse by more than the limit) or cleared.
             unstable marks a row whose free and restricted scores of the best move disagree
             by more than the limit: treat its verdict with care, search deeper

--best-defence is a RESTRICTED adversarial audit of the book: at each opponent node reached by
our book moves, the recorded pool replies plus Stockfish's single best reply (at
--defence-nodes), our side following the book, to the book's edge; reports the worst engine value
at the edge (our view) and the line that reaches it. It can expose a failure; it is no proof
against every legal defence (an unrecorded second-best move can still hurt a later book move), and
cached edge evals keep their uncertainty. --incumbent PATH adds churn against another book.

    PYTHONPATH=python .venv/Scripts/python.exe python/validate_book.py --book PATH \\
        --colour white [--meta PATH] [--nodes 5000000] [--best-defence] [--out PREFIX]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

import chess
import chess.engine
import duckdb
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
import budget_core as bc  # noqa: E402
from duck_compat import duck_pl  # noqa: E402
from eval_arrays import MISSING, lookup_evals, open_eval_arrays  # noqa: E402
from stage3_backwards_induction import cp_to_expected_score  # noqa: E402
from zobrist import zobrist_int64  # noqa: E402

SF = "C:/Users/David/AppData/Local/Microsoft/WinGet/Links/stockfish.exe"


def base_meta(book: Path) -> Path:
    """<book>.meta.json, or for an extended file its base book's."""
    p = Path(str(book) + ".meta.json")
    if p.exists():
        return p
    base = re.sub(r"_ext_[^.]+\.parquet$", ".parquet", str(book))
    return Path(base + ".meta.json")


def board(epd: str) -> chess.Board:
    return chess.Board(epd + " 0 1")


def classify(h: int, move: str, es: list[dict], source_move: str | None, ev: dict, our_white: bool,
             meta: dict, engine_moves: dict | None, epd: str) -> str:
    """A base row's status, by the builder's own candidate rule (budget_core.final_candidates)
    under the recipe's gates and limit, always with the strict source rule: the audit holds a
    book to what is promised, including a book built before that rule existed."""
    gates = meta.get("gates", True)
    limit = meta.get("max_eval_loss")
    cand, restored, _rej, _eng = bc.final_candidates(
        h, es, source_move, ev, our_white, meta["slice_prior"], require_eval=gates, gates=gates,
        max_eval_loss=limit, max_cands=meta.get("max_cands", 0) or 0, strict_source=True,
        engine_moves=engine_moves, edges_by_parent={h: es}, epd=epd)
    pairs = [(e["move_san"], e.get("child_hash")) for e, _ in cand]
    if restored is not None:
        pairs.append(restored)
    if move.rstrip("+#") not in {m.rstrip("+#") for m, _ in pairs}:
        return "outside_set"
    if limit is None:
        return "in_set"
    return bc.loss_status([c for _, c in pairs], h, ev, our_white, limit) or "in_set"


def fresh_verdict(eng, b: chess.Board, move: str, nodes: int, limit: float, our_white: bool) -> dict:
    """See `fresh` in the header. eng: a python-chess engine (analyse, configure)."""
    def search(root_moves=None):
        eng.configure({"Clear Hash": None})
        kw = {"root_moves": root_moves} if root_moves else {}
        return eng.analyse(b, chess.engine.Limit(nodes=nodes), **kw)

    def cp(info):
        return info["score"].pov(our_white).score(mate_score=2000)
    free = search()
    best = free["pv"][0]
    mv = b.parse_san(move)
    out = {"sf_best": b.san(best), "sf_best_cp": cp(free), "sf_nodes": nodes}
    if best == mv:
        return {**out, "sf_move_cp": cp(free), "sf_loss": 0, "fresh": "same_move", "unstable": False}
    best_cp, mine_cp = cp(search([best])), cp(search([mv]))
    loss = best_cp - mine_cp
    return {**out, "sf_best_restricted_cp": best_cp, "sf_move_cp": mine_cp, "sf_loss": loss,
            "fresh": "confirmed" if loss > limit else "cleared",
            "unstable": abs(cp(free) - best_cp) > limit}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--book", required=True)
    ap.add_argument("--colour", required=True, choices=["white", "black"])
    ap.add_argument("--meta", default=None, help="the recipe meta (default: the book's, or its base's)")
    ap.add_argument("--nodes", type=int, default=0, help="fresh Stockfish nodes per flagged row (0 = none)")
    ap.add_argument("--best-defence", action="store_true")
    ap.add_argument("--defence-nodes", type=int, default=1_000_000)
    ap.add_argument("--incumbent", default=None)
    ap.add_argument("--out", default=None, help="writes PREFIX.rows.parquet and PREFIX.json")
    a = ap.parse_args()
    t0 = time.time()
    our_white = a.colour == "white"
    sign = 1 if our_white else -1
    meta_p = Path(a.meta) if a.meta else base_meta(Path(a.book))
    meta = json.loads(meta_p.read_text(encoding="utf-8")) if meta_p.exists() else {}
    limit = meta.get("max_eval_loss")
    min_games = meta.get("min_games", 50)          # pre-2026-10-31 metas: the old default
    d = pl.read_parquet(a.book)
    ext = d["extension"] if "extension" in d.columns else pl.Series([False] * d.height)
    d = d.with_columns(ext.fill_null(False).alias("_ext"))
    ours = d.filter((pl.col("side_to_move") == a.colour) & pl.col("best_move").is_not_null())
    src = {}
    if meta.get("source_rep"):
        s = (pl.scan_parquet(meta["source_rep"]).filter(
            pl.col("position_hash").is_in(ours["position_hash"].to_list())
            & (pl.col("side_to_move") == a.colour)).select("position_hash", "best_move").collect())
        src = {h: m for h, m in s.iter_rows() if m}
    edges = {}
    if meta.get("stats"):
        con = duckdb.connect()
        con.execute("SET threads=4; SET memory_limit='8GB'")
        # every position in the file (ours for the candidate sets, the opponent's for the
        # best-defence walk) plus our moves' children (opponent nodes the file may not list)
        pos = set(d["position_hash"].to_list())
        for h, m, epd in ours.select("position_hash", "best_move", "position_epd").iter_rows():
            c = bc_child(epd, m)
            if c is not None:
                pos.add(c)
        con.register("keys", pl.DataFrame({"h": sorted(pos)}, schema={"h": pl.Int64}).to_arrow())
        e = duck_pl(con, f"""SELECT parent_hash, move_san, child_hash, total, white_score_avg
            FROM read_parquet('{meta['stats'].replace(chr(92), '/')}') s
            SEMI JOIN keys k ON k.h = s.parent_hash WHERE total >= {min_games}""")
        for h, g in e.group_by("parent_hash"):
            edges[h[0]] = g.to_dicts()
    # legality and the hashes to evaluate
    rows, keys = [], set()
    for r in ours.iter_rows(named=True):
        h, m, epd = r["position_hash"], r["best_move"], r["position_epd"]
        rec = {"position_hash": h, "move": m, "extension": bool(r["_ext"]), "epd": epd,
               "legal": False, "child_hash": None, "source_move": src.get(h)}
        try:
            b = board(epd)
            if b.is_valid() and (b.turn == chess.WHITE) == our_white and zobrist_int64(b) == h:
                b.push_san(m)
                rec["legal"], rec["child_hash"] = True, zobrist_int64(b)
                keys |= {h, rec["child_hash"]}
                if rec["source_move"] and not any(x["move_san"] == rec["source_move"]
                                                  for x in edges.get(h, [])):
                    rec["source_child"] = bc_child(epd, rec["source_move"])
        except ValueError:
            pass
        for x in edges.get(h, []):
            if x["child_hash"] is not None:
                keys.add(x["child_hash"])
        if rec.get("source_child") is not None:
            keys.add(rec["source_child"])
        rows.append(rec)
    mm = open_eval_arrays()
    hs = np.array(sorted(keys), dtype=np.int64)
    cps = {int(h): int(c) for h, c in zip(hs, lookup_evals(hs, *mm)) if c != MISSING}
    ev = {h: cp_to_expected_score(c) for h, c in cps.items()}
    engine_moves, conflicts = None, []
    if meta.get("engine_candidates"):
        engine_moves, conflicts = bc.load_engine_candidates(meta["engine_candidates"], ev)
    for rec in rows:
        h = rec["position_hash"]
        rec["parent_cp_us"] = sign * cps[h] if h in cps else None
        ch = rec["child_hash"]
        rec["child_cp_us"] = sign * cps[ch] if ch in cps else None
        rec["cached_loss"] = (rec["parent_cp_us"] - rec["child_cp_us"]
                              if rec["parent_cp_us"] is not None and rec["child_cp_us"] is not None else None)
        if not rec["legal"]:
            rec["status"] = "illegal"
        elif rec["extension"]:
            rec["status"] = "extension"
        elif not meta:
            rec["status"] = "no_recipe"
        else:
            rec["status"] = classify(h, rec["move"], edges.get(h, []), rec["source_move"], ev,
                                     our_white, meta, engine_moves, rec["epd"])
        rec["flagged"] = rec["legal"] and limit is not None and (
            rec["cached_loss"] is None or rec["cached_loss"] > limit)
    # fresh searches of the flagged rows
    if a.nodes and limit is not None:
        eng = chess.engine.SimpleEngine.popen_uci(SF)
        eng.configure({"Threads": 8, "Hash": 1024})
        try:
            for rec in (r for r in rows if r["flagged"]):
                rec.update(fresh_verdict(eng, board(rec["epd"]), rec["move"], a.nodes, limit,
                                         our_white))
        finally:
            eng.quit()
    out = pl.DataFrame(rows, infer_schema_length=None)
    summ = {"book": a.book, "colour": a.colour, "meta": str(meta_p) if meta else None,
            "max_eval_loss": limit, "min_games": min_games, "decisions": len(rows),
            "extension_rows": int(out["extension"].sum()),
            "status": dict(out.group_by("status").len().iter_rows()),
            "flagged": int(out["flagged"].sum()),
            "missing_child_eval": int(out["child_cp_us"].is_null().sum()),
            "engine_eval_conflicts": conflicts}
    if "fresh" in out.columns:
        summ["fresh"] = dict(out.filter(pl.col("fresh").is_not_null()).group_by("fresh").len().iter_rows())
    if a.incumbent:
        inc = pl.read_parquet(a.incumbent).filter((pl.col("side_to_move") == a.colour)
                                                  & pl.col("best_move").is_not_null())
        im = {h: m.rstrip("+#") for h, m in inc.select("position_hash", "best_move").iter_rows()}
        nm = {r["position_hash"]: r["move"].rstrip("+#") for r in rows}
        both = nm.keys() & im.keys()
        summ["churn"] = {"incumbent": a.incumbent, "shared_positions": len(both),
                         "same_move": sum(nm[h] == im[h] for h in both),
                         "changed_move": sum(nm[h] != im[h] for h in both),
                         "removed": len(im.keys() - nm.keys()), "added": len(nm.keys() - im.keys())}
    if a.best_defence:
        summ["best_defence"] = best_defence(rows, edges, our_white, a.defence_nodes, mm)
    summ["seconds"] = round(time.time() - t0)
    print(json.dumps(summ, indent=1, default=str))
    if a.out:
        out.write_parquet(a.out + ".rows.parquet")
        Path(a.out + ".json").write_text(json.dumps(summ, indent=1, default=str), encoding="utf-8")


def bc_child(epd: str, san: str):
    from build_baseline_books import child_of
    return child_of(epd, san)


def best_defence(rows, edges, our_white, nodes, mm):
    """Minimax over the book: our side plays the book's move; at each opponent node the
    replies are the recorded pool replies (at the recipe's floor) plus Stockfish's best;
    a position the book has no move for (or a repeat) is a leaf, valued by the eval arrays,
    else Stockfish at `nodes`. Returns the worst leaf value (our view, cp), the line to it,
    and how many opponent nodes / leaves the walk saw."""
    book = {r["position_hash"]: r["move"] for r in rows if r["legal"]}
    eng = chess.engine.SimpleEngine.popen_uci(SF)
    eng.configure({"Threads": 8, "Hash": 1024})
    memo: dict[int, tuple] = {}
    stats = {"opp_nodes": 0, "leaves": 0, "engine_replies_outside_pool": 0}

    def leaf(b: chess.Board) -> int:
        stats["leaves"] += 1
        if b.is_game_over(claim_draw=False):
            r = b.result(claim_draw=False)
            return 0 if r == "1/2-1/2" else (2000 if (r == "1-0") == our_white else -2000)
        h = zobrist_int64(b)
        c = int(lookup_evals(np.array([h], dtype=np.int64), *mm)[0])
        if c != MISSING:
            return c if our_white else -c
        return eng.analyse(b, chess.engine.Limit(nodes=nodes))["score"].pov(our_white).score(mate_score=2000)

    def walk(b: chess.Board, path: tuple, on_path: frozenset) -> tuple:
        h = zobrist_int64(b)
        if h in memo:
            return memo[h]
        if h in on_path or b.is_game_over(claim_draw=False):
            return (leaf(b), path)
        us = (b.turn == chess.WHITE) == our_white
        if us:
            if h not in book:
                return (leaf(b), path)
            b.push_san(book[h])
            res = walk(b, path + (book[h],), on_path | {h})
            b.pop()
        else:
            stats["opp_nodes"] += 1
            replies = {e["move_san"] for e in edges.get(h, [])} | set()
            info = eng.analyse(b, chess.engine.Limit(nodes=nodes))
            best = b.san(info["pv"][0])
            if best not in replies:
                stats["engine_replies_outside_pool"] += 1
            replies.add(best)
            res = None
            for san in sorted(replies):
                try:
                    b.push_san(san)
                except ValueError:
                    continue
                r = walk(b, path + (san,), on_path | {h})
                b.pop()
                if res is None or r[0] < res[0]:
                    res = r
            res = res or (leaf(b), path)
        memo[h] = res
        return res

    # opponent replies at nodes the base edges don't cover come from the pool too
    try:
        worst, line = walk(chess.Board(), (), frozenset())
    finally:
        eng.quit()
    return {"worst_cp_us": worst, "line": " ".join(line), **stats}


if __name__ == "__main__":
    main()
