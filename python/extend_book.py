"""Finish forcing sequences at a book's edges (user request 2026-10-24; a quiescence extension).

A book can stop in the middle of a tactic: the opponent's main reply lands us in a position with one
good move (e.g. 1.e4 e5 2.Nf3 Nc6 3.Bc4 Nf6 4.d4 exd4 5.Ng5 d5 6.exd5 Na5 7.Qe2+ Be7 8.d6 Nxc4
9.Qxc4 Qxd6 10.Qxf7+ Kd8, where 11.Qxd7 is +4 and the common 11.O-O throws most of it away).

Walk the book; every our-turn position OUT of book, reached by an opponent reply chosen at least
--reply-share of the time from a position the book reaches, is a candidate. Analyse it (Stockfish
MultiPV 3, --depth) and add the engine's best move when the rule fires; then follow the opponent's
replies from there (games blended with Maia, as in the convertibility scorer) and repeat, at most
--max-ext of our moves deep. Rules:

  user   the 2nd-best move loses more than --gap2 cp, or the 3rd-best more than --gap3 cp
         (the user's proposal; a single legal move always fires)
  user-es  the same in expected score (--gap2-es / --gap3-es through the uniform curve), so a
         +8 position where +5 also wins is not "critical" but finishing a mate still is
  weak   the expected loss of NOT knowing the move is at least --min-loss: the best move's expected
         score minus the average over how 1000-1799 players move there (weak_edges counts blended
         with Maia at --our-elo), evals through the uniform curve. Asks "would a weaker player go
         wrong here?", so an obvious recapture is not added and a natural-looking blunder is.

Positions below --min-reach (how often a game against the book's population gets there) are skipped.
Output: <book>_ext_<rule>.parquet (the book plus the added moves, extension=True) and a report.

    PYTHONPATH=python .venv/Scripts/python.exe -X utf8 python/extend_book.py \\
        --book E:/chess/repertoire/final/lifetime_white_2026-10-24.parquet --colour white \\
        --pool E:/chess/position-stats/position_stats_pooled_ge1800_2013_2026_brc.parquet --opp-elo 2000 --rule weak
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import chess
import chess.engine
import duckdb
import numpy as np
import polars as pl

sys.path.insert(0, "python")
from convertibility_eval import Maia, blend  # noqa: E402
from eval_arrays import MISSING, lookup_evals, open_eval_arrays  # noqa: E402
from stage3_backwards_induction import LICHESS_CP_SCALE  # noqa: E402
from zobrist import zobrist_int64  # noqa: E402

SF = "C:/Users/David/AppData/Local/Microsoft/WinGet/Links/stockfish.exe"
WEAK = "E:/chess/position-stats/weak_edges_b1000_1800_on_ge1800.parquet"


def es(cp: float) -> float:
    return 1.0 / (1.0 + math.exp(-LICHESS_CP_SCALE * max(-1000.0, min(1000.0, cp))))


def analyse(job):
    """[(fen, depth, multipv)] -> [[(san, cp for the side to move), ...] best first]."""
    eng = chess.engine.SimpleEngine.popen_uci(SF)
    eng.configure({"Threads": 1, "Hash": 64})
    out = []
    try:
        for fen, depth, mpv in job:
            b = chess.Board(fen)
            infos = eng.analyse(b, chess.engine.Limit(depth=depth), multipv=mpv)
            if isinstance(infos, dict):
                infos = [infos]
            row = []
            for inf in infos:
                if "pv" not in inf or not inf["pv"]:
                    continue
                cp = inf["score"].pov(b.turn).score(mate_score=2000)
                row.append((b.san(inf["pv"][0]).rstrip("+#"), float(cp)))
            out.append(row)
    finally:
        eng.quit()
    return out


def run_engine(pool, items, depth, mpv, workers):
    jobs = [[(f, depth, mpv) for f in items[i::workers]] for i in range(workers)]
    res = pool.map(analyse, jobs)
    out = {}
    for i, part in enumerate(res):
        for f, r in zip(items[i::workers], part):
            out[f] = r
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--book", required=True)
    ap.add_argument("--colour", required=True, choices=["white", "black"])
    ap.add_argument("--pool", required=True, help="the opponent population (the book's own pool)")
    ap.add_argument("--opp-elo", type=int, default=2000)
    ap.add_argument("--our-elo", type=int, default=1450)
    ap.add_argument("--rule", choices=["user", "user-es", "weak", "combined"], default="combined")
    ap.add_argument("--gap2-es", type=float, default=0.15)
    ap.add_argument("--gap3-es", type=float, default=0.25)
    ap.add_argument("--gap2", type=float, default=200.0)
    ap.add_argument("--gap3", type=float, default=300.0)
    ap.add_argument("--min-loss", type=float, default=0.05)
    ap.add_argument("--reply-share", type=float, default=0.33)
    ap.add_argument("--max-ext", type=int, default=4)
    ap.add_argument("--min-reach", type=float, default=1e-4)
    ap.add_argument("--depth", type=int, default=18)
    ap.add_argument("--k", type=float, default=100.0)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out", default=None)
    ap.add_argument("--prefer-book", default=None,
                    help="a larger book this one must stay inside (e.g. the extended full book, for a "
                         "ladder stage): where the rule fires at a position it has a move for, use "
                         "that move instead of the engine's best; positions it has no move for are "
                         "not added (the stage stays nested)")
    a = ap.parse_args()
    t0 = time.time()
    our_white = a.colour == "white"
    bd = pl.read_parquet(a.book)
    book = {h: m.rstrip("+#") for h, s, m in bd.select("position_hash", "side_to_move", "best_move").iter_rows()
            if s == a.colour and m}
    ev0, eb0 = bd["event"][0], bd["elo_band"][0]
    prefer = None              # None = no --prefer-book; {} = one that allows nothing
    if a.prefer_book is not None:
        pdf = pl.read_parquet(a.prefer_book, columns=["position_hash", "side_to_move", "best_move"])
        prefer = {h: m.rstrip("+#") for h, s_, m in pdf.iter_rows() if s_ == a.colour and m}
        # the input book must already sit inside the larger one (follow-up review R6)
        clash = [h for h, m in book.items() if h not in prefer or prefer[h] != m]
        if clash:
            raise SystemExit(f"FATAL: {len(clash)} of the input book's decisions are missing from "
                             f"or differ in --prefer-book {a.prefer_book}; the stage cannot nest")
    con = duckdb.connect()
    con.execute("SET threads=8; SET memory_limit='12GB'")
    mm = open_eval_arrays()
    cache: dict[str, dict] = {"pool": {}, "weak": {}}

    def counts(src, hs):
        c = cache[src]
        need = [h for h in set(hs) if h not in c]
        if need:
            path = a.pool if src == "pool" else WEAK
            con.register("need", pl.DataFrame({"h": need}, schema={"h": pl.Int64}).to_arrow())
            df = con.execute(f"""SELECT parent_hash, move_san, SUM(total)::DOUBLE n FROM read_parquet('{path}')
                WHERE parent_hash IN (SELECT h FROM need) GROUP BY 1, 2""").pl()
            con.unregister("need")
            for h in need:
                c[h] = {}
            for p, m, n in df.iter_rows():
                k = m.rstrip("+#")
                c[p][k] = c[p].get(k, 0.0) + n
        return [c[h] for h in hs]

    # 1. walk the book: reach by the book's moves and the pool's replies; collect the candidates
    root = chess.Board()
    reach = {zobrist_int64(root): 1.0}
    frontier = {zobrist_int64(root): root}
    seen = set()
    cands: dict[int, list] = {}        # h -> [board, reach, depth]
    while frontier:
        hs = list(frontier)
        cs = counts("pool", hs)
        nxt = {}
        for h, c in zip(hs, cs):
            b = frontier[h]
            seen.add(h)
            r = reach[h]
            if (b.turn == chess.WHITE) == our_white:
                m = book.get(h)
                if m is None:
                    continue
                b2 = b.copy(stack=False)
                b2.push_san(m)
                ch = zobrist_int64(b2)
                if r > reach.get(ch, 0.0):
                    reach[ch] = r
                if ch not in seen:
                    nxt[ch] = b2
            else:
                tot = sum(c.values())
                for m, n in c.items():
                    sh = n / tot if tot else 0.0
                    if sh < 0.002:
                        continue
                    b2 = b.copy(stack=False)
                    try:
                        b2.push_san(m)
                    except ValueError:
                        continue
                    ch = zobrist_int64(b2)
                    rr = r * sh
                    if ch in book:
                        if rr > reach.get(ch, 0.0):
                            reach[ch] = rr
                        if ch not in seen:
                            nxt[ch] = b2
                    elif sh >= a.reply_share and rr >= a.min_reach and not b2.is_game_over():
                        if ch not in cands or rr > cands[ch][1]:
                            cands[ch] = [b2, rr, 0]
        frontier = {h: b for h, b in nxt.items() if h not in seen}
    print(f"book walk: {len(seen):,} positions; {len(cands):,} candidate edge positions "
          f"(reply >= {a.reply_share:.0%}, reach >= {a.min_reach:g}) ({time.time() - t0:.0f}s)", flush=True)

    # 2. extend, a depth level at a time
    maia = Maia()
    added: dict[int, dict] = {}
    stats = {"analysed": 0, "fired": 0, "by_depth": {}}
    with Pool(a.workers) as pool:
        level = cands
        for depth in range(a.max_ext):
            if not level:
                break
            fens = {h: v[0].fen() for h, v in level.items()}
            top = run_engine(pool, sorted(set(fens.values())), a.depth, 3, a.workers)
            stats["analysed"] += len(level)
            decide = {}
            gap_fire = {}
            for h, (b, r, _d) in level.items():
                t = top.get(fens[h]) or []
                if not t:
                    continue
                nlegal = b.legal_moves.count()
                if a.rule == "user":
                    g2 = len(t) >= 2 and t[0][1] - t[1][1] > a.gap2
                    g3 = len(t) >= 3 and t[0][1] - t[2][1] > a.gap3
                else:      # the same idea in expected score: a +8 position where +5 also wins is not critical
                    g2 = len(t) >= 2 and es(t[0][1]) - es(t[1][1]) >= a.gap2_es
                    g3 = len(t) >= 3 and es(t[0][1]) - es(t[2][1]) >= a.gap3_es
                if nlegal == 1 or g2 or g3:
                    gap_fire[h] = {"best_cp": t[0][1], "second_cp": t[1][1] if len(t) > 1 else None,
                                   "third_cp": t[2][1] if len(t) > 2 else None}
            if a.rule in ("user", "user-es"):
                decide = {h: (top[fens[h]][0][0], info) for h, info in gap_fire.items()}
            else:
                hs = list(level)
                wc = counts("weak", hs)
                mp = maia.probs([level[h][0].epd() for h in hs], a.our_elo, a.opp_elo, hs)
                need_child, dists = {}, {}
                for h, c, m in zip(hs, wc, mp):
                    t = top.get(fens[h]) or []
                    if not t:
                        continue
                    dist = blend(c, m, a.k, 0.01)
                    dists[h] = dist
                    b = level[h][0]
                    known = {s: cp for s, cp in t}
                    for s in dist:
                        if s in known:
                            continue
                        b2 = b.copy(stack=False)
                        try:
                            b2.push_san(s)
                        except ValueError:
                            continue
                        need_child[(h, s)] = b2
                # child evals: eval arrays first (White-view), else Stockfish depth 12
                keys = list(need_child)
                ch_h = np.array([zobrist_int64(need_child[k]) for k in keys], dtype=np.int64)
                ev = lookup_evals(ch_h, *mm) if len(keys) else []
                child_cp, todo = {}, []
                for k, e in zip(keys, ev):
                    b2 = need_child[k]
                    if e != MISSING:
                        child_cp[k] = float(e) if our_white else -float(e)   # our view
                    elif b2.is_game_over():
                        res = b2.result()
                        child_cp[k] = 0.0 if res == "1/2-1/2" else (2000.0 if (res == "1-0") == our_white else -2000.0)
                    else:
                        todo.append(k)
                if todo:
                    r12 = run_engine(pool, sorted({need_child[k].fen() for k in todo}), 12, 1, a.workers)
                    for k in todo:
                        rr = r12.get(need_child[k].fen()) or []
                        if rr:
                            child_cp[k] = -rr[0][1]                         # side to move = opponent
                for h in hs:
                    t = top.get(fens[h]) or []
                    if not t or h not in dists:
                        continue
                    known = {s: cp for s, cp in t}
                    exp = tot = 0.0
                    for s, p in dists[h].items():
                        cp = known.get(s, child_cp.get((h, s)))
                        if cp is None:
                            continue
                        exp += p * es(cp)
                        tot += p
                    if tot <= 0:
                        continue
                    loss = es(t[0][1]) - exp / tot
                    top_weak = max(dists[h].items(), key=lambda kv: kv[1])
                    info = {"best_cp": t[0][1], "loss": loss, "weak_top": top_weak[0],
                            "weak_top_p": top_weak[1], "few_good_moves": h in gap_fire}
                    # weak: the loss alone. combined: few good moves (position-intrinsic, so a
                    # forcing continuation is booked even where the players who reach it -- having
                    # played the tactic themselves -- find it) OR the loss
                    if loss >= a.min_loss or (a.rule == "combined" and h in gap_fire):
                        decide[h] = (t[0][0], info)
                if a.rule == "combined":
                    for h, info in gap_fire.items():       # few good moves but no weak data at all
                        if h not in decide:
                            decide[h] = (top[fens[h]][0][0], {**info, "few_good_moves": True})
            stats["by_depth"][depth] = {"candidates": len(level), "added": len(decide)}
            stats["fired"] += len(decide)
            # the opponent's replies after each added move: games blended with Maia
            nxt_pos = {}
            for h, (san, info) in decide.items():
                if prefer is not None and h not in prefer:
                    # 2026-10-31 (review): the stage must stay INSIDE the larger book; the
                    # White starter's ext_c10 had 3 moves its full ext_c10 lacks
                    stats["outside_prefer"] = stats.get("outside_prefer", 0) + 1
                    continue
                if prefer is not None:
                    info = {**info, "engine_best": san, "from_prefer_book": True}
                    san = prefer[h]
                b, r, _d = level[h]
                b2 = b.copy(stack=False)
                b2.push_san(san)
                # store full SAN (check / mate marks), as the pool and the books spell it: the
                # explorer matches the book's move to the pool's moves by string
                added[h] = {"board": b, "san": b.san(b.parse_san(san)), "reach": r, "depth": depth, **info}
                if not b2.is_game_over():
                    nxt_pos[zobrist_int64(b2)] = (b2, r)
            if not nxt_pos or depth + 1 >= a.max_ext:
                break
            phs = list(nxt_pos)
            pc = counts("pool", phs)
            pm = maia.probs([nxt_pos[h][0].epd() for h in phs], a.opp_elo, a.our_elo, phs)
            level = {}
            for h, c, m in zip(phs, pc, pm):
                b2, r = nxt_pos[h]
                for s, p in blend(c, m, a.k, 0.01).items():
                    if p < a.reply_share or r * p < a.min_reach:
                        continue
                    b3 = b2.copy(stack=False)
                    try:
                        b3.push_san(s)
                    except ValueError:
                        continue
                    ch = zobrist_int64(b3)
                    if ch in book or ch in added or b3.is_game_over():
                        continue
                    if ch not in level or r * p > level[ch][1]:
                        level[ch] = [b3, r * p, depth + 1]
            print(f"  depth {depth}: {stats['by_depth'][depth]}; next {len(level):,} ({time.time() - t0:.0f}s)",
                  flush=True)
    maia.close()

    # 3. write the extended book and a report
    out = Path(a.out or str(Path(a.book).with_suffix("")) + f"_ext_{a.rule}.parquet")
    new = pl.DataFrame([{"position_hash": h, "position_epd": v["board"].epd(), "event": ev0, "elo_band": eb0,
                         "side_to_move": a.colour, "best_move": v["san"], "alloc_budget": 0}
                        for h, v in added.items()],
                       schema={"position_hash": pl.Int64, "position_epd": pl.Utf8, "event": bd.schema["event"],
                               "elo_band": bd.schema["elo_band"], "side_to_move": pl.Utf8,
                               "best_move": pl.Utf8, "alloc_budget": bd.schema["alloc_budget"]})
    # an added move replaces the book's own row there (it recorded the position with no move)
    ext = pl.concat([bd.filter(~pl.col("position_hash").is_in(list(added))).with_columns(pl.lit(False).alias("extension")),
                     new.with_columns(pl.lit(True).alias("extension"))], how="diagonal_relaxed")
    tmp = out.with_suffix(".parquet.tmp")
    ext.write_parquet(tmp)
    tmp.replace(out)
    rep = sorted(added.values(), key=lambda v: -v["reach"])
    print(json.dumps({"rule": a.rule, "book_moves": len(book), "added": len(added), **stats,
                      "secs": round(time.time() - t0)}))
    print("largest additions (reach, position after, move, details):")
    for v in rep[:25]:
        det = {k: (round(x, 3) if isinstance(x, float) else x) for k, x in v.items()
               if k not in ("board", "san", "reach", "depth")}
        print(f"  {v['reach']:.4f}  d{v['depth']}  {v['board'].fen().split(' ')[0]} {'w' if v['board'].turn else 'b'}"
              f"  ->  {v['san']}  {det}")
    print(f"-> {out}")


if __name__ == "__main__":
    sys.exit(main())
