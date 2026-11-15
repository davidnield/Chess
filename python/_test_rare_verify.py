"""Rare-move verification: unverified rare moves cannot bind a gate, verified ones can, and the
builder, validator and verifier agree on the menu. Both colours throughout.

Cases: the Petroff (9...O-O restored, 9...f5 out at 30 cp); a false rare displacement; a restored
rival that was neither selected nor engine-nominated; a transposed board; old books unchanged."""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

import chess
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import budget_core as bc                                    # noqa: E402
import cloud_feedback as fb                                 # noqa: E402
import rare_verify as rv                                    # noqa: E402
from stage3_backwards_induction import cp_to_expected_score  # noqa: E402
from validate_book import classify                          # noqa: E402
from zobrist import zobrist_int64                           # noqa: E402

FAILS = []


def check(name, ok, detail=""):
    print(f"  [{'ok ' if ok else 'FAIL'}] {name}" + (f" -- {detail}" if detail and not ok else ""))
    if not ok:
        FAILS.append(name)


def ws(cp_us, white):
    """A White-view expected score for a cp from OUR side's view."""
    return cp_to_expected_score(cp_us if white else -cp_us)


def menu(h, eval_ws, white, moves, own=None):
    """edges + evals: moves = [(san, games, cp from our view)]."""
    es = []
    for i, (san, n, cp) in enumerate(moves):
        ch = h * 100 + i + 1
        es.append({"move_san": san, "child_hash": ch, "total": n, "white_score_avg": 0.5})
        if cp is not None:
            eval_ws[ch] = ws(cp, white)
    if own is not None:
        eval_ws[h] = ws(own, white)
    return es


def kept(h, es, ev, white, limit, **kw):
    return sorted(e["move_san"] for e, _ in bc.gate_candidates(
        h, es, None, ev, white, 0.5, max_eval_loss=limit, **kw))


for white in (True, False):
    side = "white" if white else "black"
    print(side)

    # 1. The Petroff's shape: the common move is 50 cp below the position, a 12-game move is 5
    # below it, a one-game move is hopeless. No verification is needed to get this one right.
    ev = {}
    es = menu(11, ev, white, [("f5", 28, -100), ("O-O", 12, -55), ("Bf5", 1, -248)], own=-50)
    floored = [e for e in es if e["total"] >= 20]
    check("Petroff, old 20-game menu: only the common move is on offer",
          kept(11, floored, ev, white, 30) == ["f5"])
    check("Petroff, naive uncut menu: castling, and not f5, at 30 cp",
          kept(11, es, ev, white, 30) == ["O-O"])
    disputes = {}
    check("Petroff before verification: castling restored, and the old fallback is not yet pruned",
          kept(11, es, ev, white, 30, rare_floor=20, disputes=disputes) == ["O-O", "f5"]
          and disputes[11].get("fallback") and disputes[11]["at_risk"] == ["f5"])
    check("Petroff once castling is verified: castling only",
          kept(11, es, ev, white, 30, rare_floor=20, verified={11: {"O-O"}}) == ["O-O"])
    check("Petroff at 60 cp: both", kept(11, es, ev, white, 60, rare_floor=20) == ["O-O", "f5"])

    # 2. A false displacement: a two-game move stored 70 cp above the common one and 65 above
    # its own position. Unverified it cannot exclude the common move, but stays a candidate.
    ev = {}
    es = menu(21, ev, white, [("Nc6", 100, 50), ("Qc5", 2, 120), ("a6", 300, 10)], own=55)
    check("naive uncut rule: the rare move excludes the sound one",
          kept(21, es, ev, white, 30) == ["Qc5"])
    disputes = {}
    check("rare move does not bind: common move kept, rare move still a candidate",
          kept(21, es, ev, white, 30, rare_floor=20, disputes=disputes) == ["Nc6", "Qc5"])
    d = disputes.get(21, {})
    check("the dispute is recorded with the threat and the rival at risk",
          [m for m, _ in d.get("threats", [])] == ["Qc5"] and d.get("at_risk") == ["Nc6"], str(d))
    # verified and refuted: its fresh score replaces the stored one and it leaves on the merits
    ev2 = dict(ev)
    ev2[21 * 100 + 2] = ws(20, white)
    check("verified and refuted: the common move stays, the rare move goes",
          kept(21, es, ev2, white, 30, rare_floor=20, verified={21: {"Qc5"}}) == ["Nc6"])
    # verified and confirmed: now it binds like any other move
    ev3 = dict(ev)
    ev3[21] = ws(120, white)
    check("verified and confirmed: the rare move binds and the common one is out",
          kept(21, es, ev3, white, 30, rare_floor=20, verified={21: {"Qc5"}}) == ["Qc5"])
    check("an engine-nominated move binds without a games count",
          kept(21, es + [{"move_san": "Kf1", "child_hash": 2199, "total": 0, "engine": True}],
               {**ev, 2199: ws(130, white)}, white, 30, rare_floor=20) == ["Kf1", "Qc5"])
    # the relative expected-score gate takes the same rule
    ev4 = {}
    es4 = menu(22, ev4, white, [("Nc6", 100, 0), ("Qc5", 2, 600)])
    check("relative gate: an unverified rare sibling is not the baseline",
          sorted(e["move_san"] for e, _ in bc.gate_candidates(22, es4, None, ev4, white, 0.5,
                                                              rare_floor=20)) == ["Nc6", "Qc5"])
    check("relative gate, old rule: it was",
          sorted(e["move_san"] for e, _ in bc.gate_candidates(22, es4, None, ev4, white, 0.5))
          == ["Qc5"])

    # 3. Validator and builder agree, and the validator sees a restored rival that the book
    # neither chose nor had nominated.
    meta = {"slice_prior": 0.5, "max_eval_loss": 30, "rare_floor": 20, "gates": True}
    ev = {}
    es = menu(31, ev, white, [("Nc6", 100, 50), ("Bd3", 6, 98)], own=100)
    check("validator: a restored rare prescription is in the set",
          classify(31, "Bd3", es, None, ev, white, meta, None, "", None, 20, {}) == "in_set")
    check("validator on the floored menu alone would call it outside",
          classify(31, "Bd3", [e for e in es if e["total"] >= 20], None, ev, white,
                   {**meta, "rare_floor": None}, None, "", None) == "outside_set")
    check("validator: unverified, the trusted fallback is still in the set",
          classify(31, "Nc6", es, None, ev, white, meta, None, "", None, 20, {}) in ("in_set", "fallback"))
    check("validator: with the rare move verified, the common move is out",
          classify(31, "Nc6", es, None, ev, white, meta, None, "", None, 20, {31: {"Bd3"}})
          == "outside_set")
    ev = {}
    es = menu(32, ev, white, [("Nc6", 100, 50), ("Bd3", 6, 98)], own=55)
    check("validator: unverified restored rival does not disqualify the booked move",
          classify(32, "Nc6", es, None, ev, white, meta, None, "", None, 20, {}) == "in_set")
    check("validator: once verified, the unchosen restored rival decides the gate",
          classify(32, "Nc6", es, None, {**ev, 32: ws(98, white)}, white, meta, None, "", None,
                   20, {32: {"Bd3"}}) == "outside_set")

    # 4. Through build_graph and both optimisers: the dispute is recorded, nothing is pruned,
    # and after verification the book's move follows the verified scores.
    start = chess.Board()
    if not white:
        start.push_san("e4")
    root = zobrist_int64(chess.Board())
    h = zobrist_int64(start)
    good, rare = ("d4", "a4") if white else ("e5", "a5")

    def child(san):
        b = start.copy()
        b.push_san(san)
        return zobrist_int64(b)

    ev = {h: ws(30, white), child(good): ws(25, white), child(rare): ws(140, white)}
    edges = {h: [{"move_san": good, "child_hash": child(good), "total": 500,
                  "white_score_avg": 0.5, "parent_epd": start.epd()},
                 {"move_san": rare, "child_hash": child(rare), "total": 3, "white_score_avg": 0.5}]}
    if not white:
        edges[root] = [{"move_san": "e4", "child_hash": h, "total": 100, "white_score_avg": 0.5,
                        "parent_epd": chess.Board().epd()}]
    kw = dict(rep_moves={}, strict_source=True, gated_reach=True, max_eval_loss=30,
              prior_strength=1)
    g = bc.build_graph(edges, root, white, eval_ws=dict(ev), **kw)
    check("old rule through build_graph: only the rare move survives",
          [c[0] for c in g.our[h].cands] == [rare])
    g = bc.build_graph(edges, root, white, eval_ws=dict(ev), rare_floor=20, verified={}, **kw)
    check("graph keeps both moves and records the dispute",
          sorted(c[0] for c in g.our[h].cands) == sorted([good, rare])
          and list(g.meta["rare_disputes"]) == [h])
    g.our[h].l_node = 0.1
    curves, _ = bc.build_curves(g, bmax=1)
    first = bc.extract_book(g, curves, 1)["booked"].get(h)
    check("unverified, the optimiser may still take the rare move on its stored value",
          first == rare)
    with tempfile.TemporaryDirectory() as td:
        # the verifier's overlay: the rare move searched at -40, the common one at +25, root +25
        sign = 1 if white else -1
        rows = [dict(position_hash=h, position_epd=start.epd(), move_san=m, child_hash=child(m),
                     child_cp_white=sign * cp, nodes=5, depth=20, engine="t", searched="d",
                     reason="rare prescription", cloud_origin=False, parent_cp_white=sign * 25,
                     verified_loss_cp=25 - cp, verification_nodes=5, verification_best=good,
                     verification_free_cp_white=sign * 25, verification_unstable=False,
                     verification_engine_sha256="x", transposition_cp_white=None)
                for m, cp in ((good, 25), (rare, -40))]
        path = Path(td) / "overlay.parquet"
        pl.DataFrame(rows, schema=fb.SCHEMA).write_parquet(path)
        ev2 = dict(ev)
        moves, _ = bc.load_engine_candidates(path, ev2)
        ver = bc.verified_moves(pl.read_parquet(path))
        excl = bc.verified_exclusions(path, 30)
        g = bc.build_graph(edges, root, white, eval_ws=ev2, engine_moves=moves, excluded=excl,
                           rare_floor=20, verified=ver, **kw)
        g.our[h].l_node = 0.1
        curves, _ = bc.build_curves(g, bmax=1)
        check("after verification both optimisers book the sound move",
              bc.extract_book(g, curves, 1)["booked"].get(h) == good
              and bc.price_search(g, 1, log=lambda m: None)[0]["booked"].get(h) == good)
        check("and no engine candidate was added: the menu is the observed moves",
              not g.meta.get("engine_cands"))

        # 5. A transposed board: two parents reach X by rare moves with different incoming
        # scores. X takes one value everywhere; each move's loss stays with its own parent.
        X = 777
        t = [dict(rows[0], position_hash=p, position_epd="e", move_san=m, child_hash=X,
                  child_cp_white=sign * cp, parent_cp_white=sign * root_cp,
                  verified_loss_cp=root_cp - cp)
             for p, m, cp, root_cp in ((101, "Ra1", 10, 50), (102, "Rb1", 30, 40))]
        path = Path(td) / "trans.parquet"
        pl.DataFrame(t, schema=fb.SCHEMA).write_parquet(path)
        for order in (t, t[::-1]):
            pl.DataFrame(order, schema=fb.SCHEMA).write_parquet(path)
            evt = {}
            bc.load_engine_candidates(path, evt)
            check("transposed board has one value, independent of row order",
                  round(bc.ws_to_cp(evt[X], white)) == 20)
        check("the verified loss stays with its parent and move",
              bc.verified_exclusions(path, 30) == {101: {"Ra1"}})

    # 6. The verifier's plan: a booked rare prescription and a booked disputed gate are asked
    # for with their rivals; an unbooked dispute only as frontier work.
    book = pl.DataFrame({"position_hash": [41, 42], "position_epd": ["x", "y"],
                         "side_to_move": [side, side], "best_move": ["Bd3", "Nc6"]})
    cps = {}
    eg = {41: menu(41, cps, True, [("Nc6", 100, 50), ("Bd3", 6, 60)]),
          42: menu(42, cps, True, [("Nc6", 100, 50), ("Qc5", 2, 120)]),
          43: menu(43, cps, True, [("Nc6", 100, 50), ("Qc5", 2, 120)])}
    cps = {k: (1 if white else -1) * round(bc.ws_to_cp(v, True)) for k, v in cps.items()}
    dis = [{"position_hash": 42, "reach": .1, "threats": [["Qc5", 120]], "at_risk": ["Nc6"]},
           {"position_hash": 43, "reach": .01, "epd": "z", "threats": [["Qc5", 120]],
            "at_risk": ["Nc6"]}]
    empty = pl.DataFrame(schema=fb.SCHEMA)
    m = {"rare_floor": 20}
    want = rv.plan(book, m, empty, side, cps, eg, dis)
    check("plan: rare prescription with its trusted rival; disputed gate with threat and booked",
          {h: (r, mv) for h, (r, _, mv) in want.items()}
          == {41: ("rare prescription", ["Bd3", "Nc6"]), 42: ("disputed gate", ["Nc6", "Qc5"])},
          str(want))
    want = rv.plan(book, m, empty, side, cps, eg, dis, frontier_reach=.005, frontier=5)
    check("plan: an unbooked dispute joins only as frontier work",
          want.get(43, (None,))[0] == "frontier dispute")
    done = pl.DataFrame([dict(rows[0], position_hash=41, move_san=s) for s in ("Bd3", "Nc6")]
                        + [dict(rows[0], position_hash=42, move_san=s) for s in ("Qc5", "Nc6")],
                        schema=fb.SCHEMA)
    check("plan: nothing is asked twice", rv.plan(book, m, done, side, cps, eg, dis) == {})

# 7. Older books: no lookup recorded -> the floored pool, and no rare policy.
import book_graph                                            # noqa: E402
check("a book without the lookup reads its floored pool",
      book_graph.edge_source({"stats": "E:\\p.parquet", "min_games": 20}, "black")
      == ("E:/p.parquet", "s.total >= 20"))
check("and has no rare policy", book_graph.rare_policy({"min_games": 20}) == (None, {}))

# 8. The search cache: one unrestricted search per parent, restricted searches only for moves
# not yet cached, reusable by a later request (another gate or book).
with tempfile.TemporaryDirectory() as td:
    calls = []

    def fake(self, board, move=None):
        calls.append(None if move is None else move)
        self.name = "fake"
        return {"san": "e4" if move is None else move, "cp": 20, "depth": 9, "nodes": 7}

    binary = Path(td) / "sf"
    binary.write_text("x")
    with patch.object(rv.ec, "SF", str(binary)), patch.object(rv.Searches, "_one", fake):
        s = rv.Searches(Path(td) / "cache", 7, 1)
        b = chess.Board()
        s.get(zobrist_int64(b), b.epd(), ["d4"])
        n1 = len(calls)
        s.get(zobrist_int64(b), b.epd(), ["d4", "c4"])
        check("search cache: free + two restricted, then only the new move",
              n1 == 3 and len(calls) == 4 and s.stats["parent_cache_hits"] == 1, str(calls))
        saved = json.loads((Path(td) / "cache" / f"{zobrist_int64(b)}.json").read_text())
        rows = list(rv.rows_for(zobrist_int64(b), saved,
                                {"d4": {"move_san": "d4", "child_hash": 5}}, 30, 7, "sha", "r"))
        check("overlay rows cover observed moves only, with the root as the parent score",
              [r["move_san"] for r in rows] == ["d4"] and rows[0]["parent_cp_white"] == 20
              and rows[0]["verified_loss_cp"] == 0)


# ── Review fixes, 2026-11-10 ────────────────────────────────────────────────────────────────
import run_rare_book as rrb                                  # noqa: E402
import uncut_own_moves as uom                                # noqa: E402


def fake_engine(table, log):
    """A Searches._one that answers from {move or None: cp from the mover's side}."""
    def one(self, board, move=None):
        log.append(move)
        self.name = "fake"
        sign = 1 if board.turn else -1
        san = move if move is not None else table[None][0]
        cp = table[None][1] if move is None else table[rv.bare(move)]
        return {"san": san, "cp": sign * cp, "depth": 9, "nodes": self.nodes}
    return one


for white in (True, False):
    side = "white" if white else "black"
    sign = 1 if white else -1
    print(side, "(review fixes)")
    start = chess.Board()
    if not white:
        start.push_san("e4")
    h = zobrist_int64(start)
    names = ("e4", "d4", "Nf3", "a3") if white else ("e5", "d5", "Nf6", "a6")
    main, second, risky, rare = names

    def kid(san):
        b = start.copy()
        b.push_san(san)
        return zobrist_int64(b)

    def edges_of(rows):
        return [{"move_san": m, "child_hash": kid(m), "total": n, "white_score_avg": .5}
                for m, n, _ in rows]

    # 9. The rival facing exclusion is itself searched. Parent +60; the booked move +60 (100
    # games), a second +55 (70), a third +35 (30), and a one-game move stored at +85.
    rows = [(main, 100, 60), (second, 70, 55), (risky, 30, 35), (rare, 1, 85)]
    es = edges_of(rows)
    ev = {kid(m): ws(cp, white) for m, _, cp in rows} | {h: ws(60, white)}
    dis = {}
    before = sorted(e["move_san"] for e, _ in bc.gate_candidates(
        h, es, None, ev, white, 0.5, max_eval_loss=30, rare_floor=20, disputes=dis))
    check("the rare move threatens exactly the third move",
          before == sorted(names) and dis[h]["at_risk"] == [risky]
          and [m for m, _ in dis[h]["threats"]] == [rare], str(dis))
    book = pl.DataFrame({"position_hash": [h], "position_epd": [start.epd()],
                         "side_to_move": [side], "best_move": [main]})
    cps = {k: round(bc.ws_to_cp(v, True)) for k, v in ev.items()}
    listed = [{"position_hash": h, "reach": .2, "booked": main, **dis[h]}]
    meta = {"rare_floor": 20, "max_eval_loss": 30}
    empty = pl.DataFrame(schema=fb.SCHEMA)
    want = rv.plan(book, meta, empty, side, cps, {h: es}, listed)
    check("plan: the at-risk rival is searched with the threat, the booked move and a rival",
          want[h][2] == sorted(names), str(want))
    off = rv.plan(book.clear(), meta, empty, side, cps, {h: es},
                  [dict(listed[0], booked=None, epd=start.epd())], frontier_reach=.01, frontier=5)
    check("plan, off book: the frontier asks for the at-risk rival too",
          risky in off[h][2] and rare in off[h][2], str(off))
    # fresh: the rare move holds at +85, and the third move is really +70 (stored +35)
    fresh = {None: (rare, 85), rare: 85, main: 75, second: 30, risky: 70}
    with tempfile.TemporaryDirectory() as td:
        calls = []
        binary = Path(td) / "sf"
        binary.write_text("x")
        with patch.object(rv.ec, "SF", str(binary)), \
                patch.object(rv.Searches, "_one", fake_engine(fresh, calls)):
            s5 = rv.Searches(Path(td) / "cache", 5, 1)
            saved = s5.get(h, start.epd(), want[h][2])
            n1 = len(calls)
            s5.get(h, start.epd(), want[h][2])
            check("the at-risk rival gets its own restricted search, reused on a second pass",
                  risky in calls and len(calls) == n1 and s5.stats["restricted_cache_hits"] >= 4,
                  str(calls))
        menu_ = {rv.bare(e["move_san"]): e for e in es}
        check("no boundary verdicts here, so no deeper search", rv.boundary(saved, 30) == [])
        new = list(rv.rows_for(h, saved, menu_, 30, 5, "sha", "disputed gate"))
        path = Path(td) / "o.parquet"
        pl.DataFrame(new, schema=fb.SCHEMA).write_parquet(path)
        ev2 = dict(ev)
        bc.load_engine_candidates(path, ev2)
        ver = bc.verified_moves(pl.read_parquet(path))
        veto = bc.verified_exclusions(path, 30)
        after = sorted(e["move_san"] for e, _ in bc.gate_candidates(
            h, [e for e in es if e["move_san"] not in veto.get(h, ())], None, ev2, white, 0.5,
            max_eval_loss=30, rare_floor=20, verified=ver))
        check("with its fresh score the rival survives; the move 55 cp down goes",
              after == sorted([main, risky, rare]) and veto == {h: {second}}, f"{after} {veto}")
        stale = dict(ev2) | {kid(risky): ev[kid(risky)]}
        cut = sorted(e["move_san"] for e, _ in bc.gate_candidates(
            h, es, None, stale, white, 0.5, max_eval_loss=30, rare_floor=20,
            verified={h: {rare, main, second}}))
        check("left on its stored score it would have been cut by the fresh reference",
              risky not in cut, str(cut))
        # 9b. The comparison set comes from the recipe and the evidence, never from the cache.
        def answers(table):                  # mover-side cp by move; None -> the free choice
            def search(moves, first):
                got = set(moves) | {table[None]}
                return {"free": {"san": table[None]},
                        "moves": {m: {"cp": sign * table[m]} for m in got}}
            return search

        def edge(m, n):
            return {"move_san": m, "child_hash": kid(m), "total": n, "white_score_avg": .5}
        mn = {main: edge(main, 100), second: edge(second, 70), risky: edge(risky, 30),
              rare: edge(rare, 1)}
        st = {kid(main): sign * 60, kid(second): sign * 45, kid(risky): 0, kid(rare): sign * 70}
        # the rare move really is +90: the common move at +45 would be cut by the new reference
        use, _ = rv.comparison_set(answers({None: rare, rare: 90, main: 60, second: 45}),
                                   [rare], (), mn, st, 20, 30, sign)
        check("set: a common move the stored scores admit is searched with the threat",
              use == {rare, main, second}, str(use))
        use, _ = rv.comparison_set(
            answers({None: main, rare: -20, main: 10, second: 5, risky: -5}),
            [rare], (), mn, st, 20, 30, sign)
        check("set: a common move a lower fresh reference would now admit joins too",
              use == set(names), str(use))
        calls = []
        with patch.object(rv.ec, "SF", str(binary)), patch.object(
                rv.Searches, "_one", fake_engine({None: (rare, 90), rare: 90, main: 60,
                                                  second: 45}, calls)):
            sA = rv.Searches(Path(td) / "A", 5, 1)
            useA, savedA = rv.comparison_set(
                lambda ms, first: sA.get(h, start.epd(), ms, count=first), [rare], (), mn, st,
                20, 30, sign)
        got = {rv.bare(r["move_san"]): r["verified_loss_cp"] for r in rv.rows_for(
            h, savedA, mn, 30, 5, "sha", "frontier dispute", moves=useA)}
        frameA = pl.DataFrame(list(rv.rows_for(h, savedA, mn, 30, 5, "sha", "frontier dispute",
                                               moves=useA)), schema=fb.SCHEMA)
        check("the rival gets its verdict in the pass that made the reference, booked or not",
              got == {rare: 0, main: 30, second: 45}
              and second in bc.verified_moves(frameA)[h]
              and rv.plan(book.clear(), meta, frameA, side, cps, {h: es}, []) == {}, str(got))

        # the same request against a cold cache and one that already holds an unrelated search
        st2 = {kid(main): 0, kid(second): sign * -10, kid(risky): sign * -200}
        mn2 = {m: mn[m] for m in (main, second, risky)}
        table = {None: (main, 0), main: 0, second: -10, risky: 45}

        def ask(cache, deep_cache=None):
            n0 = len(calls)
            base_ = rv.Searches(cache, 5, 1)
            use_, saved_ = rv.comparison_set(
                lambda ms, first: base_.get(h, start.epd(), ms, count=first), [main, second], (),
                mn2, st2, 20, 30, sign)
            deep_ = None
            if deep_cache is not None:
                deep_ = rv.Searches(deep_cache, 20, 1, free=False).get(h, start.epd(), sorted(use_))
            rows_ = [(r["move_san"], r["verified_loss_cp"], r["parent_cp_white"])
                     for r in rv.rows_for(h, saved_, mn2, 30, 5, "sha", "r", deep_, 20, use_)]
            return use_, rv.boundary(saved_, 30, moves=use_), rows_, len(calls) - n0
        with patch.object(rv.ec, "SF", str(binary)), patch.object(
                rv.Searches, "_one", fake_engine(table, calls)):
            rv.Searches(Path(td) / "warm", 5, 1).get(h, start.epd(), [risky])
            rv.Searches(Path(td) / "warm-deep", 20, 1, free=False).get(h, start.epd(), [risky])
            cold, warm = ask(Path(td) / "cold"), ask(Path(td) / "warm")
            cold_d = ask(Path(td) / "cold", Path(td) / "cold-deep")
            warm_d = ask(Path(td) / "warm", Path(td) / "warm-deep")
        check("cache history: an unrelated cached search does not enter the comparison",
              cold[:3] == warm[:3] and cold[0] == {main, second} and cold[1] == []
              and dict((m, l) for m, l, _ in cold[2]) == {main: 0, second: 10},
              f"{cold} {warm}")
        check("cache history: the warm request costs fewer searches", warm[3] < cold[3],
              f"{cold[3]} {warm[3]}")
        check("cache history: the same through the deeper tier", cold_d[2] == warm_d[2] == cold[2])
        # cap order: 60 then 30 against 30 then 60, fixed answers
        st3 = {kid(main): 0, kid(second): sign * -10, kid(risky): sign * -55}
        table3 = {None: (main, 0), main: 0, second: -10, risky: -40}

        def sweep(cache, caps):
            out = {}
            for cap in caps:
                sv = rv.Searches(cache, 5, 1)
                u, sd = rv.comparison_set(
                    lambda ms, first: sv.get(h, start.epd(), ms, count=first), [main, second], (),
                    mn2, st3, 20, cap, sign)
                out[cap] = (sorted(u), sorted((r["move_san"], r["verified_loss_cp"])
                                              for r in rv.rows_for(h, sd, mn2, cap, 5, "sha", "r",
                                                                   moves=u)))
            return out
        with patch.object(rv.ec, "SF", str(binary)), patch.object(
                rv.Searches, "_one", fake_engine(table3, calls)):
            x, y = sweep(Path(td) / "x", (60, 30)), sweep(Path(td) / "y", (30, 60))
        check("cap order: each gate's set and verdicts are the same whichever ran first",
              x == y and x[30][0] == sorted([main, second])
              and x[60][0] == sorted([main, second, risky]), f"{x} {y}")

    # 10. No trusted reference: no eval for the position, every move rare. Nothing is cut on
    # the best unverified score, by either relative filter, and the uncertainty is recorded.
    for label, hi, kw in (("loss cap", 80, dict(max_eval_loss=30)),
                          ("relative gate", 200, dict(max_eval_loss=None))):
        es2 = edges_of([(main, 1, hi), (second, 10, 0)])
        evn = {kid(main): ws(hi, white), kid(second): ws(0, white)}
        naive = [e["move_san"] for e, _ in bc.gate_candidates(h, es2, None, evn, white, 0.5, **kw)]
        dn = {}
        both = sorted(e["move_san"] for e, _ in bc.gate_candidates(
            h, es2, None, evn, white, 0.5, rare_floor=20, disputes=dn, **kw))
        check(f"no reference ({label}): the old rule keeps only the one-game move",
              naive == [main], str(naive))
        check(f"no reference ({label}): both survive and the comparison is recorded unresolved",
              both == sorted([main, second]) and dn[h].get("unresolved")
              and dn[h]["at_risk"] == [second] and dn[h]["threats"][0][0] == main, str(dn))
    es2 = edges_of([(main, 1, 80), (second, 10, 0)])
    for e in es2:
        e["parent_epd"] = start.epd()
    graph = {h: es2}
    root = zobrist_int64(chess.Board())
    if not white:
        graph[root] = [{"move_san": "e4", "child_hash": h, "total": 100, "white_score_avg": .5,
                        "parent_epd": chess.Board().epd()}]
    evn = {kid(main): ws(80, white), kid(second): ws(0, white)}
    gkw = dict(rep_moves={}, strict_source=True, gated_reach=True, max_eval_loss=30,
               prior_strength=1)
    g = bc.build_graph(graph, root, white, eval_ws=dict(evn), rare_floor=20, verified={}, **gkw)
    check("no reference, through the graph: both continuations stay and the node is listed",
          sorted(c[0] for c in g.our[h].cands) == sorted([main, second])
          and g.meta["rare_disputes"][h].get("unresolved"))
    want = rv.plan(pl.DataFrame({"position_hash": [h], "position_epd": [start.epd()],
                                 "side_to_move": [side], "best_move": [main]}),
                   meta, empty, side, {k: round(bc.ws_to_cp(v, True)) for k, v in evn.items()},
                   {h: es2}, [{"position_hash": h, "reach": 1., **g.meta["rare_disputes"][h]}])
    check("no reference: both moves are sent for a search", want[h][2] == sorted([main, second]))
    with tempfile.TemporaryDirectory() as td:
        # fresh reversal: the one-game move is -50, the ten-game move 0
        recs = [dict(position_hash=h, position_epd=start.epd(), move_san=m, child_hash=kid(m),
                     child_cp_white=sign * cp, nodes=5, depth=20, engine="t", searched="d",
                     reason="disputed gate", cloud_origin=False, parent_cp_white=0,
                     verified_loss_cp=-cp, verification_nodes=5, verification_best=second,
                     verification_free_cp_white=0, verification_unstable=False,
                     verification_engine_sha256="x", transposition_cp_white=None)
                for m, cp in ((main, -50), (second, 0))]
        path = Path(td) / "o.parquet"
        pl.DataFrame(recs, schema=fb.SCHEMA).write_parquet(path)
        ev2 = dict(evn)
        moves, _ = bc.load_engine_candidates(path, ev2)
        g = bc.build_graph(graph, root, white, eval_ws=ev2, engine_moves=moves,
                           excluded=bc.verified_exclusions(path, 30), rare_floor=20,
                           verified=bc.verified_moves(pl.read_parquet(path)), **gkw)
        g.our[h].l_node = 0.1
        curves, _ = bc.build_curves(g, bmax=1)
        check("no reference, after a fresh reversal: the second move's line is the one booked",
              [c[0] for c in g.our[h].cands] == [second]
              and bc.extract_book(g, curves, 1)["booked"].get(h) == second)

    # 11. A verdict the budget cannot settle is searched again, deeper, in its own cache.
    def saved_for(free_cp, table):
        return {"epd": start.epd(), "free": {"san": main, "cp": sign * free_cp, "depth": 9,
                                             "nodes": 5},
                "moves": {m: {"san": m, "cp": sign * c, "depth": 9, "nodes": 5}
                          for m, c in table.items()}, "engine": "t", "date": "d"}
    close = saved_for(221, {main: 218, second: 190})      # losses 31 (free) and 28 (restricted)
    check("the two references disagree at 30 cp: a retry is due", rv.boundary(close, 30) == [second])
    check("both references agree and far from the limit: no retry",
          rv.boundary(saved_for(221, {main: 218, second: 100}), 30) == []
          and rv.boundary(saved_for(221, {main: 218, second: 215}), 30) == [])
    menu2 = {m: {"move_san": m, "child_hash": kid(m)} for m in (main, second)}
    base_rows = {r["move_san"]: r for r in rv.rows_for(h, close, menu2, 30, 5, "sha", "r")}
    check("the reference is the best restricted score, not the unrestricted one",
          base_rows[second]["verified_loss_cp"] == 28
          and base_rows[second]["parent_cp_white"] == sign * 218)
    with tempfile.TemporaryDirectory() as td:
        calls = []
        binary = Path(td) / "sf"
        binary.write_text("x")
        deep_scores = {main: 220, second: 190}
        with patch.object(rv.ec, "SF", str(binary)), \
                patch.object(rv.Searches, "_one", fake_engine(deep_scores, calls)):
            deeper = rv.Searches(Path(td) / "cache" / "deep-20", 20, 1, free=False)
            deep = deeper.get(h, start.epd(), sorted(close["moves"]))
            deeper.get(h, start.epd(), sorted(close["moves"]))
        check("the retry is restricted searches only, cached apart, and reused",
              calls == sorted(close["moves"]) and deep["free"] is None
              and (Path(td) / "cache" / "deep-20" / f"{h}.json").exists()
              and not (Path(td) / "cache" / f"{h}.json").exists(), str(calls))
        rows30 = {r["move_san"]: r for r in rv.rows_for(h, close, menu2, 30, 5, "sha", "r",
                                                        deep, 20)}
        check("the deeper scores decide: a loss of exactly 30 passes",
              rows30[second]["verified_loss_cp"] == 30 and rows30[second]["verification_nodes"] == 20
              and not rows30[second]["verification_unstable"]
              and rows30[second]["parent_cp_white"] == sign * 220)
        deep["moves"][second]["cp"] = sign * 189
        rows31 = {r["move_san"]: r for r in rv.rows_for(h, close, menu2, 30, 5, "sha", "r",
                                                        deep, 20)}
        check("at 31 it fails, and the row says the two budgets disagreed",
              rows31[second]["verified_loss_cp"] == 31 and rows31[second]["verification_unstable"])

# 12. Round receipts: a stage is reused only when it is provably this stage.
with tempfile.TemporaryDirectory() as td:
    td = Path(td)
    outs = [td / "book.parquet", td / "book.parquet.meta.json"]
    ran = []

    def work():
        ran.append(1)
        for p_ in outs:
            p_.write_text("made " + p_.name)

    ident = {"code": "c1", "nodes": 5, "overlay_in": "o1"}
    rc = td / "build.receipt.json"
    _, reused = rrb.stage(rc, ident, outs, work)
    _, again = rrb.stage(rc, ident, outs, work)
    check("receipt: an identical rerun reuses the stage", not reused and again and len(ran) == 1)

    def refused(identity):
        try:
            rrb.stage(rc, identity, outs, work)
        except rrb.StaleRound as e:
            return str(e)
        return None

    check("receipt: changed verification code is reported", "code" in (refused(dict(ident, code="c2")) or ""))
    check("receipt: a changed search budget is reported", "nodes" in (refused(dict(ident, nodes=9)) or ""))
    check("receipt: a swapped overlay is reported",
          "overlay_in" in (refused(dict(ident, overlay_in="o2")) or ""))
    outs[1].write_text("made")
    check("receipt: a truncated sidecar is reported", "meta.json" in (refused(ident) or ""))
    outs[1].unlink()
    check("receipt: a removed sidecar is reported", "meta.json" in (refused(ident) or ""))
    check("receipt: nothing was recomputed or overwritten on a mismatch", len(ran) == 1)
    rc.unlink()
    outs[0].write_text("half")
    _, reused = rrb.stage(rc, ident, outs, work)
    check("receipt: an interrupted stage (no receipt) is recomputed from scratch",
          not reused and len(ran) == 2 and outs[0].read_text() == "made book.parquet")
    f = td / "a.py"
    f.write_text("x = 1\n")
    one = rrb.code_sha([f])
    f.write_text("x = 2\n")
    check("receipt: the code identity follows the file contents", rrb.code_sha([f]) != one)
    # an older receipt's command: retired options carrying today's behaviour are dropped
    cmd = ["--rep", "r", "--max-cands", "0", "--stop-model", "weak", "--method", "price",
           "--price-seed", "source", "--price-reprice", "max", "--price-rounds", "1",
           "--price-select", "best", "--price-dag-mode", "polish", "--budget", "1"]
    try:
        rrb.modern(["--rep", "r", "--price-rounds", "4"])
        refused = False
    except ValueError:
        refused = True
    check("driver: retired options with today's values are dropped, any other value refused",
          rrb.modern(cmd) == ["--rep", "r", "--method", "price", "--budget", "1"] and refused)
    (f.parent / "book_builder_rs.pyd").write_bytes(b"x")
    check("driver: the Rust extension is part of the code a snapshot copies and hashes",
          any(q.name == "book_builder_rs.pyd" for q in rrb.code_files(f.parent)))

# 13. The graph reader hands the lookup the opponent overlay the book was built with.
with tempfile.TemporaryDirectory() as td:
    td = Path(td)
    for n in ("pool.parquet", "aux.parquet", "prior.parquet", "other.parquet"):
        (td / n).write_text(n)
        (td / (n + ".meta.json")).write_text("{}")

    def lookup(name, overlay):
        d_ = td / name
        d_.mkdir()
        (d_ / "recipe.json").write_text("{}")
        (d_ / "ready.json").write_text(json.dumps({
            "recipe_sha256": uom.digest(d_ / "recipe.json"), "files": {},
            "contract": uom.contract(td / "pool.parquet", td / "aux.parquet", "black", overlay)}))
        return {"stats": str(td / "pool.parquet"), "aux_stats": str(td / "aux.parquet"),
                "min_games": 20, "uncut_own_moves": str(d_),
                "uncut_own_recipe": uom.digest(d_ / "recipe.json")}

    def reads(meta_):
        try:
            return book_graph.edge_source(meta_, "black")[0].endswith("graph.parquet")
        except ValueError:
            return False

    prior = {"file": str(td / "prior.parquet"), "sha256": uom.digest(td / "prior.parquet")}
    with_prior = lookup("with", td / "prior.parquet")
    plain = lookup("plain", None)
    check("overlay identity: a historical-only uncut book reads its lookup", reads(plain))
    check("overlay identity: a book built with an opponent overlay reads with that overlay",
          reads(dict(with_prior, opponent_prior=prior)))
    check("overlay identity: the same lookup without the overlay is refused", not reads(with_prior))
    check("overlay identity: another overlay is refused",
          not reads(dict(with_prior, opponent_prior={
              "file": str(td / "other.parquet"), "sha256": uom.digest(td / "other.parquet")})))
    (td / "prior.parquet").write_text("swapped")
    check("overlay identity: an overlay changed on disk is refused",
          not reads(dict(with_prior, opponent_prior=prior)))


# ── Follow-up review, 2026-11-10 ─────────────────────────────────────────────────────────────
import argparse                                              # noqa: E402
import build_budget_books as bbb                             # noqa: E402

# 14. The limit is inclusive everywhere: 29 and 30 cp stay, 31 goes, for both colours, through the
# verifier's row, the gate, gated reach, the validator and the graph.
for white in (True, False):
    side = "white" if white else "black"
    sign = 1 if white else -1
    start = chess.Board()
    if not white:
        start.push_san("e4")
    h = zobrist_int64(start)
    main, second = ("e4", "d4") if white else ("e5", "d5")

    def kid(san):
        b = start.copy()
        b.push_san(san)
        return zobrist_int64(b)
    es = [{"move_san": m, "child_hash": kid(m), "total": 100, "white_score_avg": .5,
           "parent_epd": start.epd()} for m in (main, second)]
    menu2 = {m: e for m, e in zip((main, second), es)}
    root = zobrist_int64(chess.Board())
    graph = {h: es}
    if not white:
        graph[root] = [{"move_san": "e4", "child_hash": h, "total": 100, "white_score_avg": .5,
                        "parent_epd": chess.Board().epd()}]
    meta = {"slice_prior": 0.5, "max_eval_loss": 30, "rare_floor": 20, "gates": True}
    for d in (29, 30, 31):
        saved = {"epd": start.epd(), "free": {"san": main, "cp": sign * 221, "depth": 9, "nodes": 5},
                 "moves": {m: {"san": m, "cp": sign * c, "depth": 9, "nodes": 5}
                           for m, c in ((main, 220), (second, 220 - d))}, "engine": "t", "date": "d"}
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "o.parquet"
            rows = list(rv.rows_for(h, saved, menu2, 30, 5, "sha", "r"))
            pl.DataFrame(rows, schema=fb.SCHEMA).write_parquet(path)
            ev = {}
            moves, _ = bc.load_engine_candidates(path, ev)
            ver = bc.verified_moves(pl.read_parquet(path))
            veto = bc.verified_exclusions(path, 30)
            want = d <= 30
            row = next(r for r in rows if r["move_san"] == second)
            gate = [e["move_san"] for e, _ in bc.gate_candidates(
                h, es, None, ev, white, 0.5, max_eval_loss=30, rare_floor=20, verified=ver)]
            admit = object.__new__(bbb.GateAdmit)
            admit.rep_moves, admit.excluded, admit.eval_ws = {}, veto, ev
            admit.our_white, admit.slice_prior, admit.gates = white, 0.5, True
            admit.max_eval_loss, admit.gate_settings = 30, {}
            admit.rare_floor, admit.verified = 20, ver
            g = bc.build_graph(graph, root, white, eval_ws=dict(ev), engine_moves=moves,
                               excluded=veto, rare_floor=20, verified=ver, rep_moves={},
                               strict_source=True, gated_reach=True, max_eval_loss=30,
                               prior_strength=1)
            status = classify(h, second, es, None, ev, white, meta, None, start.epd(), veto, 20, ver)
            ok = (row["verified_loss_cp"] == d and ((second in veto.get(h, ())) != want)
                  and (second in gate) == want and (kid(second) in admit(h, es)) == want
                  and (second in [c[0] for c in g.our[h].cands]) == want
                  and status == ("in_set" if want else "outside_set")
                  and not g.meta.get("loss_exceptions"))
            check(f"{side}: a verified loss of {d} cp {'stays' if want else 'goes'} at every step",
                  ok, f"gate {gate} veto {veto} status {status}")
    # the dispute's own boundaries: a rare move level with the reference is no threat, and a rival
    # exactly 30 below the threat is not at risk
    for rare_cp, rival_cp, threat, risk in ((60, 40, False, False), (61, 40, True, False),
                                            (90, 60, True, False), (90, 59, True, True)):
        ev = {}
        es3 = menu(77, ev, white, [("Nc6", 100, 60), ("Be7", 50, rival_cp), ("Qc5", 2, rare_cp)],
                   own=60)
        dis = {}
        kept(77, es3, ev, white, 30, rare_floor=20, disputes=dis)
        d77 = dis.get(77, {})
        check(f"{side}: dispute boundary, rare {rare_cp} / rival {rival_cp}",
              bool(d77.get("threats")) == threat and ("Be7" in d77.get("at_risk", ())) == risk,
              str(d77))

# 15. The driver: any new evidence is followed by a rebuild, the final book names the overlay it
# consumed, and every stage runs from the snapshot.
with tempfile.TemporaryDirectory() as td:
    td = Path(td)
    src = td / "src"
    src.mkdir()
    (src / "rare_verify.py").write_text("# v1\n")
    (src / "build_budget_books.py").write_text(
        "import sys, json, pathlib\n"
        "import polars as pl\n"
        "a = sys.argv\n"
        "b = pathlib.Path(a[a.index('--out-prefix') + 1] + '_b1.parquet')\n"
        "pl.DataFrame({'position_hash': [1], 'side_to_move': ['black'], 'best_move': ['e5']})"
        ".write_parquet(b)\n"
        "pathlib.Path(str(b) + '.meta.json').write_text(json.dumps({'ran': __file__}))\n"
        "pathlib.Path(str(b) + '.rare.json').write_text('[]')\n")
    (td / "baseline" / "stages").mkdir(parents=True)
    (td / "baseline" / "stages" / "black-cp30.json").write_text(json.dumps({"command": [
        "py", "x.py", "--rep", "r", "--budget", "1", "--max-eval-loss", "30", "--out-prefix", "x",
        "--tmp-dir", "t"]}))
    (td / "lookup").mkdir()
    (td / "lookup" / "ready.json").write_text(json.dumps({"recipe_sha256": "L"}))

    def ns(out):
        return argparse.Namespace(
            colour="black", cap=30, baseline=td / "baseline", template_cap=None,
            lookup=td / "lookup", out=td / out, searches=td / "s", nodes=5, deep_nodes=20, band=10,
            threads=1, frontier_reach=.002, frontier=5, max_rounds=4, wait=False)
    log = []

    def build(args, _log, names_overlay=True):
        book = Path(args[args.index("--out-prefix") + 1] + "_b1.parquet")
        ov = args[args.index("--engine-candidates") + 1] if "--engine-candidates" in args else None
        # the corrected off-book value brings another line into the book
        pl.DataFrame({"position_hash": [2 if ov else 1], "side_to_move": ["black"],
                      "best_move": ["Nf6" if ov else "e5"]}).write_parquet(book)
        Path(str(book) + ".meta.json").write_text(json.dumps(
            {"engine_candidates_sha256": rrb.sha(ov) if ov and names_overlay else None}))
        Path(str(book) + ".rare.json").write_text("[]")
        log.append("build+overlay" if ov else "build")

    def verify(book, out, front, _log):
        # round 0 finds work on the unbooked frontier only; later rounds find nothing
        pl.DataFrame({"x": [1]}).write_parquet(out)
        Path(str(out) + ".meta.json").write_text(json.dumps(
            {"requested_parents": 1 if front else 0,
             "by_reason": {"frontier dispute": 1} if front else {}}))
        log.append("verify")

    final = rrb.drive(ns("a"), build, verify, source=src)
    booked = pl.read_parquet(final["book"])["best_move"].to_list()
    check("driver: a frontier-only round is followed by a rebuild that consumes it",
          log == ["build", "verify", "build+overlay", "verify"] and final["round"] == 1
          and booked == ["Nf6"] and final["overlay"].endswith("overlay.parquet")
          and final["applied_overlay_sha256"] == rrb.sha(final["overlay"]), f"{log} {final}")
    again = rrb.drive(ns("a"), build, verify, source=src)
    check("driver: an identical resume reuses every stage", len(log) == 4 and again == final)
    try:
        rrb.drive(ns("b"), lambda a_, l_: build(a_, l_, names_overlay=False), verify, source=src)
        closed = True
    except rrb.StaleRound:
        closed = False
    check("driver: a final book that does not name the last overlay is refused", not closed)

    # --seed-overlay: round 0 is already built with another run's evidence, and names it
    seeded = ns("f")
    seeded.seed_overlay = Path(final["overlay"])
    n0 = len(log)
    fs = rrb.drive(seeded, build, verify, source=src)
    r0 = json.loads((td / "f" / "round-00" / "build.receipt.json").read_text())["identity"]
    check("driver: a seeded run builds round 0 with the seed and records it",
          log[n0:] == ["build+overlay", "verify", "build+overlay", "verify"]
          and r0["overlay_in"] == rrb.sha(final["overlay"])
          and str(final["overlay"]) in r0["command"] and fs["seed_overlay"] == str(final["overlay"])
          and final.get("seed_overlay") is None, f"{log[n0:]} {fs}")

    # the default stages run the snapshot's files, not the working tree's
    final = rrb.drive(ns("c"), None, lambda b_, o_, f_, l_: (
        pl.DataFrame({"x": [1]}).write_parquet(o_),
        Path(str(o_) + ".meta.json").write_text(json.dumps({"requested_parents": 0, "by_reason": {}}))),
        source=src)
    ran = json.loads(Path(final["book"] + ".meta.json").read_text())["ran"]
    snap = Path(final["code_snapshot"])
    check("snapshot: the build ran from the snapshot directory",
          Path(ran).parent == snap and snap.parent == td / "c", ran)
    # a live edit after round 0 cannot run under the old identity
    n = len(log)

    def editing_build(args, _log):
        build(args, _log)
        (src / "rare_verify.py").write_text("# v2, edited while the run is live\n")
    final = rrb.drive(ns("d"), editing_build, verify, source=src)
    snap = Path(final["code_snapshot"])
    receipts = [json.loads(Path(r).read_text()) for r in final["receipts"]]
    check("snapshot: a mid-run edit of the tree leaves the run on the bytes it started with",
          (snap / "rare_verify.py").read_text() == "# v1\n"
          and {r["identity"]["code"] for r in receipts} == {final["code_sha256"]}
          and rrb.code_sha(rrb.code_files(snap)) == final["code_sha256"])
    try:
        rrb.drive(ns("d"), build, verify, source=src)
        msg = ""
    except rrb.StaleRound as e:
        msg = str(e)
    check("snapshot: resuming with the edited tree is reported, not silently mixed", "code" in msg, msg)

    def tampering_build(args, _log):
        build(args, _log)
        next((td / "e").glob("code-*")).joinpath("rare_verify.py").write_text("# tampered\n")
    try:
        rrb.drive(ns("e"), tampering_build, verify, source=src)
        msg = ""
    except rrb.StaleRound as e:
        msg = str(e)
    check("snapshot: a stage whose code changed underneath it gets no receipt",
          "snapshot" in msg and not (td / "e" / "round-00" / "build.receipt.json").exists(), msg)


# 16. Depth is never lost at a parent (through rare_verify.run, reading and writing overlays).
# After 1.e4, from Black's side: ...e5 100 games +100; ...d5 one game, +70 at 5M but +115 at 20M;
# ...c5 one game +117. Round 0 books ...d5 (loss 30 at 5M -> settled at 20M: best). Round 1 books
# ...c5: at 5M ...d5 would read 47 cp down and be vetoed; at the parent's 20M tier it is 2 down.
import numpy as np                                           # noqa: E402

with tempfile.TemporaryDirectory() as td:
    td = Path(td)
    start = chess.Board()
    start.push_san("e4")
    h = zobrist_int64(start)

    def kid(san):
        b = start.copy()
        b.push_san(san)
        return zobrist_int64(b)
    games = {"e5": 100, "d5": 1, "c5": 1}
    stored_black = {"e5": 100, "d5": 70, "c5": 117}
    by_budget = {5: {None: ("e5", 116), "e5": 100, "d5": 70, "c5": 117},
                 20: {"e5": 100, "d5": 115, "c5": 117}}
    edges = {h: [{"move_san": m, "child_hash": kid(m), "total": n, "white_score_avg": .5}
                 for m, n in games.items()]}
    stored_white = {kid(m): -c for m, c in stored_black.items()} | {h: -100}
    stats = td / "pool.parquet"
    pl.DataFrame({"parent_hash": [h], "parent_epd": [start.epd()]}).write_parquet(stats)
    searched = []

    def engine(self, board, move=None):
        searched.append((self.nodes, move))
        self.name = "fake"
        table = by_budget[self.nodes]
        san = move if move is not None else table[None][0]
        cp = table[None][1] if move is None else table[rv.bare(move)]
        return {"san": san, "cp": -cp, "depth": 9, "nodes": self.nodes}

    def book_for(name, move, overlay):
        book = td / f"{name}.parquet"
        pl.DataFrame({"position_hash": [h], "position_epd": [start.epd()],
                      "side_to_move": ["black"], "best_move": [move]}).write_parquet(book)
        Path(str(book) + ".meta.json").write_text(json.dumps({
            "rare_floor": 20, "max_eval_loss": 30, "min_games": 20, "stats": str(stats),
            "engine_candidates": str(overlay) if overlay else None,
            "engine_candidates_sha256": rrb.sha(overlay) if overlay else None}))
        Path(str(book) + ".rare.json").write_text("[]")
        return book

    def verify(book, out, cache):
        n0 = len(searched)
        rv.run(book, "black", td / cache, td / out, nodes=5, threads=1, deep_nodes=20)
        rows = {r["move_san"]: (r["verified_loss_cp"], r["verification_nodes"])
                for r in pl.read_parquet(td / out).iter_rows(named=True)}
        return rows, searched[n0:], json.loads((td / (out + ".meta.json")).read_text())

    binary = td / "sf"
    binary.write_text("x")
    with patch.object(rv.ec, "SF", str(binary)), patch.object(rv.Searches, "_one", engine), \
            patch.object(rv, "edges_at", lambda con, meta, colour, parents: edges), \
            patch.object(rv, "open_eval_arrays", lambda: ()), \
            patch.object(rv, "lookup_evals", lambda keys: np.array(
                [stored_white.get(int(k), rv.MISSING) for k in keys])):
        r0, _, m0 = verify(book_for("b0", "d5", None), "o0.parquet", "cache")
        check("depth: round 0 settles the 30 cp verdict at the deeper budget",
              r0 == {"d5": (0, 20), "e5": (15, 20)}
              and m0["retried_parents"][0].get("why") == "boundary", str(r0))
        b1 = book_for("b1", "c5", td / "o0.parquet")
        r1, s1, m1 = verify(b1, "o1.parquet", "cache")
        check("depth: a later rival is compared at the parent's deeper budget, not a shallower one",
              r1 == {"c5": (0, 20), "d5": (2, 20), "e5": (17, 20)}, str(r1))
        tiers = m1["retried_parents"]
        check("depth: only the new rival needs a deep search; the receipt says it was carried",
              [x for x in s1 if x[0] == 20] == [(20, "c5")] and len(tiers) == 1
              and tiers[0].get("why") == "carried forward" and tiers[0].get("deep_cache_hits") == 2
              and tiers[0].get("compared") == ["c5", "d5", "e5"], f"{s1} {m1}")
        ev = {}
        moves, _ = bc.load_engine_candidates(td / "o1.parquet", ev)
        ver = bc.verified_moves(pl.read_parquet(td / "o1.parquet"))
        veto = bc.verified_exclusions(td / "o1.parquet", 30)
        gate = sorted(e["move_san"] for e, _ in bc.gate_candidates(
            h, [e for e in edges[h] if e["move_san"] not in veto.get(h, ())], None, ev, False, 0.5,
            max_eval_loss=30, rare_floor=20, verified=ver))
        meta_ = {"slice_prior": 0.5, "max_eval_loss": 30, "rare_floor": 20, "gates": True}
        check("depth: the move admitted at 20M stays eligible for the builder and the validator",
              gate == ["c5", "d5", "e5"] and not veto
              and classify(h, "d5", edges[h], None, ev, False, meta_, None, start.epd(), veto, 20,
                           ver) == "in_set", f"{gate} {veto}")
        r1b, s1b, _ = verify(b1, "o1b.parquet", "cache")
        check("depth: an identical repeat gives the same rows with no new search",
              r1b == r1 and s1b == [], str(s1b))
        # control: the deep cache is warm but no deep verdict was APPLIED -> the base tier decides
        cold, _, _ = verify(book_for("b2", "c5", None), "o2.parquet", "empty")
        warm, sw, mw = verify(book_for("b3", "c5", None), "o3.parquet", "cache")
        check("depth: a warm deep cache alone does not change a base-only verdict",
              warm == cold and not [x for x in sw if x[0] == 20] and not mw["retried_parents"]
              and set(n for _, n in warm.values()) == {5}, f"{cold} {warm}")
        # an applied verdict deeper than this run can match is refused
        d0 = pl.read_parquet(td / "o0.parquet")
        d0.with_columns(pl.lit(80).cast(d0.schema["verification_nodes"])
                        .alias("verification_nodes")).write_parquet(td / "o80.parquet")
        try:
            verify(book_for("b4", "c5", td / "o80.parquet"), "o4.parquet", "cache")
            msg = ""
        except ValueError as e:
            msg = str(e)
        check("depth: an applied verdict deeper than the run supports is an explicit error",
              "deepest budget" in msg, msg)

print("ALL PASS" if not FAILS else f"{len(FAILS)} FAILED: {FAILS}")
sys.exit(1 if FAILS else 0)
