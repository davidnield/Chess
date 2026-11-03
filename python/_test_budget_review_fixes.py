"""The 2026-10-31 review's construction fixes, through build_graph and the engines.

  S  --strict-source (build_graph strict_source): a source-book move the gates reject
     stays rejected. Without it, build_graph restored it as an "engine-augmented"
     candidate, so --max-eval-loss never bound the source move and the DP and the price
     search both booked a -120 cp trap whose empirical score was high. An UNRECORDED
     source move is restored only if it passes --max-eval-loss (and has an eval).
  C  --crack-at-parents (build_graph crack_at_parents): 100 continuing games at 0.5 and
     100 opponent resignations at an our-turn arrival are worth 0.75 to the parent
     whether we stop or book (Stage 3's rule, aux_our_blend on the chosen value). The
     legacy graph credits the resignations to stopping only, so booking reads 0.5, and a
     move that beats stopping is never booked. Legacy and compiled engines agree.
  F  --min-games defaults to the pool's own floor (build_budget_books._pool_min_games).
  W  the book frame holds a loss label past polars' 100-row inference, and changes nothing else.
  O  both flags off are exact no-ops on the graph.

Run: .venv/Scripts/python.exe python/_test_budget_review_fixes.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import chess
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import budget_core as bc  # noqa: E402
from stage3_backwards_induction import aux_our_blend  # noqa: E402
from stage3_backwards_induction import cp_to_expected_score as ws  # noqa: E402
from zobrist import zobrist_int64  # noqa: E402

FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


PURE = dict(prior_strength=0.0, eval_weight=0.0, eps=1e-6, share_floor=0.0)


def h_after(*sans: str) -> int:
    b = chess.Board()
    for s in sans:
        b.push_san(s)
    return zobrist_int64(b)


# ── S: the source move and the soundness limit ────────────────────────────────
START = chess.Board()
ROOT = zobrist_int64(START)
EPD = START.epd()


def root_edges(with_src: bool) -> dict:
    """Root (White to move) with e4 (0.7 after) and d4 (0.6), plus the trap g4: thin,
    empirically 0.95, objectively -120 cp. Each child has one recorded reply, so it is
    an opponent node in the subgraph and its value is its reply's score."""
    def mv(san, total, score):
        return {"move_san": san, "child_hash": h_after(san), "total": total,
                "white_score_avg": score, "parent_epd": EPD}

    def reply(san, score):
        return [{"move_san": "a6" if san != "g4" else "a5", "child_hash": h_after(san, "a6"),
                 "total": 500, "white_score_avg": score}]
    es = [mv("e4", 1000, 0.7), mv("d4", 1000, 0.6)]
    out = {h_after("e4"): reply("e4", 0.7), h_after("d4"): reply("d4", 0.6)}
    if with_src:
        es.append(mv("g4", 100, 0.95))
        out[h_after("g4")] = reply("g4", 0.95)
    return {ROOT: es, **out}


EV = {ROOT: ws(30), h_after("e4"): ws(40), h_after("d4"): ws(35), h_after("g4"): ws(-120)}


def cands(g) -> list[str]:
    return sorted(c[0] for c in g.our[ROOT].cands)


def book_dp(g) -> dict:
    curves, _ = bc.build_curves(g, bmax=4)
    return bc.extract_book(g, curves, 1)["booked"]


def book_price(g) -> dict:
    res, _ = bc.price_search(g, 1, log=lambda m: None)
    return res["booked"]


print("S  source move vs --max-eval-loss")
SPURE = {**PURE, "prior_strength": 1e-9}   # >0: the restored move has no games
kw = dict(rep_moves={ROOT: "g4"}, eval_ws=EV, gates=False, max_eval_loss=60, **SPURE)
legacy = bc.build_graph(root_edges(True), ROOT, True, **kw)
strict = bc.build_graph(root_edges(True), ROOT, True, strict_source=True, **kw)
check("legacy restores the rejected source move", "g4" in cands(legacy), str(cands(legacy)))
check("...and both engines book it", book_dp(legacy).get(ROOT) == "g4"
      and book_price(legacy).get(ROOT) == "g4",
      f"dp {book_dp(legacy).get(ROOT)}, price {book_price(legacy).get(ROOT)}")
check("strict keeps it out", cands(strict) == ["d4", "e4"], str(cands(strict)))
check("...counts it", strict.meta.get("src_rejected") == 1, str(strict.meta.get("src_rejected")))
check("...and both engines book a sound move", book_dp(strict).get(ROOT) in ("e4", "d4")
      and book_price(strict).get(ROOT) in ("e4", "d4"),
      f"dp {book_dp(strict).get(ROOT)}, price {book_price(strict).get(ROOT)}")

nf3 = h_after("Nf3")
for ev, want, label in ((ws(30), True, "+30 (within 60 of +40)"),
                        (ws(-100), False, "-100"), (None, False, "no eval")):
    e = dict(EV)
    if ev is not None:
        e[nf3] = ev
    kw2 = dict(rep_moves={ROOT: "Nf3"}, eval_ws=e, gates=False, max_eval_loss=60, **SPURE)
    gs = bc.build_graph(root_edges(False), ROOT, True, strict_source=True, **kw2)
    gl = bc.build_graph(root_edges(False), ROOT, True, **kw2)
    check(f"unrecorded source move at {label}: strict {'restores' if want else 'refuses'} it",
          ("Nf3" in cands(gs)) == want, str(cands(gs)))
    check(f"unrecorded source move at {label}: legacy restores it", "Nf3" in cands(gl))

print("S2 an unrecorded source move better than every recorded one (follow-up review R1)")


def aug_graph(white: bool):
    """Our node with one recorded move (0 cp, empirically 0.95) and an unrecorded
    source move at +300 cp; the position's own eval is 0 and the limit 30. The loss
    rule over the complete set keeps only the source move."""
    pre = () if white else ("e4",)
    rec, aug = ("e4", "d4") if white else ("e5", "d5")
    b = chess.Board()
    for s in pre:
        b.push_san(s)
    h = zobrist_int64(b)
    sgn = 1 if white else -1
    hr, ha = h_after(*pre, rec), h_after(*pre, aug)
    emp = 0.95 if white else 0.05
    edges = {h: [{"move_san": rec, "child_hash": hr, "total": 100,
                  "white_score_avg": emp, "parent_epd": b.epd()}]}
    if not white:       # the root (White to move) leads to our node
        edges = {ROOT: [{"move_san": "e4", "child_hash": h, "total": 1000,
                         "white_score_avg": 0.5, "parent_epd": EPD}], **edges}
    ev = {h: ws(0), hr: ws(0), ha: ws(sgn * 300)}
    g = bc.build_graph(edges, ROOT, white, rep_moves={h: aug}, eval_ws=ev, gates=False,
                       max_eval_loss=30, strict_source=True, prior_strength=1,
                       eval_weight=0)
    g.our[h].l_node = 0.2          # a weak stop model can be worse than booking
    return g, h, aug


for white in (True, False):
    g, h, aug = aug_graph(white)
    c = "White" if white else "Black"
    check(f"{c}: only the qualifying source move is a candidate",
          [x[0] for x in g.our[h].cands] == [aug], str([x[0] for x in g.our[h].cands]))
    check(f"{c}: both engines book it", book_dp(g).get(h) == aug
          and book_price(g).get(h) == aug,
          f"dp {book_dp(g).get(h)}, price {book_price(g).get(h)}")
    check(f"{c}: no loss exception", g.meta["loss_exceptions"] == {},
          str(g.meta["loss_exceptions"]))

print("S3 loss exceptions are recorded")
es3 = {ROOT: [{"move_san": "e4", "child_hash": h_after("e4"), "total": 100,
               "white_score_avg": 0.6, "parent_epd": EPD},
              {"move_san": "d4", "child_hash": h_after("d4"), "total": 100,
               "white_score_avg": 0.6, "parent_epd": EPD}]}
g = bc.build_graph(es3, ROOT, True, eval_ws={ROOT: ws(200), h_after("e4"): ws(0),
                                             h_after("d4"): ws(-50)},
                   gates=False, max_eval_loss=30, strict_source=True, **SPURE)
check("all fail: the least-losing move stays, flagged 'fallback'",
      cands(g) == ["e4"] and g.meta["loss_exceptions"] == {ROOT: "fallback"},
      f"{cands(g)} {g.meta['loss_exceptions']}")
g = bc.build_graph(es3, ROOT, True, eval_ws={}, gates=False, max_eval_loss=30,
                   strict_source=True, **SPURE)
check("no evals: every move stays, flagged 'unknown'",
      cands(g) == ["d4", "e4"] and g.meta["loss_exceptions"] == {ROOT: "unknown"},
      f"{cands(g)} {g.meta['loss_exceptions']}")
g = bc.build_graph(es3, ROOT, True, eval_ws={}, gates=False, **SPURE)
check("no limit: nothing recorded", "loss_exceptions" not in g.meta)

print("S4 engine-proposed candidates (--engine-candidates)")
nf3 = h_after("Nf3")
ev4 = {ROOT: ws(200), h_after("e4"): ws(0), h_after("d4"): ws(-50), nf3: ws(190)}
g = bc.build_graph(es3, ROOT, True, eval_ws=ev4, gates=False, max_eval_loss=30,
                   strict_source=True, engine_moves={ROOT: [("Nf3", nf3)]}, **SPURE)
check("an engine move within the limit replaces the all-fail fallback",
      cands(g) == ["Nf3"] and g.meta["loss_exceptions"] == {} and g.meta["engine_cands"] == 1,
      f"{cands(g)} {g.meta['loss_exceptions']}")
g.our[ROOT].l_node = 0.2
check("...and both engines book it", book_dp(g).get(ROOT) == "Nf3" and book_price(g).get(ROOT) == "Nf3")
g = bc.build_graph(es3, ROOT, True, eval_ws={**ev4, nf3: ws(-100)}, gates=False, max_eval_loss=30,
                   strict_source=True, engine_moves={ROOT: [("Nf3", nf3)]}, **SPURE)
check("a worse engine move is judged like any other (dropped)", cands(g) == ["e4"], str(cands(g)))
a_ = bc.build_graph(es3, ROOT, True, eval_ws=ev4, gates=False, max_eval_loss=30, strict_source=True,
                    **SPURE)
b_ = bc.build_graph(es3, ROOT, True, eval_ws=ev4, gates=False, max_eval_loss=30, strict_source=True,
                    engine_moves=None, **SPURE)
check("engine_moves=None is a no-op", cands(a_) == cands(b_) == ["e4"])

# ── C: opponent collapses at an our-turn arrival ──────────────────────────────
# A Black book. Root R (White to move) -> x -> A, y -> B; A and B are ours.
R, A, B, MA, MB = 1, 2, 3, 4, 5


def crack_graph(child_ws: float, flag: bool):
    edges = {
        R: [{"move_san": "x", "child_hash": A, "total": 1000, "white_score_avg": 0.5},
            {"move_san": "y", "child_hash": B, "total": 1000, "white_score_avg": 0.5}],
        # A: 100 games continue (stop value 0.5); the booked move's child scores
        # 1 - child_ws for Black
        A: [{"move_san": "m", "child_hash": MA, "total": 100, "white_score_avg": 0.5}],
        B: [{"move_san": "n", "child_hash": MB, "total": 100, "white_score_avg": 0.5}],
    }
    # leaf candidate value comes from the edge; give A's move its own score
    edges[A][0]["white_score_avg"] = child_ws
    aux = {A: {"term_normal_total": 100, "term_normal_black_wins": 100}}
    # stop value of A: leaf_node_value over A's edges -> make it 0.5 regardless
    g = bc.build_graph(edges, R, False, aux_rows=aux, gates=False, crack_at_parents=flag,
                       **PURE)
    g.our[A].l_node = 0.5 if flag else aux_our_blend(0.5, 100, 100, 1.0)
    return g


print("C  opponent collapses: stop vs book")
for flag in (False, True):
    g = crack_graph(0.5, flag)              # booking A's move is worth exactly stopping
    order = bc.topo_up(g)[0]
    stop = bc.policy_value(g, {}, order)["value"]
    book = bc.policy_value(g, {A: "m"}, order)["value"]
    want = 0.5 * aux_our_blend(0.5, 100, 100, 1.0) + 0.5 * 0.5     # Stage 3: 0.625
    if flag:
        check("flag: stop and an equal booked move both give Stage 3's 0.625",
              abs(stop - want) < 1e-12 and abs(book - want) < 1e-12, f"{stop:.6f} / {book:.6f}")
        cg = __import__("budget_compiled").compile_graph(g, order)
        cb = __import__("budget_compiled").policy_value(cg, {A: "m"})["value"]
        check("flag: the compiled engine agrees", abs(cb - book) < 1e-15, f"{cb!r} vs {book!r}")
        check("flag: one parent edge rescaled", g.meta.get("crack_edges") == 1)
    else:
        check("legacy: booking loses the resignations' credit (0.5 vs 0.625)",
              abs(stop - want) < 1e-12 and abs(book - 0.5) < 1e-12, f"{stop:.6f} / {book:.6f}")

for flag in (False, True):
    g = crack_graph(0.4, flag)              # Black's child scores 0.6 > stop 0.5
    curves, _ = bc.build_curves(g, bmax=4)
    dp = bc.extract_book(g, curves, 2)["booked"]
    pr, _ = bc.price_search(g, 1, log=lambda m: None)
    booked = A in dp and A in pr["booked"]
    check(f"{'flag' if flag else 'legacy'}: a move worth 0.6 over a 0.5 stop is "
          f"{'booked' if flag else 'never booked'} (dp and price)",
          booked == flag, f"dp {sorted(dp)}, price {sorted(pr['booked'])}")

g = crack_graph(0.5, True)
bc.apply_stop_values(g, {A: 0.4}, {})
check("flag: a stop model's W is not re-blended at the node", abs(g.our[A].l_node - 0.4) < 1e-15)
g = crack_graph(0.5, False)
bc.apply_stop_values(g, {A: 0.4}, {})
check("legacy: the stop model's W is blended as before",
      abs(g.our[A].l_node - aux_our_blend(0.4, 100, 100, 1.0)) < 1e-15)

# ── O: off is a no-op ─────────────────────────────────────────────────────────
print("O  flags off")


def snap(g):
    return ({h: (n.l_node, n.cands, n.crack) for h, n in g.our.items()},
            {h: (n.const_base, n.kids, n.floor) for h, n in g.opp.items()})


a = bc.build_graph(root_edges(True), ROOT, True, **kw)
b = bc.build_graph(root_edges(True), ROOT, True, strict_source=False, crack_at_parents=False, **kw)
check("source-move graph: explicit False == default", snap(a) == snap(b))
CE = {R: [{"move_san": "x", "child_hash": A, "total": 1000, "white_score_avg": 0.5}],
      A: [{"move_san": "m", "child_hash": MA, "total": 100, "white_score_avg": 0.4}]}
CA = {A: {"term_normal_total": 100, "term_normal_black_wins": 100}}
a = bc.build_graph(CE, R, False, aux_rows=CA, gates=False, **PURE)
b = bc.build_graph(CE, R, False, aux_rows=CA, gates=False, strict_source=False,
                   crack_at_parents=False, **PURE)
check("crack graph: explicit False == default", snap(a) == snap(b))

# ── F: --min-games from the pool's meta ──────────────────────────────────────
print("F  pool floor")
from build_budget_books import _pool_min_games  # noqa: E402
with tempfile.TemporaryDirectory() as td:
    p = Path(td) / "pool.parquet"
    check("no meta -> None", _pool_min_games(str(p)) is None)
    Path(str(p) + ".meta.json").write_text(json.dumps({"min_games": 20}))
    check("meta floor 20 is read", _pool_min_games(str(p)) == 20)

# ── W: the book frame ────────────────────────────────────────────────────────
print("W  book frame")
from build_budget_books import book_frame  # noqa: E402
late = [{"position_hash": i, "loss_exception": "fallback" if i == 150 else None} for i in range(200)]
wf = book_frame(late)
check("a label after row 100 is kept", wf["loss_exception"].drop_nulls().to_list() == ["fallback"])
nl = book_frame([{"position_hash": 1, "loss_exception": None}])
check("no exceptions: still a string column", nl["loss_exception"].dtype == pl.Utf8, str(nl.schema))
plain = [{"position_hash": i, "value": i / 2, "best_move": None if i % 2 else "e4"} for i in range(9)]
check("a book without the column is the default frame", book_frame(plain).equals(pl.DataFrame(plain)))

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
