"""The referee (convertibility_eval) and its tables (side_explorer): the follow-up review's
R2/R3/R6 fixes, on mock move tables, a mock Maia and a mock eval lookup.

  E  sampling is unbiased: the mean over seeds of a fully sampled walk matches the exact walk
     of the same book, and identical inputs reproduce identical results.
  V  streams and variance agree (R3): the same position started at two plies (a legal knight
     round trip) gets independent draws, so the reported mc_se matches the spread over seeds;
     with one rollout per stratum the SE is reported unavailable, not zero.
  B  book moves are counted in the sampled games too: book + weak moves = our moves per game.
  P  paired differences: a book against itself differs by exactly 0 on every seed.
  F  the fold contract (R2a): tables without _explorer_params.json, or declaring one fold, are
     refused for a bootstrap; folds coarsen by fold % G; replicate weights are shared by every
     table, normalised to keep the total data weight (R2b), and all-zero draws are redrawn.
  S  side_explorer: fold_of nests over powers of two, the sample is game-keyed, and a table dir
     is locked to its settings.
  M  main, end to end on fakes for the engine and Maia: a killed run resumes with its rows and
     leaves; a resume under changed table data, bootstrap seed or options is refused, a reordered
     --seed list is not; an incomplete table is refused; leaves cover the score table. A missing
     book is refused before any engine starts, and a refusal after they start releases them (an
     open engine keeps the process alive, stalling whatever waits on it).

Run: .venv/Scripts/python.exe python/_test_convertibility_eval.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import chess
import duckdb
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import convertibility_eval as cv  # noqa: E402
import side_explorer as sx  # noqa: E402
from eval_arrays import MISSING  # noqa: E402
from zobrist import zobrist_int64  # noqa: E402

FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global FAIL
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not ok:
        FAIL = 1


class NoCounts:
    def get(self, hashes, rep=0):
        return [{} for _ in hashes]


class MockMaia:
    """Three quiet moves where legal (no mate is reachable in the plies these tests walk),
    probabilities fixed by position so every walk sees the same distribution."""
    PREF = ["Nf3", "Nc3", "Nf6", "Nc6", "Ng1", "Nb1", "Ng8", "Nb8", "e3", "e6", "d3", "d6",
            "a3", "a6", "h3", "h6"]

    def probs(self, epds, se, oe, keys):
        out = []
        for e in epds:
            b = chess.Board(e + " 0 1")
            legal = {b.san(m) for m in b.legal_moves}
            ms = [m for m in self.PREF if m in legal][:3]
            w = [3.0, 2.0, 1.0][:len(ms)]
            out.append({m: x / sum(w) for m, x in zip(ms, w)})
        return out


def lookup(hs):
    """A deterministic eval for every position: cp = hash mod 401 - 200."""
    return np.array([(int(h) % 401) - 200 for h in hs], dtype=np.int64)


def opts(**kw):
    base = dict(tau=0.0, rollouts=4, max_ply=6, k=1.0, min_p=0.0, our_elo=1450, opp_elo=2000)
    return SimpleNamespace(**{**base, **kw})


CURVES = ((0.0, 0.004), (0.0, 0.004))
MAIA, NC = MockMaia(), NoCounts()


def score(book, our_white, a, seed=1, starts_override=None):
    ex, starts, ex_book, _ = cv.enumerate_book(book, our_white, 0, a, NC, NC, MAIA, lookup)
    leaves, ro_book = list(ex), 0.0
    if starts_override is not None:
        starts = starts_override
    if starts:
        rl, ro_book = cv.rollouts(starts, book, our_white, 0, a, seed, NC, NC, MAIA, lookup)
        leaves += rl
    v = cv.value_leaves(leaves, our_white, lookup, None, CURVES)
    return {**cv.summarize(v, a.rollouts, a.max_ply), "book_moves_played": ex_book + ro_book}


START = chess.Board()
BOOK_W = {zobrist_int64(START): "e4"}

print("E  sampling vs the exact walk")
for our_white, book in ((True, BOOK_W), (False, {})):
    exact = score(book, our_white, opts())["uniform"]
    runs = [score(book, our_white, opts(tau=1.01), seed=s)["uniform"] for s in range(1, 201)]
    m, se = float(np.mean(runs)), float(np.std(runs, ddof=1) / np.sqrt(len(runs)))
    c = "White" if our_white else "Black"
    check(f"{c}: mean of 200 sampled walks within 4 SE of the exact walk",
          abs(m - exact) < 4 * se, f"{m:.5f} vs {exact:.5f} (se {se:.5f})")
r1 = score(BOOK_W, True, opts(tau=1.01), seed=7)
r2 = score(BOOK_W, True, opts(tau=1.01), seed=7)
check("same inputs and seed -> the same result", r1 == r2)
check("a different seed -> a different sample", r1["uniform"] != score(BOOK_W, True, opts(tau=1.01), seed=8)["uniform"])

print("V  one position started at two plies (a knight round trip)")
# The review's fixture: at the start position Nf3 or e4, 50/50; after Nf3 the knights go
# round (Nf6 Ng1 Ng8) back to the start; after e4 the first legal move by UCI. Starts at plies
# 24 and 28 both sit on the start position, and the game started at 24 is back there at 28.
# The payoff is "the e2 pawn never moved". With the start ply out of the stream key the two
# strata reused draws at ply 28+ and the reported variance came out ~1/1.47 of the true one.


def bh(*sans):
    b = chess.Board()
    for x in sans:
        b.push_san(x)
    return zobrist_int64(b)


h0 = bh()
check("the round trip returns the start position", h0 == bh("Nf3", "Nf6", "Ng1", "Ng8"))
CYCLE = {bh("Nf3"): {"Nf6": 1.0}, bh("Nf3", "Nf6"): {"Ng1": 1.0},
         bh("Nf3", "Nf6", "Ng1"): {"Ng8": 1.0}, h0: {"Nf3": 0.5, "e4": 0.5}}


class CycleMaia:
    def probs(self, epds, se, oe, keys):
        out = []
        for e, k in zip(epds, keys):
            b = chess.Board(e + " 0 1")
            out.append(CYCLE.get(k) or {b.san(sorted(b.legal_moves, key=lambda m: m.uci())[0]): 1.0})
        return out


def zero(hs):
    return np.zeros(len(hs), dtype=np.int64)


E2 = chess.Piece(chess.PAWN, chess.WHITE)
two = [(h0, chess.Board().epd(), 0.5, 24, None, 0.0, 0.0), (h0, chess.Board().epd(), 0.5, 28, None, 0.0, 0.0)]


def cycle_run(seed, K):
    a = opts(rollouts=K, max_ply=30)
    rl, _ = cv.rollouts(two, {}, True, 0, a, seed, NC, NC, CycleMaia(), zero)
    v = cv.value_leaves(rl, True, zero, None, CURVES)
    v["uniform"] = np.array([float(chess.Board(l[5] + " 0 1").piece_at(chess.E2) == E2) for l in rl])
    v["fitted"] = v["uniform"]
    return cv.summarize(v, K, 30)


vals, ses = [], []
for s in range(1, 1501):
    r = cycle_run(s, 2)
    vals.append(r["uniform"])
    ses.append(r["mc_se_uniform"] ** 2)
ratio = float(np.var(vals, ddof=1) / np.mean(ses))
check("observed variance over 1,500 seeds / mean reported variance in [0.85, 1.18]",
      0.85 <= ratio <= 1.18, f"{ratio:.3f} (the old key: ~1.47)")
se_m = float(np.std(vals, ddof=1) / np.sqrt(len(vals)))
check("the target is unchanged: mean within 4 SE of 0.5 x (1/2 + 1/4)",
      abs(np.mean(vals) - 0.375) < 4 * se_m, f"{np.mean(vals):.4f} (se {se_m:.4f})")
check("one rollout per stratum: mc_se unavailable (None), not 0",
      cycle_run(1, 1)["mc_se_uniform"] is None)

print("B  book + weak moves = our moves per game")
for our_white, book in ((True, BOOK_W), (False, {})):
    for tau in (0.0, 1.01, 0.05):
        r = score(book, our_white, opts(tau=tau, max_ply=6))
        ours = 3.0                     # six plies, no game ends: three moves each
        c = "White" if our_white else "Black"
        check(f"{c}, tau {tau}: book {r['book_moves_played']:.4f} + weak "
              f"{r['weak_moves_per_game']:.4f} = 3",
              abs(r["book_moves_played"] + r["weak_moves_per_game"] - ours) < 1e-9
              and r["mass_at_max_ply"] > 1 - 1e-9)

print("X  book exits (stop_alignment's input)")
for our_white, book, ply0 in ((True, BOOK_W, 2), (False, {}, 1)):
    for tau in (0.0, 1.01):
        a = opts(tau=tau)
        ex: dict = {}
        _, starts, _, _ = cv.enumerate_book(book, our_white, 0, a, NC, NC, MAIA, lookup, ex)
        if starts:
            cv.rollouts(starts, book, our_white, 0, a, 3, NC, NC, MAIA, lookup, ex)
        c = "White" if our_white else "Black"
        check(f"{c}, tau {tau}: every game exits once, at ply {ply0}",
              abs(sum(v[0] for v in ex.values()) - 1.0) < 1e-9 and {p for _, p in ex} == {ply0},
              f"{sum(v[0] for v in ex.values()):.6f} {sorted({p for _, p in ex})}")

print("T  direct starts: terminal and horizon positions are leaves as they stand")
MATED_W = "rnb1kbnr/pppp1ppp/8/4p3/6Pq/5P2/PPPPP2P/RNBQKBNR w KQkq -"     # fool's mate, White mated
STALE = "7k/5Q2/6K1/8/8/8/8/8 b - -"                                    # Black stalemated
BARE = "8/8/4k3/8/8/3K4/8/8 w - -"                                      # insufficient material
for our_white in (True, False):
    c = "White" if our_white else "Black"
    a = opts(rollouts=3, max_ply=30)
    for epd, ply, want_ply, want_exact, label in (
            (chess.Board().epd(), 30, 30, None, "at the horizon"),
            (chess.Board().epd(), 34, 34, None, "past the horizon"),
            (MATED_W, 4, 4, 0.0 if our_white else 1.0, "checkmated"),
            (STALE, 20, 20, 0.5, "stalemate"),
            (BARE, 20, 20, 0.5, "insufficient material")):
        st = [(zobrist_int64(chess.Board(epd + " 0 1")), epd, 1.0, ply, None, 0.0, 0.0)]
        rl, bm = cv.rollouts(st, {}, our_white, 0, a, 1, NC, NC, MAIA, lookup)
        ok = (len(rl) == 3 and all(l[6] == want_ply for l in rl)
              and all((l[4] is None) if want_exact is None else (l[4] == want_exact) for l in rl))
        check(f"{c}: {label}: leaves at ply {want_ply}, exact {want_exact}", ok,
              str([(l[6], l[4]) for l in rl]))
a = opts(rollouts=2, max_ply=30)
st = [(zobrist_int64(chess.Board()), chess.Board().epd(), 1.0, 28, None, 0.0, 0.0)]
rl, _ = cv.rollouts(st, {}, True, 0, a, 1, NC, NC, MAIA, lookup)
check("below the horizon: games still end exactly at ply 30", {l[6] for l in rl} == {30})

print("P  paired differences")
rows = []
for s in (1, 2, 3):
    for lab in ("A", "B"):
        rows.append({"label": lab, "replicate": 0, "seed": s,
                     "uniform": score(BOOK_W, True, opts(tau=1.01), seed=s)["uniform"]})
pdiff = cv.paired_differences(pl.DataFrame(rows), "A")
check("a book against itself: difference 0, SE 0, on 3 seeds",
      pdiff.filter(pl.col("label") == "B").select("mean_diff", "se_diff", "seeds").row(0) == (0.0, 0.0, 3))

print("F  fold contract and weights")
with tempfile.TemporaryDirectory() as td:
    td = Path(td)

    def table(name, folds, params=True, rows=None):
        d = td / name
        (d / "month=2026_8" / "bkt=0").mkdir(parents=True)
        rows = rows or [(1, "e4", f % max(folds, 1), 10 + f) for f in range(max(folds, 1))] + \
            [(1, "d4", 0, 7)]
        pl.DataFrame(rows, schema={"parent_hash": pl.Int64, "move_san": pl.Utf8,
                                   "fold": pl.Int16, "total": pl.Int64}, orient="row") \
            .write_parquet(d / "month=2026_8" / "bkt=0" / "part-0000.parquet")
        if params:
            (d / "_explorer_params.json").write_text(json.dumps(
                {"folds": folds, "fold_rule": sx.FOLD_RULE, "sample_rule": sx.SAMPLE_RULE}))
        return str(d)

    con = duckdb.connect()
    no_par, one, f16 = table("nopar", 16, params=False), table("one", 1), table("f16", 16)
    check("no params: refused", cv.Counts(con, no_par, "t").bootstrap_problem() is not None)
    check("declared 1 fold: refused", "unsplit" in (cv.Counts(con, one, "t").bootstrap_problem() or ""))
    try:
        cv.Counts(con, one, "t")._rep([1], 1)
        check("an unsplit table's replicate raises", False)
    except SystemExit:
        check("an unsplit table's replicate raises", True)
    c16 = cv.Counts(con, f16, "t")
    check("16 declared folds: accepted", c16.bootstrap_problem() is None and c16.folds == 16)
    try:
        cv.Counts(con, f16, "t", use_folds=3)
        check("--use-folds 3 of 16: refused", False)
    except SystemExit:
        check("--use-folds 3 of 16: refused", True)
    rep0 = c16.get([1])[0]
    check("replicate 0 = the unsplit counts", rep0 == {"e4": float(sum(10 + f for f in range(16))),
                                                       "d4": 7.0}, str(rep0))
    c4 = cv.Counts(con, f16, "t", use_folds=4)
    w = cv.fold_weights(5, 4, 1)
    want = sum((10 + f) * w[f % 4] for f in range(16))
    check("use_folds 4: replicate counts = fold % 4 groups x weights",
          abs(c4._rep([1], 5)[0]["e4"] - want) < 1e-9)
    ws = [cv.fold_weights(r, 16, 1) for r in range(1, 2001)]
    check("normalised weights sum to F in every replicate", all(abs(x.sum() - 16) < 1e-9 for x in ws))
    raw = [cv.fold_weights(r, 16, 1, "raw") for r in range(1, 2001)]
    sd = float(np.std([x.sum() / 16 for x in raw]))
    check("raw weights move the total by ~1/sqrt(F)", 0.2 < sd < 0.3, f"{sd:.3f}")
    check("weights are a pure function of (rep, F, seed): every table shares them",
          np.array_equal(cv.fold_weights(9, 16, 1), cv.fold_weights(9, 16, 1)))
    zero = next(r for r in range(1, 10000)
                if np.random.Generator(np.random.PCG64([1, r])).poisson(1.0, 2).sum() == 0)
    wz = cv.fold_weights(zero, 2, 1)
    check("an all-zero draw is redrawn", wz.sum() == 2, f"rep {zero}: {wz}")
    two = cv.Counts(con, f"{f16},{one}", "t")
    check("comma-separated dirs that disagree on folds: refused",
          "disagree" in (two.bootstrap_problem() or ""))

print("S  side_explorer")
g = [f"game{i}" for i in range(3000)]
check("fold_of(g, 64) % 16 == fold_of(g, 16)", all(sx.fold_of(x, 64) % 16 == sx.fold_of(x, 16) for x in g))
check("fold_of(g, 1) == 0", all(sx.fold_of(x, 1) == 0 for x in g[:50]))
kept = [x for x in g if sx.in_sample(x, 0.35)]
check("the sample keeps ~35% and is game-keyed", 0.3 < len(kept) / len(g) < 0.4
      and kept == [x for x in g if sx.in_sample(x, 0.35)], f"{len(kept) / len(g):.3f}")
with tempfile.TemporaryDirectory() as td:
    out = Path(td) / "t"
    args = SimpleNamespace(folds=64, sample=0.35, months="2026_8", events="Blitz",
                           our_colour="white", our_elo="1000:1800", opp_min_elo=0, max_ply=30)
    sx.lock_params(out, args)
    sx.lock_params(out, args)
    check("the same settings resume", True)
    try:
        sx.lock_params(out, SimpleNamespace(**{**vars(args), "folds": 16}))
        check("other settings are refused", False)
    except SystemExit:
        check("other settings are refused", True)

with tempfile.TemporaryDirectory() as td:
    td = Path(td)
    src = td / "src.parquet"
    games = ["1. e4 e5 2. Nf3 Nc6 3. Bb5 a6", "1. e4 e5 2. Nf3 Nf6", "1. d4 d5 2. c4 e6",
             "1. e4 c5 2. Nf3 d6 3. d4 cxd4", "1. e4 e5 2. Nf3 Nc6 3. Bc4 Bc5"] * 4
    pl.DataFrame({"movetext": games, "white_elo": [1500] * 20, "black_elo": [1900] * 20,
                  "white_score": [1.0, 0.5, 0.0, 1.0, 0.5] * 4, "white_title": [None] * 20,
                  "black_title": [None] * 20, "termination": ["Normal"] * 20,
                  "game_id": [f"g{i}" for i in range(20)]}).write_parquet(src)
    args = (str(src), True, 1000, 1800, 0, 30, 1.0, 4)
    ps, tm, kept, failed = sx.one_file(args)
    parts = td / "parts"
    parts.mkdir()
    sx.file_to_part((args, str(parts / "p0000")))
    sx.file_to_part(((str(src), True, 1000, 1800, 0, 30, 1.0, 4), str(parts / "p0001")))
    n_ps, n_tm, tot = sx.merge_parts(parts, td / "ps.parquet", td / "tm.parquet", str(td), "1GB")
    got = pl.read_parquet(td / "ps.parquet").sort("parent_hash", "move_san", "fold")
    want = (pl.concat([ps, ps]).group_by("parent_hash", "move_san", "child_hash", "fold")
            .agg(pl.col(c).sum() for c in ("white_wins", "draws", "black_wins", "total"))
            .sort("parent_hash", "move_san", "fold"))
    check("the DuckDB month merge = the polars sum of the parts, schema included",
          got.select(want.columns).equals(want) and got.schema == want.schema
          and tot == 2 * (kept - failed) and n_tm == tm.height,
          f"{got.schema} vs {want.schema}")

print("M  main: resume and fingerprints")
with tempfile.TemporaryDirectory() as td:
    td = Path(td)
    curves = td / "curves.parquet"
    pl.DataFrame({"cell": ["stm1000-1799_vs_opp1800+", "stm1800+_vs_opp1000-1799"],
                  "a": [0.0, 0.0], "k": [0.004, 0.004]}).write_parquet(curves)

    def table(name):
        d = td / name
        (d / "month=2026_8" / "bkt=0").mkdir(parents=True)
        pl.DataFrame({"parent_hash": [1], "move_san": ["e4"], "fold": [0], "total": [10]},
                     schema={"parent_hash": pl.Int64, "move_san": pl.Utf8, "fold": pl.Int16,
                             "total": pl.Int64}).write_parquet(d / "month=2026_8" / "bkt=0" / "part-0000.parquet")
        (d / "_explorer_params.json").write_text(json.dumps(
            {"folds": 4, "fold_rule": sx.FOLD_RULE, "sample_rule": sx.SAMPLE_RULE, "months": "2026_8"}))
        (d / "_month=2026_8.DONE").write_text("ok")
        return d

    ours, opp = table("ours"), table("opp")
    bk = td / "book.parquet"
    pl.DataFrame({"position_hash": [zobrist_int64(chess.Board())], "side_to_move": ["white"],
                  "best_move": ["e4"]}).write_parquet(bk)
    out, lv = td / "r.parquet", td / "l.parquet"

    closed: list[str] = []

    class FakeMaia(MockMaia):
        def __init__(self, *a, **k):
            closed.append("maia started")
            super().__init__(*a, **k)

        def close(self):
            closed.append("maia")

    class FakeSF:
        engine = "fake 1"

        def __init__(self, *a, **k):
            pass

        def __call__(self, todo):
            return {h: 0 for h, _ in todo}

        def close(self):
            closed.append("sf")

    saved = (cv.Maia, cv.StockfishFill, cv.open_eval_arrays, cv.lookup_evals, cv.CURVES, cv.summarize)
    cv.Maia, cv.StockfishFill, cv.open_eval_arrays = FakeMaia, FakeSF, lambda: ()
    cv.lookup_evals, cv.CURVES = (lambda hs, *mm: lookup(hs)), str(curves)

    def run(*extra, seed="1,2", kill_after=None):
        calls = [0]

        def summ(*a_, **k_):
            calls[0] += 1
            if kill_after is not None and calls[0] > kill_after:
                raise RuntimeError("killed")
            return saved[5](*a_, **k_)
        cv.summarize = summ
        argv = sys.argv
        sys.argv = ["convertibility_eval", "--book", f"A=white={bk}", "--book", f"B=white={bk}",
                    "--max-ply", "4", "--tau", "0.2", "--rollouts", "2", "--seed", seed,
                    "--our-moves", str(ours), "--opp-moves", str(opp), "--out", str(out),
                    "--leaves-out", str(lv), "--sf-cache", "", *extra]
        try:
            cv.main()
        finally:
            sys.argv = argv
    partial = Path(f"{out}.partial.parquet")
    try:
        try:
            run(kill_after=3)
            check("the kill fires", False)
        except RuntimeError:
            check("a killed run leaves its rows", pl.read_parquet(partial).height == 3)
        # a score whose leaves were lost is redone, not trusted
        lost = next((td / "l.parquet.parts").glob("b000_s2*"))
        lost.unlink()
        run()
        r = pl.read_parquet(out)
        check("resumed: all four (book, seed) rows", sorted(r.select("label", "seed").rows()) ==
              [("A", 1), ("A", 2), ("B", 1), ("B", 2)], str(r.select("label", "seed").rows()))
        ll = pl.read_parquet(lv)
        check("the leaves cover every row", sorted(ll.select("label", "seed").unique().rows()) ==
              [("A", 1), ("A", 2), ("B", 1), ("B", 2)])
        check("every leaf carries its start's identity",
              {"start_hash", "start_ply", "rollout", "seed"} <= set(ll.columns))
        check("the partial is gone", not partial.exists())

        def killed_partial():
            out.unlink(missing_ok=True)
            partial.unlink(missing_ok=True)
            try:
                run(kill_after=1)
            except RuntimeError:
                pass

        def is_refused(*extra):
            try:
                run(*extra)
                return False
            except SystemExit:
                return True
        part = opp / "month=2026_8" / "bkt=0" / "part-0000.parquet"
        orig = part.read_bytes()
        killed_partial()
        part.write_bytes(orig + b"x")
        check("a table whose data changed is refused, params unchanged", is_refused())
        part.write_bytes(orig)
        killed_partial()
        check("another bootstrap seed is refused", is_refused("--boot-seed", "2"))
        check("another option is refused", is_refused("--k", "50"))
        killed_partial()
        closed.clear()
        check("refused after the engines started", is_refused("--k", "50"))
        check("  and they are released", sorted(closed) == ["maia", "maia started", "sf"], str(closed))
        closed.clear()
        argv = sys.argv
        sys.argv = ["convertibility_eval", "--book", f"A=white={td / 'no_such_book.parquet'}",
                    "--our-moves", str(ours), "--opp-moves", str(opp), "--sf-cache", ""]
        try:
            cv.main()
            check("a missing book is refused", False)
        except SystemExit as e:
            check("a missing book is refused", "no such book" in str(e.code), str(e.code))
        finally:
            sys.argv = argv
        check("  before any engine starts", closed == [], str(closed))
        run(seed="2,1")
        check("a reordered --seed list resumes", pl.read_parquet(out).height == 4)
        (ours / "_month=2026_8.DONE").unlink()
        out.unlink()
        check("a table with an unfinished month is refused", is_refused())
        (ours / "_month=2026_8.DONE").write_text("ok")
    finally:
        cv.Maia, cv.StockfishFill, cv.open_eval_arrays, cv.lookup_evals, cv.CURVES, cv.summarize = saved

print("\nPASS" if FAIL == 0 else "\nFAIL")
sys.exit(FAIL)
