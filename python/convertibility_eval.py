"""Convertibility: expected score at ply 30 when OUR side plays the book and then moves like a
1000-1799 player, while the opponent moves like an 1800+ player (user's design, 2026-10-20; the
user's objective restated 2026-10-31: a SIMULATION of how each population moves in each
position, scored by the engine where the simulated games end). The referee every candidate
book is judged by (moved from scratch/ to python/ 2026-10-31, follow-up review R6).

For each book, every branch is followed from the start position to ply 30:
  our turn, in book     the book's move
  our turn, out of book the moves players rated 1000-1799 make there (Aug-Sep 2026 games where
                        the mover was 1000-1799, any opponent: side_explorer movers1000_*)
  opponent's turn       the moves 1800+ players make there (default the Aug-Sep 2026 explorer
                        month of games with mean Elo >= 1800; --opp-moves for a mover table)
Move shares blend the counts with Maia-3 as a K-game prior: (n_m + K p_maia) / (N + K), Maia at
--our-elo vs --opp-elo for our moves and the reverse for theirs; moves under --min-p dropped.

Enumeration and sampling. Branches are enumerated exactly while their probability is at least
--tau (enumerate_book); below it each one -- a (position, ply) stratum -- is continued by
--rollouts sampled games to ply 30 (rollouts; 2026-10-31: the old referee stopped there and
asked the engine, which ended 98-99% of games before ply 30 after about one weak move of ours;
--rollouts 0 reproduces it). A sampled choice is a pure function of (seed, start position,
start ply, game index, ply): strata are independent within a book, and every book that reaches
a stratum samples the same games there, so comparisons between books are paired. mc_se_* is
the sampling SE of one book's score; the SE of a DIFFERENCE between books comes from several
seeds (--seed 1,2,3,4 --compare-to LABEL), each seed one independent set of sampled games.

Leaves (ply 30, game over, or no recorded/predicted move) are valued by the engine (eval
arrays; missing evals by Stockfish at --stockfish-depth, cached on disk) through
  uniform   the Lichess curve
  fitted    fit_eval_curve's 1000-1799-vs-1800+ curves (side to move's own cell)
The book's score is sum over leaves of probability x value, our side's view. Rows also report
the sampled mass, the mean leaf ply, the mass that reaches ply 30, our out-of-book (weak) and
book moves per game (book moves counted in the sampled games too, since 2026-10-31).

Bootstrap. --replicates R reweights whole game folds (fold_weights: the random-group
approximation to the fixed-N game bootstrap, weights normalised to keep the total data weight).
The tables must carry side_explorer's fold contract (_explorer_params.json, F >= 2, one fold
rule across every table); anything else is refused before any work starts.

Resume. --out PATH checkpoints to PATH.partial.parquet; every row carries `config`, a
fingerprint of the options, book files and tables (config_key), and a resume under another
fingerprint is refused.

    PYTHONPATH=python .venv/Scripts/python.exe python/convertibility_eval.py \
        --book L=COLOUR=PATH ... [--tau 1e-4] [--rollouts 4] [--seed 1,2,3,4] [--out PATH]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

import chess
import duckdb
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from duck_compat import duck_pl  # noqa: E402
from eval_arrays import MISSING, lookup_evals, open_eval_arrays  # noqa: E402
from stage3_backwards_induction import LICHESS_CP_SCALE  # noqa: E402
from zobrist import zobrist_int64  # noqa: E402

HX = "E:/chess/holdout_explorer"
CURVES = "E:/chess/calibration/eval_curves_pooled_20261020.parquet"
MAIA_PY = "D:/chess/maia/.venv/Scripts/python.exe"


class Maia:
    """The Maia-3 server process. A request that fails (the server died -- on 2026-10-23 a
    CUDA "unspecified launch failure" killed two servers sharing the GPU) restarts it and
    retries, so one GPU hiccup no longer ends a multi-hour run."""

    def __init__(self):
        self.cache: dict[tuple, dict] = {}
        self.p = None
        self.start()

    def start(self):
        import os
        if self.p is not None:
            self.close()
        env = dict(os.environ, HF_HOME="D:/chess/maia/hf_cache")
        self.p = subprocess.Popen([MAIA_PY, "-I", "-X", "utf8", str(Path(__file__).with_name("maia_server.py"))],
                                  stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
                                  encoding="utf-8", env=env, bufsize=1)
        line = self.p.stdout.readline()
        assert json.loads(line).get("ready"), line

    def close(self):
        try:
            self.p.stdin.close()
            self.p.wait(timeout=60)
        except Exception:
            self.p.kill()

    def ask(self, req: str) -> list:
        for attempt in range(4):
            try:
                self.p.stdin.write(req)
                self.p.stdin.flush()
                return json.loads(self.p.stdout.readline())["probs"]
            except (OSError, ValueError, KeyError) as e:
                print(f"maia request failed ({type(e).__name__}); restarting the server "
                      f"(attempt {attempt + 1})", file=sys.stderr, flush=True)
                time.sleep(10)
                self.start()
        raise RuntimeError("maia server failed 4 times")

    def probs(self, epds, self_elo, oppo_elo, keys):
        todo = [(k, e) for k, e in zip(keys, epds) if (k, self_elo) not in self.cache]
        for s in range(0, len(todo), 20000):
            part = todo[s:s + 20000]
            out = self.ask(json.dumps({"epd": [e for _k, e in part],
                                       "self": [self_elo] * len(part),
                                       "oppo": [oppo_elo] * len(part)}) + "\n")
            for (k, _e), d in zip(part, out):
                self.cache[(k, self_elo)] = d
        return [self.cache[(k, self_elo)] for k in keys]


FOLD_PARAMS = "_explorer_params.json"
CONTRACT_KEYS = ("folds", "fold_rule", "sample_rule")


def table_params(root: str) -> dict | None:
    """A table dir's _explorer_params.json (side_explorer, 2026-10-31+), or None."""
    p = Path(root) / FOLD_PARAMS
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def fold_weights(rep: int, folds: int, seed: int, mode: str = "normalized") -> np.ndarray:
    """Replicate `rep`'s weight on each game fold, from a seeded PCG64 (domain [seed, rep]).

    The resampling target (follow-up review R2) is the ordinary game bootstrap: resample the
    tables' N games with replacement, N fixed, every game moving all of its edges, ours and
    the opponent's, together (a game sits in one fold in every table: side_explorer). The F
    hash folds are random groups of games, and a replicate draws Poisson(1) weights per group
    -- the random-group approximation to that bootstrap.
      normalized (default)  the weights are rescaled to sum to F, so the total data weight is
                            the observed one in every replicate, as it is in the target. Raw
                            Poisson weights moved the total by 1/sqrt(F) (25% at F=16), which
                            moved the balance against the fixed --k Maia prior with it; a
                            per-game bootstrap of millions of games moves it by ~0.
      raw                   the plain Poisson(1) weights (kept for the validation study).
    All-zero draws (probability e^-F) are redrawn from the next substream [seed, rep, i]."""
    for i in range(1000):
        key = [seed, rep] if i == 0 else [seed, rep, i]
        w = np.random.Generator(np.random.PCG64(key)).poisson(1.0, folds).astype(np.float64)
        if w.sum() > 0:
            return w * (folds / w.sum()) if mode == "normalized" else w
    raise RuntimeError("fold_weights: 1000 all-zero draws")


class Counts:
    """Move counts by position from explorer month dirs, fetched a frontier at a time."""

    def __init__(self, con, roots, name, seed: int = 1, use_folds: int | None = None,
                 weights: str = "normalized"):
        """roots: one explorer dir, or several comma-separated (their counts are summed).
        use_folds G: bootstrap over G folds, fold % G (G must divide the tables' F; folds
        nest when F is a power of two). Default F."""
        globs = [f"{r}/month=*/bkt=*/*.parquet" for r in roots.split(",")]
        self.con, self.glob, self.name = con, "[" + ", ".join(f"'{g}'" for g in globs) + "]", name
        self.roots, self.seed, self.weights = roots, seed, weights
        self.cache: dict[int, dict] = {}
        self.rcache: dict[tuple, dict] = {}
        self.fcache: dict[int, dict] = {}
        params = [table_params(r) for r in roots.split(",")]
        self.contract = None
        if all(p is not None for p in params):
            cs = {tuple(p.get(k) for k in CONTRACT_KEYS) for p in params}
            if len(cs) == 1:
                self.contract = dict(zip(CONTRACT_KEYS, cs.pop()))
        self.params = params
        self.folds = 0
        if self.contract and (self.contract["folds"] or 0) >= 2:
            F = int(self.contract["folds"])
            G = use_folds or F
            if F % G or G < 2:
                raise SystemExit(f"{roots}: --use-folds {G} must divide the tables' {F} folds and be >= 2")
            self.table_folds, self.folds = F, G

    def bootstrap_problem(self) -> str | None:
        """Why these tables cannot be bootstrapped (None = they can)."""
        if any(p is None for p in self.params):
            return "no _explorer_params.json (tables built before 2026-10-31, or not by side_explorer)"
        if self.contract is None:
            return "the comma-separated dirs disagree on folds or hash rules"
        if (self.contract["folds"] or 0) < 2:
            return f"declared folds = {self.contract['folds']} (an unsplit table)"
        return None

    def _rep(self, hashes, rep: int) -> list[dict]:
        """Bootstrap replicate `rep`: each game fold's counts weighted by fold_weights. The
        old per-edge Poisson draw (CRC32 as the uniform) is gone: its draws were correlated
        across edges, and drawing edges independently ignores that one game spans many."""
        why = self.bootstrap_problem()
        if why:
            raise SystemExit(f"{self.roots}: no bootstrap: {why}")
        need = [h for h in set(hashes) if h not in self.fcache]
        if need:
            self.con.register("need", pl.DataFrame({"h": need}, schema={"h": pl.Int64}).to_arrow())
            df = duck_pl(self.con, f"""SELECT parent_hash, move_san, fold % {self.folds} f,
                SUM(total)::DOUBLE n FROM read_parquet({self.glob})
                WHERE parent_hash IN (SELECT h FROM need) GROUP BY 1, 2, 3""")
            self.con.unregister("need")
            for h in need:
                self.fcache[h] = {}
            for h, m, f, n in df.iter_rows():
                v = self.fcache[h].setdefault(m.rstrip("+#"), np.zeros(self.folds))
                v[f] += n
        w = fold_weights(rep, self.folds, self.seed, self.weights)
        out = []
        for h in hashes:
            d = self.rcache.get((h, rep))
            if d is None:
                d = {m: float(x) for m, v in self.fcache[h].items() if (x := v @ w) > 0}
                self.rcache[(h, rep)] = d
            out.append(d)
        return out

    def get(self, hashes, rep: int = 0):
        if rep:
            return self._rep(hashes, rep)
        need = [h for h in set(hashes) if h not in self.cache]
        if need:
            self.con.register("need", pl.DataFrame({"h": need}, schema={"h": pl.Int64}).to_arrow())
            df = duck_pl(self.con, f"""SELECT parent_hash, move_san, SUM(total)::DOUBLE n
                FROM read_parquet({self.glob}) WHERE parent_hash IN (SELECT h FROM need)
                GROUP BY 1, 2""")
            self.con.unregister("need")
            for h in need:
                self.cache[h] = {}
            for h, m, n in df.iter_rows():
                d = self.cache[h]
                k = m.rstrip("+#")
                d[k] = d.get(k, 0.0) + n
        return [self.cache[h] for h in hashes]


def blend(counts: dict, maia: dict, k: float, min_p: float) -> dict:
    n = sum(counts.values())
    out = {}
    for san, pm in maia.items():
        p = (counts.get(san, 0.0) + k * pm) / (n + k)
        if p >= min_p:
            out[san] = p
    s = sum(out.values())
    return {m: p / s for m, p in out.items()} if s > 0 else {}


M64 = (1 << 64) - 1


def u01(*xs: int) -> float:
    """A uniform in [0, 1) that is a pure function of the integers xs: splitmix64's mixer
    chained over them (a counter-based generator). Replaces zlib.crc32, which is linear over
    GF(2): inputs that differ in the same bits give outputs that differ in the same bits, so
    two edges' replicate series were almost perfectly correlated (-0.996, review 2026-10-31)."""
    z = 0x9E3779B97F4A7C15
    for x in xs:
        z = ((z ^ (x & M64)) * 0xBF58476D1CE4E5B9) & M64
        z ^= z >> 31
        z = (z * 0x94D049BB133111EB) & M64
        z ^= z >> 29
    return (z >> 11) / 9007199254740992.0


def _outcome(b: chess.Board, our_white: bool) -> float:
    r = b.result(claim_draw=False)
    return 0.5 if r == "1/2-1/2" else (1.0 if (r == "1-0") == our_white else 0.0)


def enumerate_book(book, our_white, rep, a, our_counts, opp_counts, maia, lookup, exits=None):
    """The exact part of the walk: every branch from the start while its probability is at
    least a.tau. Returns (leaves, starts, book_mass, nodes): leaves are
    (prob, hash, cp_fallback, us_to_move, exact|None, epd, ply, weak_sum, start_id, k) with
    start_id = k = -1; starts are the branches under tau, (hash, epd, prob, ply, cp, weak_sum,
    in-book share),
    for rollouts (or engine leaves when a.rollouts == 0, the old referee); book_mass is the
    probability-weighted count of book moves played. exits (a dict, optional) collects the
    book's exits: (hash, ply) -> [probability, epd] of games that reach an our-turn position
    the book has no move for, before any move of ours out of book (the walk carries each
    position's in-book share separately, so paths that merge there are split correctly)."""
    start = chess.Board()
    # frontier: hash -> [epd, prob, last known cp (white view) or None, weak-move sum]
    # (weak-move sum = sum over the paths merged here of prob x our out-of-book moves)
    # (ib = the part of prob whose paths have not yet left the book: exits are counted on it)
    frontier = {zobrist_int64(start): [start.epd(), 1.0, None, 0.0, 1.0]}
    leaves, starts = [], []
    book_mass = 0.0
    nodes = 0

    def put(nxt, ch, epd, pr, cp, ws, ib):
        e = nxt.get(ch)
        if e is None:
            nxt[ch] = [epd, pr, cp, ws, ib]
        else:
            e[1] += pr
            e[3] += ws
            e[4] += ib

    for ply in range(a.max_ply + 1):
        if not frontier:
            break
        nodes += len(frontier)
        hs = list(frontier)
        for h, c in zip(hs, lookup(np.array(hs, dtype=np.int64))):
            if c != MISSING:
                frontier[h][2] = int(c)
        nxt: dict[int, list] = {}
        ours_need, theirs_need = [], []
        for h, (epd, pr, cp, ws, ib) in frontier.items():
            b = chess.Board(epd + " 0 1")
            us = (b.turn == chess.WHITE) == our_white
            if b.is_game_over(claim_draw=False):
                leaves.append((pr, h, cp, us, _outcome(b, our_white), epd, ply, ws, -1, -1))
                continue
            if ply == a.max_ply:
                leaves.append((pr, h, cp, us, None, epd, ply, ws, -1, -1))
                continue
            if pr < a.tau:
                if a.rollouts:
                    starts.append((h, epd, pr, ply, cp, ws, ib / pr))
                else:            # the old referee: an engine leaf right here
                    leaves.append((pr, h, cp, us, None, epd, ply, ws, -1, -1))
                continue
            if us and h in book:
                try:
                    mv = b.parse_san(book[h])
                except ValueError:
                    leaves.append((pr, h, cp, us, None, epd, ply, ws, -1, -1))
                    continue
                book_mass += pr
                b.push(mv)
                put(nxt, zobrist_int64(b), b.epd(), pr, cp, ws, ib)
                continue
            if us and exits is not None and ib > 0.0:
                exits.setdefault((h, ply), [0.0, epd])[0] += ib
            (ours_need if us else theirs_need).append(h)
        for need, cnt, se, oe, weak in ((ours_need, our_counts, a.our_elo, a.opp_elo, True),
                                        (theirs_need, opp_counts, a.opp_elo, a.our_elo, False)):
            if not need:
                continue
            cs = cnt.get(need, rep)
            ms = maia.probs([frontier[h][0] for h in need], se, oe, need)
            for h, c, m in zip(need, cs, ms):
                epd, pr, cp, ws, ib = frontier[h]
                dist = blend(c, m, a.k, a.min_p)
                if not dist:
                    leaves.append((pr, h, cp, weak, None, epd, ply, ws, -1, -1))
                    continue
                b = chess.Board(epd + " 0 1")
                for san, p in dist.items():
                    b.push(b.parse_san(san))
                    put(nxt, zobrist_int64(b), b.epd(), pr * p, cp,
                        p * (ws + (pr if weak else 0.0)), 0.0 if weak else ib * p)
                    b.pop()
        frontier = nxt
    return leaves, starts, book_mass, nodes


def rollouts(starts, book, our_white, rep, a, seed, our_counts, opp_counts, maia, lookup,
             exits=None):
    """Continue every branch the exhaustive walk left under --tau by sampling: --rollouts K
    games each from its position to --max-ply (or game over), moving exactly as the walk does
    (the book where it has a move, else the blended move shares). Each sampled game carries
    prob/K. Returns (leaves, book_mass), leaves in enumerate_book's format with start_id =
    index into starts and k = the game's index.

    Streams (follow-up review R3): the choice at each step is u01(seed, start hash, START PLY,
    k, ply). A start is a (position, ply) stratum -- the walk merges paths by position within
    a ply, so no two starts share both -- and including the start ply makes distinct strata
    independent, which the per-start variance sum in mc_se assumes (without it a position
    started at plies 24 and 28 reused draws at overlapping plies: variance understated ~47% in
    the review's fixture). The same stratum in two books gets the same draws, so book-vs-book
    differences stay paired (common random numbers). exits: as enumerate_book's, from the
    sampled games (each game's first our-turn position out of book)."""
    K = a.rollouts
    # one state per sampled game: [board, hash, weight, ply, cp, weak, start_id, k, in-book
    # share f]: a game from a start whose paths are partly in book carries that share, and its
    # first out-of-book move of ours counts w * f as an exit
    R = []
    leaves = []
    book_mass = 0.0
    for si, (h, epd, pr, ply, cp, ws, f) in enumerate(starts):
        b0 = chess.Board(epd + " 0 1")
        # a start that is already over, or at/past the horizon, is a leaf as it stands (follow-up
        # review of R1-R6, #2): enumerate_book never schedules one, but a direct caller (the
        # stop sampler) can, and playing a move there scored an extra ply or an engine guess
        # instead of the game's result
        if b0.is_game_over(claim_draw=False) or ply >= a.max_ply:
            us0 = (b0.turn == chess.WHITE) == our_white
            ex = _outcome(b0, our_white) if b0.is_game_over(claim_draw=False) else None
            for k in range(K):
                leaves.append((pr / K, h, cp, us0, ex, epd, ply, ws / K, si, k))
            continue
        for k in range(K):
            R.append([b0.copy(stack=False), h, pr / K, ply, cp, ws / K, si, k, f])
    dists: dict[tuple, list] = {}
    while R:
        hs = np.array([r[1] for r in R], dtype=np.int64)
        for r, c in zip(R, lookup(hs)):
            if c != MISSING:
                r[4] = int(c)
        ours, theirs = set(), set()
        for r in R:
            b = r[0]
            us = (b.turn == chess.WHITE) == our_white
            if us and r[1] in book:
                continue
            (ours if us else theirs).add(r[1])
        for need, cnt, se, oe, tag in ((ours, our_counts, a.our_elo, a.opp_elo, "o"),
                                       (theirs, opp_counts, a.opp_elo, a.our_elo, "t")):
            need = [h for h in need if (h, tag) not in dists]
            if not need:
                continue
            epd_of = {}
            for r in R:
                if r[1] not in epd_of:
                    epd_of[r[1]] = r[0].epd()
            cs = cnt.get(need, rep)
            ms = maia.probs([epd_of[h] for h in need], se, oe, need)
            for h, c, m in zip(need, cs, ms):
                dists[(h, tag)] = sorted(blend(c, m, a.k, a.min_p).items())
        nxt = []
        for r in R:
            b, h, w, ply, cp, wk, si, k, f = r
            us = (b.turn == chess.WHITE) == our_white
            if us and h in book:
                try:
                    mv = b.parse_san(book[h])
                except ValueError:
                    leaves.append((w, h, cp, us, None, b.epd(), ply, wk, si, k))
                    continue
                book_mass += w
            else:
                if us and exits is not None and f > 0.0:
                    exits.setdefault((h, ply), [0.0, b.epd()])[0] += w * f
                if us:
                    f = 0.0
                d = dists[(h, "o" if us else "t")]
                if not d:
                    leaves.append((w, h, cp, us, None, b.epd(), ply, wk, si, k))
                    continue
                u, acc, san = u01(seed, starts[si][0], starts[si][3], k, ply), 0.0, d[-1][0]
                for s, p in d:
                    acc += p
                    if u < acc:
                        san = s
                        break
                mv = b.parse_san(san)
                if us:
                    wk += w
            b.push(mv)
            ply += 1
            h2 = zobrist_int64(b)
            if b.is_game_over(claim_draw=False):
                leaves.append((w, h2, cp, (b.turn == chess.WHITE) == our_white,
                               _outcome(b, our_white), b.epd(), ply, wk, si, k))
            elif ply >= a.max_ply:
                leaves.append((w, h2, cp, (b.turn == chess.WHITE) == our_white, None,
                               b.epd(), ply, wk, si, k))
            else:
                nxt.append([b, h2, w, ply, cp, wk, si, k, f])
        R = nxt
    return leaves, book_mass


def value_leaves(leaves, our_white, lookup, sf_fill, curves):
    """Leaves -> per-leaf arrays: the eval (arrays, else Stockfish through sf_fill, else the
    nearest evaluated ancestor's cp), its uniform and fitted utilities, and the exact result
    where the game ended. sf_fill([(hash, epd)]) -> {hash: cp} or None (no Stockfish)."""
    (a_us, k_us), (a_th, k_th) = curves
    ps = np.array([l[0] for l in leaves])
    hs = np.array([l[1] for l in leaves], dtype=np.int64)
    cps = lookup(hs).astype(np.float64)
    fb = np.array([np.nan if l[2] is None else l[2] for l in leaves])
    missing = cps == MISSING
    missing_sf = np.zeros_like(missing)
    if sf_fill is not None and missing.any():
        idx = np.nonzero(missing)[0]
        got = sf_fill([(leaves[i][1], leaves[i][5]) for i in idx])
        for i in idx:
            cps[i] = got[leaves[i][1]]
        missing_sf, missing = missing.copy(), np.zeros_like(missing)
    cps = np.where(missing, fb, cps)
    nocp = np.isnan(cps)
    cps = np.where(nocp, 0.0, cps)
    us = np.array([l[3] for l in leaves])
    exact = np.array([np.nan if l[4] is None else l[4] for l in leaves])
    cp_us = np.clip(cps if our_white else -cps, -1000, 1000)
    uni = 1 / (1 + np.exp(-LICHESS_CP_SCALE * cp_us))
    fit = np.where(us, 1 / (1 + np.exp(-(a_us + k_us * cp_us))),
                   1 - 1 / (1 + np.exp(-(a_th + k_th * -cp_us))))
    return {"ps": ps, "hs": hs, "cp_us": cp_us, "us": us, "exact": exact,
            "uniform": np.where(np.isnan(exact), uni, exact),
            "fitted": np.where(np.isnan(exact), fit, exact),
            "missing": missing, "missing_sf": missing_sf, "nocp": nocp,
            "ply": np.array([l[6] for l in leaves]), "wsum": np.array([l[7] for l in leaves]),
            "sid": np.array([l[8] for l in leaves]), "k": np.array([l[9] for l in leaves])}


def summarize(v, K: int, max_ply: int) -> dict:
    """A book's score and diagnostics from value_leaves' arrays. mc_se_* is the Monte Carlo
    standard error of the sampled part: per start (stratum) the variance of its K games'
    utilities, times its probability squared, over K, summed over the independent strata.
    It needs K >= 2 per stratum and is None (unavailable, not zero) otherwise."""
    ps, rolled = v["ps"], v["sid"] >= 0
    se = {"mc_se_uniform": None, "mc_se_fitted": None}
    if rolled.any() and K >= 2:
        rf = pl.DataFrame({"s": v["sid"][rolled], "w": v["ps"][rolled],
                           "u": v["uniform"][rolled], "f": v["fitted"][rolled]})
        # per-game value within a stratum: the games' leaves summed (a game is one leaf)
        g = rf.group_by("s").agg(pl.col("w").sum().alias("pr"),
                                 pl.col("u").var().alias("vu"), pl.col("f").var().alias("vf"))
        for c, col in (("uniform", "vu"), ("fitted", "vf")):
            se[f"mc_se_{c}"] = float(math.sqrt((g["pr"] ** 2 * g[col].fill_null(0)).sum() / K))
    tot = ps.sum()
    return {"uniform": float((ps * v["uniform"]).sum() / tot),
            "fitted": float((ps * v["fitted"]).sum() / tot),
            "mean_cp_us": float((ps * v["cp_us"]).sum() / tot),
            "mass": float(tot), "leaves": len(ps),
            "eval_missing_mass": float(ps[v["missing"]].sum()),
            "stockfish_mass": float(ps[v["missing_sf"]].sum()),
            "no_eval_mass": float(ps[v["nocp"]].sum()),
            "game_over_mass": float(ps[~np.isnan(v["exact"])].sum()),
            "sampled_mass": float(ps[rolled].sum()),
            "sampled_starts": len(set(v["sid"][rolled].tolist())),
            "mean_leaf_ply": float((ps * v["ply"]).sum() / tot),
            "mass_at_max_ply": float(ps[(v["ply"] >= max_ply) & np.isnan(v["exact"])].sum()),
            "early_nonterminal_mass": float(ps[(v["ply"] < max_ply) & np.isnan(v["exact"])].sum()),
            "weak_moves_per_game": float(v["wsum"].sum() / tot), **se}


def paired_differences(df: pl.DataFrame, base: str, metric: str = "uniform") -> pl.DataFrame:
    """Each book minus `base`, per (replicate, seed), then the mean and its standard error
    over the seeds at replicate 0: the Monte Carlo uncertainty of the DIFFERENCE, which the
    per-book mc_se does not give (the books' samples are paired, so their errors correlate).
    Needs >= 2 seeds for an SE."""
    b = df.filter(pl.col("label") == base).select("replicate", "seed", pl.col(metric).alias("_b"))
    d = (df.join(b, on=["replicate", "seed"]).with_columns((pl.col(metric) - pl.col("_b")).alias("diff"))
         .filter(pl.col("replicate") == 0))
    return (d.group_by("label").agg(pl.col("diff").mean().alias("mean_diff"),
                                    (pl.col("diff").std() / pl.len().sqrt()).alias("se_diff"),
                                    pl.len().alias("seeds")).sort("mean_diff", descending=True))


def _sha(path) -> str | None:
    p = Path(path)
    return hashlib.sha256(p.read_bytes()).hexdigest()[:16] if p.exists() else None


def identity() -> dict:
    """What produced a number, by content: this code, the Maia server and its interpreter, the
    fitted curves, and the eval arrays' own fingerprint file."""
    here = Path(__file__).resolve()
    from eval_arrays import DEFAULT_ARRAY_DIR, META_NAME
    return {"referee": _sha(here), "maia_server": _sha(here.with_name("maia_server.py")),
            "maia_batch": _sha(here.with_name("maia_batch.py")), "maia_python": MAIA_PY,
            "curves": _sha(CURVES), "eval_arrays": _sha(Path(DEFAULT_ARRAY_DIR) / META_NAME)}


def table_manifest(root: str) -> list:
    """One table dir's data files (path, size, mtime), sorted: the data itself, not just its
    declared settings. Refuses a table whose params promise months that are not finished."""
    pr = table_params(root)
    if pr is not None:
        missing = [m for m in str(pr.get("months", "")).split(",")
                   if m and not (Path(root) / f"_month={m}.DONE").exists()]
        if missing:
            raise SystemExit(f"FATAL: {root} is incomplete: no _month=M.DONE for {missing}")
    fs = sorted(Path(root).glob("month=*/bkt=*/*.parquet"))
    return [[str(f), f.stat().st_size, f.stat().st_mtime_ns] for f in fs]


def config_key(a, books, tables, engine: str | None = None) -> str:
    """Fingerprint of everything a result depends on (follow-up review R6): the options that
    change a score (the bootstrap seed included), each book file (path, size, mtime), each
    table's params and data files, the engine that fills leaves and identity(). A resume refuses
    rows made under another key instead of trusting the label and replicate number. Which
    --seed list is run is not part of it: rows are keyed by their own seed."""
    opts = {k: getattr(a, k) for k in ("tau", "rollouts", "max_ply", "k", "min_p", "our_elo",
                                        "opp_elo", "stockfish_depth", "our_moves", "opp_moves",
                                        "use_folds", "fold_weights", "boot_seed")}
    bks = []
    for spec in books:
        label, colour, path = spec.split("=", 2)
        st = Path(path).stat()
        bks.append([label, colour, str(path), st.st_size, st.st_mtime_ns])
    tbs = {r: {"params": c.params, "files": [table_manifest(x) for x in r.split(",")]}
           for r, c in tables.items()}
    blob = json.dumps({"opts": opts, "books": bks, "tables": tbs, "code": identity(),
                       "engine": [SF_PATH, engine]}, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


class StockfishFill:
    """Evals for leaves the arrays lack: Stockfish at a fixed depth, `procs` engines in
    parallel (2 threads, 128 MB each), behind a persistent (hash, depth) -> cp cache.
    The cache's sidecar <cache>.meta.json records the engine that filled it; a different
    engine refuses the cache (its numbers would not be this engine's)."""

    def __init__(self, cache: str | None, depth: int, procs: int = 4):
        import chess.engine
        self.depth, self.path = depth, Path(cache) if cache else None
        self.engines = []
        for _ in range(max(1, procs)):
            e = chess.engine.SimpleEngine.popen_uci(SF_PATH)
            e.configure({"Threads": 2, "Hash": 128})
            self.engines.append(e)
        self.engine = self.engines[0].id.get("name", "stockfish")
        self.cache: dict[int, int] = {}
        if self.path is not None:
            mp = Path(str(self.path) + ".meta.json")
            if mp.exists():
                have = json.loads(mp.read_text(encoding="utf-8")).get("engine")
                if have != self.engine:
                    raise SystemExit(f"FATAL: {self.path} was filled by {have}, not {self.engine}")
            elif self.path.exists():      # filled before 2026-10-31 by the engine in use since
                mp.write_text(json.dumps({"engine": self.engine, "assumed": True}), encoding="utf-8")
            if self.path.exists():
                c = pl.read_parquet(self.path).filter(pl.col("depth") == depth)
                self.cache = dict(zip(c["hash"].to_list(), c["cp"].to_list()))
                print(f"stockfish cache: {len(self.cache):,} positions at depth {depth}", flush=True)
        self._n0 = len(self.cache)

    def save(self):
        if self.path is None or len(self.cache) == self._n0:
            return
        new = pl.DataFrame({"hash": list(self.cache), "cp": list(self.cache.values())},
                           schema={"hash": pl.Int64, "cp": pl.Int32}).with_columns(
            pl.lit(self.depth, pl.Int32).alias("depth"))
        if self.path.exists():
            old = pl.read_parquet(self.path).filter(pl.col("depth") != self.depth)
            new = pl.concat([old, new.select(old.columns)])
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        new.write_parquet(tmp)
        tmp.replace(self.path)
        Path(str(self.path) + ".meta.json").write_text(json.dumps({"engine": self.engine}),
                                                         encoding="utf-8")
        self._n0 = len(self.cache)

    def __call__(self, todo):
        """[(hash, epd)] -> {hash: cp}, the engines in parallel (one thread per engine)."""
        import chess.engine
        from concurrent.futures import ThreadPoolExecutor
        need = [(h, e) for h, e in dict(todo).items() if h not in self.cache]

        def work(i):
            eng = self.engines[i]
            for h, epd in need[i::len(self.engines)]:
                info = eng.analyse(chess.Board(epd + " 0 1"), chess.engine.Limit(depth=self.depth))
                self.cache[h] = info["score"].white().score(mate_score=2000)
        if need:
            with ThreadPoolExecutor(len(self.engines)) as ex:
                list(ex.map(work, range(len(self.engines))))
            self.save()
        return self.cache

    def close(self):
        for e in self.engines:
            e.quit()


SF_PATH = "C:/Users/David/AppData/Local/Microsoft/WinGet/Links/stockfish.exe"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--book", action="append", required=True, metavar="LABEL=COLOUR=PATH")
    ap.add_argument("--tau", type=float, default=1e-4,
                    help="branches under this probability are continued by sampling (--rollouts) "
                         "instead of being enumerated")
    ap.add_argument("--rollouts", type=int, default=4,
                    help="sampled games per branch under --tau (2026-10-31). 0 = the old referee: "
                         "an engine leaf right there, which ended 98-99%% of games before ply 30 "
                         "(mean leaf ply ~16, ~1.1 weak moves of ours per game)")
    ap.add_argument("--seed", default="1",
                    help="the sampled continuations' stream; a comma-separated list runs each "
                         "seed on one exact walk (rows carry `seed`), which is what "
                         "--compare-to needs for the SE of a book-vs-book difference")
    ap.add_argument("--compare-to", default=None, metavar="LABEL",
                    help="print every book minus this one, per seed, with the SE over seeds")
    ap.add_argument("--sf-procs", type=int, default=4, help="Stockfish processes for missing evals")
    ap.add_argument("--sf-cache", default="E:/chess/calibration/convertibility_sf_cache.parquet",
                    help="persistent (hash, depth) -> cp cache for --stockfish-depth leaves")
    ap.add_argument("--max-ply", type=int, default=30)
    ap.add_argument("--k", type=float, default=100.0)
    ap.add_argument("--min-p", type=float, default=0.005)
    ap.add_argument("--our-elo", type=int, default=1450)
    ap.add_argument("--opp-elo", type=int, default=2000)
    ap.add_argument("--out", default=None)
    ap.add_argument("--replicates", type=int, default=0,
                    help="also score R bootstrap replicates (paired across books): game folds "
                         "reweighted (see fold_weights), so every table must carry the fold "
                         "contract (side_explorer --folds F >= 2); rows carry `replicate` "
                         "(0 = as observed). Refused before any work if a table lacks it")
    ap.add_argument("--boot-seed", type=int, default=1,
                    help="the fold weights' stream, shared by every table and book in a run")
    ap.add_argument("--use-folds", type=int, default=None,
                    help="bootstrap over G folds (fold %% G; G divides the tables' F)")
    ap.add_argument("--fold-weights", choices=["normalized", "raw"], default="normalized",
                    help="see fold_weights; raw only for the validation study")
    ap.add_argument("--leaves-out", default=None, help="write every book's valued leaves (prob, cp_us, ...) here")
    ap.add_argument("--stockfish-depth", type=int, default=10,
                    help="evaluate leaves missing from the eval arrays with Stockfish (0 = use the "
                         "nearest evaluated ancestor)")
    ap.add_argument("--our-moves", default=HX + "/movers1000_{colour}_2026_08_09",
                    help="explorer dir(s) of our side's moves, comma-separated ({colour} = our colour)")
    ap.add_argument("--opp-moves", default=HX + "/ge1800_brc_2026_08_09",
                    help="explorer dir(s) of the opponent's moves, comma-separated "
                         "({colour} = the opponent's colour); default the 1800+ month")
    a = ap.parse_args()
    seeds = [int(s) for s in str(a.seed).split(",")]
    missing = [spec for spec in a.book if not Path(spec.split("=", 2)[2]).exists()]
    if missing:
        raise SystemExit(f"FATAL: no such book: {missing}")
    con = duckdb.connect()
    con.execute("SET threads=8; SET memory_limit='16GB'")
    mm = open_eval_arrays()

    def lookup(hs):
        return lookup_evals(hs, *mm)

    tables: dict[str, Counts] = {}    # one cache per explorer dir set, shared across books
    for spec in a.book:
        colour = spec.split("=", 2)[1]
        for r, nm in ((a.our_moves.format(colour=colour), "ours"),
                      (a.opp_moves.format(colour="black" if colour == "white" else "white"), "opp")):
            if r not in tables:
                tables[r] = Counts(con, r, nm, a.boot_seed, a.use_folds, a.fold_weights)
    if a.replicates:
        # every problem up front, not after hours of replicate 0 (follow-up review R2a)
        bad = {r: c.bootstrap_problem() for r, c in tables.items() if c.bootstrap_problem()}
        if bad:
            raise SystemExit("FATAL: --replicates over tables that cannot be bootstrapped:\n"
                             + "\n".join(f"  {r}: {w}" for r, w in bad.items()))
        cs = {tuple(c.contract[k] for k in CONTRACT_KEYS) + (c.folds,) for c in tables.values()}
        if len(cs) != 1:
            raise SystemExit(f"FATAL: the tables disagree on the fold contract: {sorted(cs)} -- a "
                             f"game must carry one weight in every table")
    maia = Maia()
    sf = None
    try:
      sf = StockfishFill(a.sf_cache, a.stockfish_depth, a.sf_procs)
      cfg = config_key(a, a.book, tables, sf.engine)
      cv = {r["cell"]: (r["a"], r["k"]) for r in pl.read_parquet(CURVES).iter_rows(named=True)}
      curves = (cv["stm1000-1799_vs_opp1800+"], cv["stm1800+_vs_opp1000-1799"])
      rows = []
      leaf_dir = Path(f"{a.leaves_out}.parts") if a.leaves_out else None
      if leaf_dir is not None:
          leaf_dir.mkdir(parents=True, exist_ok=True)

      def leaf_part(label, seed):
          # by position in --book: labels may hold anything a file name may not
          return leaf_dir / f"b{[s.split('=', 1)[0] for s in a.book].index(label):03d}_s{seed}.parquet"
      partial = Path(f"{a.out}.partial.parquet") if a.out else None
      done: set[tuple] = set()
      if partial is not None and partial.exists():
          rows = pl.read_parquet(partial).to_dicts()
          other = {r.get("config") for r in rows} - {cfg}
          if other:
              raise SystemExit(f"FATAL: {partial} holds rows made under another configuration "
                               f"{sorted(map(str, other))} (this one is {cfg}); delete it to start over")
          if leaf_dir is not None:      # a score whose leaves were not kept is redone
              rows = [r for r in rows if r["replicate"] != 0 or leaf_part(r["label"], r["seed"]).exists()]
          done = {(r["replicate"], r["label"], r["seed"]) for r in rows}
          print(f"resuming: {len(done)} (replicate, book, seed) results already saved", flush=True)
      for rep in range(a.replicates + 1):
        for spec in a.book:
            label, colour, path = spec.split("=", 2)
            todo = [s for s in seeds if (rep, label, s) not in done]
            if not todo:
                continue
            t0 = time.time()
            our_white = colour == "white"
            our_counts = tables[a.our_moves.format(colour=colour)]
            opp_counts = tables[a.opp_moves.format(colour="black" if our_white else "white")]
            bd = pl.read_parquet(path, columns=["position_hash", "side_to_move", "best_move"])
            book = {h: m for h, s, m in bd.iter_rows() if s == colour and m}
            ex_leaves, starts, ex_book, nodes = enumerate_book(
                book, our_white, rep, a, our_counts, opp_counts, maia, lookup)
            for seed in todo:
                leaves, ro_book = list(ex_leaves), 0.0
                if starts:
                    rl, ro_book = rollouts(starts, book, our_white, rep, a, seed,
                                           our_counts, opp_counts, maia, lookup)
                    leaves += rl
                v = value_leaves(leaves, our_white, lookup,
                                 sf if a.stockfish_depth > 0 else None, curves)
                row = {"label": label, "colour": colour, "replicate": rep, "seed": seed,
                       "book_moves": len(book), **summarize(v, a.rollouts, a.max_ply),
                       "nodes": nodes, "book_moves_played": ex_book + ro_book,
                       "rollouts": a.rollouts, "tau": a.tau, "config": cfg,
                       "secs": round(time.time() - t0)}
                if leaf_dir is not None and rep == 0:      # before the row that counts on them
                    sid = v["sid"]
                    lf = pl.DataFrame({
                        "label": [label] * len(leaves), "seed": seed, "prob": v["ps"],
                        "cp_us": v["cp_us"], "us_to_move": v["us"], "exact": v["exact"],
                        "uniform": v["uniform"], "fitted": v["fitted"], "ply": v["ply"],
                        "sampled": sid >= 0, "start_id": sid, "rollout": v["k"],
                        "start_hash": [starts[i][0] if i >= 0 else None for i in sid.tolist()],
                        "start_ply": [starts[i][3] if i >= 0 else None for i in sid.tolist()],
                        "hash": v["hs"], "epd": [l[5] for l in leaves],
                        "stockfish": v["missing_sf"]})
                    lp = leaf_part(label, seed)
                    lf.write_parquet(lp.with_suffix(".tmp"))
                    lp.with_suffix(".tmp").replace(lp)
                rows.append(row)
                if partial is not None:      # checkpoint: a crash or shutdown loses one walk
                    tmp = partial.with_suffix(".tmp")
                    pl.DataFrame(rows).write_parquet(tmp)
                    tmp.replace(partial)
                print(json.dumps(row), flush=True)
                t0 = time.time()
    finally:
        # always release the engines: an exception used to leave the process alive (the
        # Stockfish thread), stalling the chain behind it for hours
        if sf is not None:
            sf.close()
        maia.close()
    df = pl.DataFrame(rows)
    if a.out:
        tmp = Path(a.out + ".tmp")
        df.write_parquet(tmp)
        tmp.replace(a.out)
        if partial is not None and partial.exists():
            partial.unlink()
    if a.leaves_out:
        # every replicate-0 row has its leaves, resumed or not
        parts = [leaf_part(r["label"], r["seed"]) for r in rows if r["replicate"] == 0]
        tmp = Path(a.leaves_out + ".tmp")
        pl.concat([pl.read_parquet(x) for x in parts]).write_parquet(tmp)
        tmp.replace(a.leaves_out)
    with pl.Config(tbl_rows=60, tbl_width_chars=200, float_precision=4):
        print(df.select("label", "colour", "seed", "book_moves", "uniform", "fitted",
                        "mc_se_uniform", "book_moves_played", "stockfish_mass", "secs"))
        if a.compare_to:
            print(paired_differences(df, a.compare_to))


if __name__ == "__main__":
    sys.exit(main())
