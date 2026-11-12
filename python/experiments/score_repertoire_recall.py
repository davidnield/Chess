"""Paired full-game Monte Carlo pilot for the current Rapid population.

Each schedule is a paired block. Sample recall afresh at every visit, including
re-entry. Use the production referee's human move policies and leaf valuation.
"""
import argparse
from contextlib import closing, ExitStack
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sys

import chess
import duckdb
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import convertibility_eval as ref


@dataclass
class Game:
    label: str
    colour: str
    model: str
    number: int
    board: chess.Board
    recalled: int = 0
    unaided: int = 0
    found: int = 0


def human_moves(positions, ours, counts, maia, cache):
    missing = sorted(h for h in positions if (ours, h) not in cache)
    if not missing:
        return
    empirical = counts.get(missing)
    elos = (1450, 1900) if ours else (1900, 1450)
    pred = maia.probs([positions[h].epd() for h in missing], *elos, missing)
    for h, obs, prior in zip(missing, empirical, pred):
        dist = sorted(ref.blend(obs, prior, 30., .005).items())
        if not dist:
            raise ValueError(f"Empty human policy at {positions[h].fen()}")
        moves, weights = zip(*dist)
        cdf = np.cumsum(weights)
        cdf[-1] = 1.
        cache[ours, h] = moves, cdf


def play(arms, profiles, seed, games, counts, maia, cache, lookup, sf, curves):
    live = [Game(arm["label"], arm["colour"], name, k, chess.Board())
            for arm in arms for name in profiles[arm["label"]] for k in range(games)]
    rng = np.random.default_rng(np.random.SeedSequence([20261109, seed]))
    recall_draw, human_draw = rng.random((2, 30, games))
    done = []
    for ply in range(30):
        need = {True: {}, False: {}}
        human, played = [], []
        for g in live:
            b = g.board
            h = ref.zobrist_int64(b)
            ours = (b.turn == chess.WHITE) == (g.colour == "white")
            card = profiles[g.label][g.model].get(h) if ours else None
            if card and recall_draw[ply, g.number] < card[1]:
                g.recalled += 1
                played.append((g, card[0]))
            else:
                need[ours][h] = b
                human.append((g, h, ours, card))
        for ours in (True, False):
            human_moves(need[ours], ours, counts[ours], maia, cache)
        for g, h, ours, card in human:
            moves, cdf = cache[ours, h]
            move = moves[np.searchsorted(cdf, human_draw[ply, g.number], side="right")]
            if card:
                g.unaided += 1
                g.found += int(move.rstrip("+#") == card[0].rstrip("+#"))
            played.append((g, move))
        live = []
        for g, move in played:
            g.board.push_san(move)
            if ply == 29 or g.board.is_game_over(claim_draw=False):
                done.append(g)
            else:
                live.append(g)
    rows = []
    for colour in sorted({g.colour for g in done}):
        batch = [g for g in done if g.colour == colour]
        white = colour == "white"
        leaves = []
        for g in batch:
            b = g.board
            exact = ref._outcome(b, white) if b.is_game_over(claim_draw=False) else None
            leaves.append((1., ref.zobrist_int64(b), None, (b.turn == chess.WHITE) == white,
                           exact, b.epd(), b.ply(), 0., 0, g.number))
        v = ref.value_leaves(leaves, white, lookup, sf, curves)
        for i, g in enumerate(batch):
            rows.append(dict(label=g.label, model=g.model, seed=seed, game=g.number,
                ply=g.board.ply(), score=v["uniform"][i], fitted=v["fitted"][i],
                recalled=g.recalled, unaided_book_visits=g.unaided, found=g.found))
    return pl.DataFrame(rows)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--profiles", required=True, type=Path)
    p.add_argument("--models", required=True, type=Path, help="rapid-now population model folder")
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--games", type=int, default=32, help="games per schedule per scenario")
    p.add_argument("--blocks", type=int, default=8)
    p.add_argument("--scenarios", nargs="+", default=["perfect", "scratch-central-30",
        "scratch-central-90", "scratch-central-240", "scratch-low-90", "scratch-high-90"])
    a = p.parse_args()
    design = json.loads((a.profiles / "design.json").read_text())
    arms = design["arms"]
    if not 1 <= a.blocks <= design["schedules"] or a.games < 1:
        p.error("invalid block or game count")
    if a.models.name != "rapid-now":
        p.error("this pilot uses the current Rapid Elo settings")
    a.out.mkdir(parents=True, exist_ok=True)
    cards = {}
    for arm in arms:
        label = arm["label"]
        cards[label] = dict(pl.read_parquet(a.profiles / f"{label}-cards.parquet")
                            .select("position_hash", "best_move").iter_rows())
    print("Loaded prescriptions, opening Rapid populations and evaluation arrays", flush=True)
    with ExitStack() as stack:
        con = stack.enter_context(closing(duckdb.connect()))
        con.execute("SET threads=2")
        counts = {ours: ref.Counts(con, str(a.models / sub), sub)
                  for ours, sub in [(True, "current"), (False, "borrowed")]}
        arrays = ref.open_eval_arrays()
        cv = {r["cell"]: (r["a"], r["k"]) for r in pl.read_parquet(ref.CURVES).to_dicts()}
        curves = cv["stm1000-1799_vs_opp1800+"], cv["stm1800+_vs_opp1000-1799"]
        print("Opening the pilot's Stockfish cache", flush=True)
        sf = stack.enter_context(closing(ref.StockfishFill(str(a.out / "sf.parquet"), 10, 1)))
        meta = dict(design=design, scenarios=a.scenarios, games=a.games, referee=ref.identity(),
                    stockfish=sf.engine, depth=10, code=ref._sha(__file__),
                    tables={str(x.roots):ref.table_manifest(x.roots) for x in counts.values()})
        def lookup(hs):
            return ref.lookup_evals(hs, *arrays)
        maia = None
        cache = {}
        for seed in range(a.blocks):
            profiles, sources = {}, {}
            for label, book in cards.items():
                profiles[label] = {}
                for name in a.scenarios:
                    if name == "perfect":
                        prob = dict.fromkeys(book, 1.)
                    else:
                        start, curve, day = name.split("-")
                        path = a.profiles / label / start / curve / f"{seed}.parquet"
                        sources[str(path)] = ref._sha(path)
                        prob = dict(pl.read_parquet(path).select("position_hash", f"day{day}").iter_rows())
                    if book.keys() != prob.keys() or not all(0 <= r <= 1 for r in prob.values()):
                        raise ValueError(f"Incomplete or invalid recall profile for {label} {name}")
                    profiles[label][name] = {h:(m,prob[h]) for h,m in book.items()}
            key = hashlib.sha256(json.dumps([meta, sources, cards],sort_keys=True).encode()).hexdigest()
            dest = a.out / f"seed-{seed}.parquet"
            if dest.exists():
                import pyarrow.parquet as pq
                if pq.read_metadata(dest).metadata.get(b"config") != key.encode():
                    raise ValueError(f"{dest}: existing block uses a different configuration")
                continue
            if maia is None:
                print("Loading Maia for the pilot", flush=True)
                maia = stack.enter_context(closing(ref.Maia()))
            print(f"Scoring paired block {seed+1}/{a.blocks}", flush=True)
            result = play(arms, profiles, seed, a.games, counts, maia, cache, lookup, sf, curves)
            expected = len(arms) * len(a.scenarios) * a.games
            if result.height != expected or result.unique(["label","model","game"]).height != expected:
                raise ValueError("paired games lost or duplicated")
            tmp = dest.with_suffix(".tmp")
            result.write_parquet(tmp, metadata={"config":key})
            tmp.replace(dest)
            print(f"Saved {dest.name}, {result.height} trajectories", flush=True)


if __name__ == "__main__":
    main()
