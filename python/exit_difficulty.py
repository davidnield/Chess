"""Measure the risk of losing engine utility in the first unaided moves after book exit.

Use first-exit files from convertibility_eval --exits-out, one book/scenario and all
its seeds. A single stop_alignment exit table (epd, mass, hash, ply) also works.
Paths stop at the absolute horizon or after --decisions of our moves, whichever is
earlier. A loss is the drop in uniform engine utility across one of OUR moves.
"""
import argparse
import json
from pathlib import Path
from types import SimpleNamespace

import chess
import duckdb
import numpy as np
import polars as pl
import pyarrow.parquet as pq

import convertibility_eval as cv
from eval_arrays import lookup_evals, open_eval_arrays
from zobrist import zobrist_int64


def read_exits(paths):
    """Average unconditional exit masses, counting an absent exit as zero."""
    if len({Path(p).resolve() for p in paths}) != len(paths):
        raise ValueError("duplicate exit partition")
    frames = [pl.read_parquet(p) for p in paths]
    for df in frames:
        if len(frames) > 1 and "seed" not in df.columns:
            raise ValueError("multiple exit files must identify their seeds")
    if "seed" in frames[0].columns:
        identities = set()
        seeds = set()
        for path, df in zip(paths, frames):
            raw = (pq.read_metadata(path).metadata or {}).get(b"exit_job")
            if raw is not None:
                label, colour, seed, cfg = json.loads(raw)
                rows = [(label, colour, cfg, seed)]
                if df.height and df.select("book", "colour", "config", "seed").unique().rows() != rows:
                    raise ValueError("exit metadata and rows disagree")
            elif df.height:
                rows = df.select("book", "colour", "config", "seed").unique().rows()
            else:
                raise ValueError("empty exit partition lacks job identity")
            if len(rows) != 1 or rows[0][3] in seeds:
                raise ValueError("expected a different single seed in each partition")
            identities.add(rows[0][:3])
            seeds.add(rows[0][3])
        if len(identities) > 1:
            raise ValueError("exit files describe different books or configurations")
    columns = ["hash", "ply", "epd", "mass"]
    data = pl.concat([df.select(columns) for df in frames])
    if data.filter(pl.col("mass").is_null() | ~pl.col("mass").is_finite() | (pl.col("mass") < 0)).height:
        raise ValueError("invalid exit probability")
    return (data.group_by("hash", "ply", "epd").agg(pl.col("mass").sum() / len(paths))
            .sort(["mass", "hash", "ply"], descending=[True, False, False]))


def edge_risks(edges, games, our_white, threshold, lookup, sf):
    """Per game: any single-move loss >= threshold, and the worst single loss."""
    positions = {}
    for _, _, parent, epd, child, child_epd in edges:
        positions[parent] = epd
        positions[child] = child_epd
    leaves = []
    for h, epd in positions.items():
        board = chess.Board(epd + " 0 1")
        exact = cv._outcome(board, our_white) if board.is_game_over(claim_draw=False) else None
        leaves.append((1., h, None, board.turn == our_white, exact, epd, 0, 0., -1, -1))
    worst = np.zeros(games)
    if leaves:
        from weak_stop import LICHESS_CP_SCALE
        curve = (0., LICHESS_CP_SCALE)
        values = cv.value_leaves(leaves, our_white, lookup, sf, (curve, curve))
        if values["missing"].any():
            raise ValueError("difficulty needs an evaluation for every observed position")
        utility = dict(zip(positions, values["uniform"]))
        for _, k, parent, _, child, _ in edges:
            worst[k] = max(worst[k], utility[parent] - utility[child])
    risk = float((worst >= threshold).mean())
    return risk, float(worst.mean()), float(np.std(worst >= threshold, ddof=1) / np.sqrt(games))


def measure(exits, a, counts, opponent, maia, lookup, sf):
    selected = exits.filter(pl.col("ply") < a.max_ply).head(a.top)
    rows = []
    for row in selected.iter_rows(named=True):
        board = chess.Board(row["epd"] + " 0 1")
        our_white = a.colour == "white"
        if board.turn != our_white or zobrist_int64(board) != row["hash"]:
            raise ValueError("exit board differs from selected colour or hash")
        until = min(a.max_ply, row["ply"] + 2 * a.decisions - 1)
        opts = SimpleNamespace(**{**vars(a), "rollouts": a.games, "max_ply": until})
        start = (row["hash"], row["epd"], 1., row["ply"], None, 0., 0.)
        edges = []
        cv.rollouts([start], {}, our_white, 0, opts, a.seed, counts, opponent, maia,
                    lookup, weak_edges=edges)
        risk, loss, se = edge_risks(edges, a.games, our_white, a.threshold, lookup, sf)
        rows.append({**row, "risk": risk, "risk_mc_se": se, "mean_worst_loss": loss,
                     "mean_decisions": len(edges) / a.games, "loss_threshold": a.threshold,
                     "games": a.games, "seed": a.seed})
    if not rows:
        raise ValueError("no exits within the horizon")
    return pl.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--exits", required=True, nargs="+")
    ap.add_argument("--colour", choices=["white", "black"], required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--our-moves", required=True)
    ap.add_argument("--opp-moves", required=True)
    ap.add_argument("--our-elo", type=int, default=1450)
    ap.add_argument("--opp-elo", type=int, default=2000)
    ap.add_argument("--decisions", type=int, default=3)
    ap.add_argument("--games", type=int, default=32)
    ap.add_argument("--max-ply", type=int, default=30)
    ap.add_argument("--top", type=int, default=24)
    ap.add_argument("--threshold", type=float, default=.10)
    ap.add_argument("--k", type=float, default=100)
    ap.add_argument("--min-p", type=float, default=.005)
    ap.add_argument("--seed", type=int, default=91)
    ap.add_argument("--sf-cache")
    a = ap.parse_args()
    if min(a.top, a.decisions, a.max_ply) < 1 or a.games < 2 or not 0 < a.threshold <= 1:
        ap.error("positive top, decisions, horizon, threshold <= 1, and >= 2 games required")
    exits = read_exits(a.exits)
    arrays = open_eval_arrays()
    def lookup(hashes):
        return lookup_evals(hashes, *arrays)
    with duckdb.connect() as con:
        ours = cv.Counts(con, a.our_moves.format(colour=a.colour), "ours")
        opp = cv.Counts(con, a.opp_moves.format(colour="black" if a.colour=="white" else "white"), "opp")
        maia = cv.Maia()
        try:
            sf = cv.StockfishFill(a.sf_cache, 10, 1)
            try:
                result = measure(exits, a, ours, opp, maia, lookup, sf)
            finally:
                sf.close()
        finally:
            maia.close()
    dest = Path(a.out)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(dest) + ".tmp")
    result.write_parquet(tmp)
    tmp.replace(dest)
    mass = result["mass"].sum()
    print(f"Covered {mass:.4f} of root mass ({exits['mass'].sum():.4f} supplied). "
          f"Conditional risk on covered exits: {result['risk'].dot(result['mass']) / mass:.4f}")


if __name__ == "__main__":
    main()
