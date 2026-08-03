"""
Export a Stage-3 repertoire as a variation-tree PGN for import into Chessbook
(https://chessbook.com -> Import -> PGN), or any PGN-consuming trainer.

The walk starts at the standard start position. At OUR turns it plays the
repertoire's #1 move (best_move) — one move, no siblings. At OPPONENT turns it
branches over their recorded replies, most-played first (first reply = PGN
mainline, the rest = parenthesised variations), pruned by:

  --min-reach   cumulative probability of the line VIA THIS MOVE ORDER (product
                of the opponent's empirical reply shares; our moves don't reduce
                reach). Deliberately path-based, NOT transposition-summed: move
                order is part of the repertoire's value (it tricks opponents out
                of their preferred setups), so a line is kept by how often WE
                reach it in our order. Note Chessbook's "1 in N games" IS
                transposition-summed, so it can rank a position 2-5x higher
                than our path reach (measured on the Scandinavian ...Qa5
                tabiya) and may occasionally ask about a pruned branch — use
                Chessbook's own prune/review prioritization for those. Default
                0.0002 (1-in-5000) is conservative for exactly that reason.
  --max-branches   cap on replies per opponent node (after the reach cut).
  --max-ply     hard depth stop.

A branch is only emitted if we have a booked answer to it (the reply's child
position is in the repertoire with a best_move) — an opponent move we never
answer trains nothing. Lines also stop where OUR book truncates (best_move null
past the eval-DB frontier) or the game ends (mate). Transpositions are expanded
per move-order (PGN is a tree, the repertoire is a DAG); Chessbook merges
transpositions on import, and the reach cut keeps the duplication bounded.

The output is ONE PGN game per repertoire with nested variations. The file is
round-trip validated with python-chess before the script reports success.

Usage:
    # Canonical pair, both colors, defaults (writes E:/chess/repertoire/exports/):
    .venv/Scripts/python.exe python/export_chessbook.py

    # One rep, custom pruning:
    .venv/Scripts/python.exe python/export_chessbook.py \\
        --repertoire E:/chess/repertoire/repertoire_pooled_white_sharp.parquet \\
        --perspective white --min-reach 0.002 --output my_white.pgn
"""
from __future__ import annotations

import argparse
import sys
import time
from collections import defaultdict
from pathlib import Path

import chess
import chess.pgn
import chess.polyglot
import polars as pl

from zobrist import zobrist_int64

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

DEFAULT_STATS = Path("E:/chess/position-stats/position_stats_pooled_ge1800_2013_2025_brc.parquet")
DEFAULT_REPS = [
    ("white", Path("E:/chess/repertoire/repertoire_pooled_white_sharp.parquet")),
    ("black", Path("E:/chess/repertoire/repertoire_pooled_black_sharp.parquet")),
]
DEFAULT_OUT_DIR = Path("E:/chess/repertoire/exports")


def load_rep(path: Path) -> dict[int, tuple[str, str | None]]:
    """position_hash -> (side_to_move, best_move)."""
    df = pl.read_parquet(path, columns=["position_hash", "side_to_move", "best_move"])
    return {h: (s, b) for h, s, b in zip(df["position_hash"], df["side_to_move"],
                                         df["best_move"])}


def load_edges(path: Path) -> dict[int, list[tuple[str, int, int]]]:
    """parent_hash -> [(san, child_hash, total)], most-played first."""
    df = pl.read_parquet(path, columns=["parent_hash", "move_san", "child_hash", "total"])
    edges: dict[int, list[tuple[str, int, int]]] = defaultdict(list)
    for p, m, c, t in zip(df["parent_hash"], df["move_san"], df["child_hash"], df["total"]):
        edges[p].append((m, c, t))
    for v in edges.values():
        v.sort(key=lambda e: -e[2])
    return edges


def build_game(rep: dict, edges: dict, perspective: str, min_reach: float,
               max_branches: int, max_ply: int, label: str,
               annots: dict | None = None) -> tuple[chess.pgn.Game, dict]:
    game = chess.pgn.Game()
    game.headers["Event"] = label
    game.headers["White"] = f"{perspective.capitalize()} repertoire" if perspective == "white" else "Opponent field"
    game.headers["Black"] = f"{perspective.capitalize()} repertoire" if perspective == "black" else "Opponent field"
    game.headers["Result"] = "*"

    stats = {"our_moves": 0, "opp_branches": 0, "leaves": 0, "max_ply": 0,
             "pruned_reach": 0, "pruned_cap": 0, "pruned_unanswered": 0}
    start = chess.Board()
    # Iterative DFS: (pgn_node, board, path_reach, ply). Each entry's board is a
    # copy — ~tree-node count boards live transiently; the reach cut keeps that small.
    stack = [(game, start, 1.0, 0)]
    while stack:
        node, board, path_reach, ply = stack.pop()
        stats["max_ply"] = max(stats["max_ply"], ply)
        if ply >= max_ply or board.is_game_over():
            stats["leaves"] += 1
            continue
        ph = zobrist_int64(board)
        row = rep.get(ph)
        our_turn = (board.turn == chess.WHITE) == (perspective == "white")
        if our_turn:
            best = row[1] if row else None
            if best is None:
                stats["leaves"] += 1      # book truncates here
                continue
            try:
                mv = board.parse_san(best)
            except ValueError:
                stats["leaves"] += 1      # stale SAN (shouldn't happen)
                continue
            child = node.add_variation(mv)
            if annots:                      # attach the study note as a PGN comment
                a = annots.get(ph)
                if a and a.get("annotation"):
                    txt = a["annotation"]
                    if a.get("memory_rule"):
                        txt += f"  [Memory: {a['memory_rule']}]"
                    child.comment = txt
            b2 = board.copy(stack=False)
            b2.push(mv)
            stats["our_moves"] += 1
            stack.append((child, b2, path_reach, ply + 1))
        else:
            replies = edges.get(ph, [])
            tot = sum(t for _, _, t in replies)
            if tot == 0:
                stats["leaves"] += 1
                continue
            kept = []
            for san, ch, t in replies:
                r2 = path_reach * (t / tot)
                if r2 < min_reach:
                    stats["pruned_reach"] += 1
                    continue
                crow = rep.get(ch)
                if crow is None or crow[1] is None:
                    stats["pruned_unanswered"] += 1   # no booked answer -> skip
                    continue
                kept.append((san, r2))
            if len(kept) > max_branches:
                stats["pruned_cap"] += len(kept) - max_branches
                kept = kept[:max_branches]
            if not kept:
                stats["leaves"] += 1
                continue
            for san, r2 in kept:
                try:
                    mv = board.parse_san(san)
                except ValueError:
                    continue
                child = node.add_variation(mv)   # 1st add = mainline, rest = variations
                b2 = board.copy(stack=False)
                b2.push(mv)
                stats["opp_branches"] += 1
                stack.append((child, b2, r2, ply + 1))
    return game, stats


def validate(pgn_path: Path) -> int:
    """Round-trip parse; returns the total move count. Raises on parser errors."""
    with open(pgn_path, encoding="utf-8") as f:
        game = chess.pgn.read_game(f)
    if game is None:
        raise SystemExit(f"FATAL: {pgn_path} did not parse as a PGN game")
    if game.errors:
        raise SystemExit(f"FATAL: parser errors in {pgn_path}: {game.errors[:3]}")
    n = 0
    todo = [game]
    while todo:
        nd = todo.pop()
        for v in nd.variations:
            n += 1
            todo.append(v)
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repertoire", default=None,
                    help="Repertoire parquet. Default: canonical white AND black pair.")
    ap.add_argument("--perspective", choices=["white", "black"], default=None,
                    help="Required with --repertoire.")
    ap.add_argument("--stats", default=str(DEFAULT_STATS))
    ap.add_argument("--output", default=None,
                    help="Output PGN path (default: exports/chessbook_<persp>.pgn).")
    ap.add_argument("--min-reach", type=float, default=0.0002,
                    help="Keep opponent branches whose line, VIA THIS MOVE ORDER, is met in "
                         ">= this share of games (default 0.0002 = 1-in-5000). Path-based by "
                         "design — see the module docstring; Chessbook's transposition-summed "
                         "incidence can rank a pruned branch higher, its prune/review handles those.")
    ap.add_argument("--max-branches", type=int, default=12,
                    help="Max opponent replies per node after the reach cut (default 12).")
    ap.add_argument("--max-ply", type=int, default=40,
                    help="Hard depth stop in plies (default 40).")
    ap.add_argument("--annotations", default=None,
                    help="annotations.parquet — attach study notes as PGN comments "
                         "on our moves (e.g. trainer_data/annotations/annotations.parquet).")
    args = ap.parse_args()

    annots_by_color: dict[str, dict] = {}
    if args.annotations and Path(args.annotations).exists():
        for r in pl.read_parquet(args.annotations).iter_rows(named=True):
            if r["annotation"]:
                annots_by_color.setdefault(r["color"], {})[r["position_hash"]] = {
                    "annotation": r["annotation"], "memory_rule": r["memory_rule"]}

    if args.repertoire:
        if not args.perspective:
            sys.exit("--perspective is required with --repertoire")
        targets = [(args.perspective, Path(args.repertoire))]
    else:
        targets = [(p, path) for p, path in DEFAULT_REPS if path.exists()]

    t0 = time.time()
    print(f"Loading stats: {args.stats}", flush=True)
    edges = load_edges(Path(args.stats))
    print(f"  {len(edges):,} positions with edges ({time.time()-t0:.0f}s)", flush=True)

    DEFAULT_OUT_DIR.mkdir(parents=True, exist_ok=True)
    for persp, rep_path in targets:
        out = Path(args.output) if args.output else DEFAULT_OUT_DIR / f"chessbook_{persp}.pgn"
        print(f"\n{persp}: {rep_path.name}", flush=True)
        rep = load_rep(rep_path)
        label = f"Sharp {persp} repertoire ({rep_path.stem})"
        game, st = build_game(rep, edges, persp, args.min_reach,
                              args.max_branches, args.max_ply, label,
                              annots=annots_by_color.get(persp))
        tmp = out.with_suffix(".pgn.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            exporter = chess.pgn.FileExporter(f)
            game.accept(exporter)
        tmp.replace(out)
        n_nodes = validate(out)
        print(f"  our moves {st['our_moves']:,} | opponent branches {st['opp_branches']:,} "
              f"| lines {st['leaves']:,} | deepest ply {st['max_ply']}")
        print(f"  pruned: {st['pruned_reach']:,} below min-reach, {st['pruned_cap']:,} over "
              f"branch cap, {st['pruned_unanswered']:,} unanswered")
        print(f"  wrote {out}  ({out.stat().st_size/1e3:.0f} KB, {n_nodes:,} moves, "
              f"round-trip parse OK)")

    print(f"\nDone in {(time.time()-t0)/60:.1f} min. Import at chessbook.com -> "
          f"your repertoire -> Import -> PGN (one file per color).")


if __name__ == "__main__":
    main()
