"""
Build the training pack: compact parquets the Trainer/Deviations sections run
from, so the app never loads the 13M-row repertoire or 23M-edge stats at
runtime.

The tree walk adapts export_chessbook.py's build_game: start position; at OUR
turns play best_move only; at OPPONENT turns branch over recorded replies
(share = total/Σtotal), pruning branches with path_reach < --min-reach, more
than --max-branches replies, no booked answer at the child, or ply >= --max-ply.
Reach is PATH-based (per move order), matching plan_consistency_report's
node_reach when summed per position over arrivals.

Outputs (per color, into <data>/pack/, atomic writes):
  <color>_tree.parquet   one row per tree node:
      node_id, parent_id (-1 root), move_san (move INTO the node; null at root),
      position_hash, epd, ply, is_our_turn, best_move, path_reach, reply_share
  <color>_cards.parquet  one row per distinct our-turn position with a booked move:
      position_hash, epd, best_move, reach (Σ path_reach over arrivals),
      memo_cost (from the rep parquet), canonical_node_id (arrival with max
      path_reach), min_ply
  dev_lookup_<color>.parquet  ALL in-book our-turn (position_hash, best_move)
      from the full repertoire (no reach cutoff) — the deviation scanner's
      probe table, detecting deviations deeper than the training region.
  pack_meta.json         provenance + counts + the rep sidecar's crush_weight.

Usage (defaults = canonical paths from trainer_app.config):
    set PYTHONPATH=<repo>\\python
    .venv\\Scripts\\python.exe -m trainer_app.build_training_pack [--data DIR]
        [--min-reach 0.0002] [--max-branches 12] [--max-ply 40]
        [--white-rep PATH] [--black-rep PATH] [--stats PATH]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import defaultdict, deque
from pathlib import Path

import chess
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # python/ for zobrist
from zobrist import zobrist_int64  # noqa: E402

from trainer_app.config import DEFAULTS, resolve_data_dir  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def load_rep(path: Path) -> tuple[dict[int, str | None], dict[int, float]]:
    """position_hash -> best_move (our-turn rows keyed by having one) and
    position_hash -> memo_cost."""
    df = pl.read_parquet(path, columns=["position_hash", "best_move", "memo_cost"])
    best = {h: b for h, b in zip(df["position_hash"], df["best_move"])}
    memo = {h: m for h, m in zip(df["position_hash"], df["memo_cost"])
            if m is not None}
    return best, memo


def load_edges(path: Path) -> dict[int, list[tuple[str, int, int]]]:
    """parent_hash -> [(san, child_hash, total)], most-played first
    (same as export_chessbook.load_edges)."""
    df = pl.read_parquet(path, columns=["parent_hash", "move_san", "child_hash", "total"])
    edges: dict[int, list[tuple[str, int, int]]] = defaultdict(list)
    for p, m, c, t in zip(df["parent_hash"], df["move_san"], df["child_hash"], df["total"]):
        edges[p].append((m, c, t))
    for v in edges.values():
        v.sort(key=lambda e: -e[2])
    return edges


def _line_to_sans(line: str) -> list[str]:
    """Split a move-list string into SAN tokens, dropping move numbers.
    Accepts '1. e4 c5', '1.e4 c5', 'e4 c5', '1... c5' forms."""
    out = []
    for tok in line.replace(".", ". ").split():
        tok = re.sub(r"^\d+\.*$", "", tok)     # bare '1.' / '1...' -> ''
        tok = re.sub(r"^\d+\.+", "", tok)      # '1.e4' -> 'e4'
        if tok:
            out.append(tok)
    return out


def load_overrides(data_dir: Path) -> dict[str, dict[int, tuple[str, str, str]]]:
    """Read forced_moves.json. Accepts either the data dir (the file is looked up
    inside it) or a direct path to the json — export_chessbook.py reads the same
    file so the PGN and the training pack cannot drift apart.

    Each entry {line, move[, note]} forces OUR move at the position that `line`
    reaches, overriding the repertoire's best_move there. Color is derived from
    whose turn it is at that position. Returns
    {color -> {position_hash -> (move_san, line, note)}}.

    Invalid entries are reported and skipped, never fatal — so a typo'd SAN
    yields a book WITHOUT that forced move rather than a failed build. Callers
    that depend on a specific count should check it (see the caller in
    export_chessbook.py, which reports how many loaded)."""
    path = data_dir / "forced_moves.json" if data_dir.is_dir() else data_dir
    out: dict[str, dict[int, tuple[str, str, str]]] = {"white": {}, "black": {}}
    if not path.exists():
        return out
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        print(f"  WARNING: could not read {path.name}: {e}")
        return out
    for e in entries:
        line, move = e.get("line", ""), e.get("move", "")
        note = e.get("note", "")
        board = chess.Board()
        try:
            for san in _line_to_sans(line):
                board.push_san(san)
            mv = board.parse_san(move)          # validates legality at this node
        except ValueError as err:
            print(f"  WARNING: skipping forced move '{line}' -> '{move}': {err}")
            continue
        color = "white" if board.turn == chess.WHITE else "black"
        out[color][zobrist_int64(board)] = (board.san(mv), line, note)
    return out


def _subtree_card_count(tree_rows: list[dict], forced_hash: int) -> int:
    """Distinct our-turn position hashes in the subtree rooted at forced_hash
    (a confirmation number: how many cards the forced branch contributes)."""
    kids: dict[int, list[int]] = defaultdict(list)
    for nid, r in enumerate(tree_rows):
        if r["parent_id"] >= 0:
            kids[r["parent_id"]].append(nid)
    starts = [nid for nid, r in enumerate(tree_rows)
              if r["position_hash"] == forced_hash]
    seen_nodes, seen_hashes = set(), set()
    dq = deque(starts)
    while dq:
        nid = dq.popleft()
        if nid in seen_nodes:
            continue
        seen_nodes.add(nid)
        r = tree_rows[nid]
        if r["is_our_turn"] and r["best_move"] is not None:
            seen_hashes.add(r["position_hash"])
        dq.extend(kids.get(nid, []))
    return len(seen_hashes)


def build_tree(best: dict[int, str | None], edges: dict, perspective: str,
               min_reach: float, max_branches: int, max_ply: int) -> list[dict]:
    """Walk the repertoire; return tree rows (see module docstring for schema)."""
    rows: list[dict] = []

    def add_node(parent_id: int, san: str | None, board: chess.Board,
                 ply: int, path_reach: float, reply_share: float | None) -> int:
        ph = zobrist_int64(board)
        our_turn = (board.turn == chess.WHITE) == (perspective == "white")
        rows.append({
            "node_id": len(rows), "parent_id": parent_id, "move_san": san,
            "position_hash": ph, "epd": board.epd(), "ply": ply,
            "is_our_turn": our_turn,
            "best_move": best.get(ph) if our_turn else None,
            "path_reach": path_reach, "reply_share": reply_share,
        })
        return len(rows) - 1

    start = chess.Board()
    root_id = add_node(-1, None, start, 0, 1.0, None)
    # Iterative DFS over (node_id, board, path_reach, ply); boards are copies.
    stack: list[tuple[int, chess.Board, float, int]] = [(root_id, start, 1.0, 0)]
    while stack:
        node_id, board, path_reach, ply = stack.pop()
        if ply >= max_ply or board.is_game_over():
            continue
        row = rows[node_id]
        if row["is_our_turn"]:
            bm = row["best_move"]
            if bm is None:
                continue                      # book truncates here
            try:
                mv = board.parse_san(bm)
            except ValueError:
                continue                      # stale SAN (shouldn't happen)
            b2 = board.copy(stack=False)
            b2.push(mv)
            cid = add_node(node_id, bm, b2, ply + 1, path_reach, None)
            stack.append((cid, b2, path_reach, ply + 1))
        else:
            replies = edges.get(row["position_hash"], [])
            tot = sum(t for _, _, t in replies)
            if tot == 0:
                continue
            kept = []
            for san, ch, t in replies:
                r2 = path_reach * (t / tot)
                if r2 < min_reach:
                    continue
                if best.get(ch) is None:      # no booked answer -> untrainable
                    continue
                kept.append((san, r2, t / tot))
            for san, r2, share in kept[:max_branches]:
                try:
                    mv = board.parse_san(san)
                except ValueError:
                    continue
                b2 = board.copy(stack=False)
                b2.push(mv)
                cid = add_node(node_id, san, b2, ply + 1, r2, share)
                stack.append((cid, b2, r2, ply + 1))
    return rows


def build_cards(tree_rows: list[dict], memo: dict[int, float]) -> list[dict]:
    """Collapse our-turn tree nodes with a booked move into per-position cards."""
    agg: dict[int, dict] = {}
    for r in tree_rows:
        if not r["is_our_turn"] or r["best_move"] is None:
            continue
        ph = r["position_hash"]
        c = agg.get(ph)
        if c is None:
            agg[ph] = {
                "position_hash": ph, "epd": r["epd"], "best_move": r["best_move"],
                "reach": r["path_reach"], "memo_cost": memo.get(ph),
                "canonical_node_id": r["node_id"], "min_ply": r["ply"],
                "_best_arrival": r["path_reach"],
            }
        else:
            c["reach"] += r["path_reach"]
            c["min_ply"] = min(c["min_ply"], r["ply"])
            if r["path_reach"] > c["_best_arrival"]:
                c["_best_arrival"] = r["path_reach"]
                c["canonical_node_id"] = r["node_id"]
    for c in agg.values():
        del c["_best_arrival"]
    return sorted(agg.values(), key=lambda c: -c["reach"])


def write_atomic(df: pl.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(".parquet.tmp")
    df.write_parquet(tmp)
    tmp.replace(path)


TREE_SCHEMA = {"node_id": pl.Int32, "parent_id": pl.Int32, "move_san": pl.Utf8,
               "position_hash": pl.Int64, "epd": pl.Utf8, "ply": pl.Int16,
               "is_our_turn": pl.Boolean, "best_move": pl.Utf8,
               "path_reach": pl.Float64, "reply_share": pl.Float64}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", default=None, help="App data dir (see trainer_app.config)")
    ap.add_argument("--white-rep", default=DEFAULTS["white_rep"])
    ap.add_argument("--black-rep", default=DEFAULTS["black_rep"])
    ap.add_argument("--stats", default=DEFAULTS["stats"])
    ap.add_argument("--min-reach", type=float, default=0.0002)
    ap.add_argument("--max-branches", type=int, default=12)
    ap.add_argument("--max-ply", type=int, default=40)
    args = ap.parse_args()

    data_dir = resolve_data_dir(args.data)
    pack_dir = data_dir / "pack"
    t0 = time.time()
    print(f"Loading stats: {args.stats}", flush=True)
    edges = load_edges(Path(args.stats))
    print(f"  {len(edges):,} positions with edges ({time.time()-t0:.0f}s)", flush=True)

    overrides = load_overrides(data_dir)
    n_ov = sum(len(v) for v in overrides.values())
    if n_ov:
        print(f"Loaded {n_ov} forced move(s) from forced_moves.json", flush=True)

    meta: dict = {"built": time.strftime("%Y-%m-%d %H:%M:%S"),
                  "stats": str(args.stats), "min_reach": args.min_reach,
                  "max_branches": args.max_branches, "max_ply": args.max_ply,
                  "forced_moves": [], "colors": {}}
    for color, rep_path in (("white", Path(args.white_rep)), ("black", Path(args.black_rep))):
        print(f"\n{color}: {rep_path.name}", flush=True)
        best, memo = load_rep(rep_path)

        # Apply forced overrides: replace OUR chosen move at the branch node. The
        # subtree below already carries the repertoire's own (backwards-induced,
        # self-consistent) best_moves, so the walk follows them automatically.
        ov = overrides.get(color, {})
        for h, (mv, line, note) in ov.items():
            prev = best.get(h)
            best[h] = mv
            print(f"  FORCED: {line} -> {mv} (repertoire had {prev})"
                  + (f"  [{note}]" if note else ""))

        tree_rows = build_tree(best, edges, color, args.min_reach,
                               args.max_branches, args.max_ply)
        for h, (mv, line, note) in ov.items():
            n_cards_sub = _subtree_card_count(tree_rows, h)
            meta["forced_moves"].append({"color": color, "line": line, "move": mv,
                                         "note": note, "subtree_cards": n_cards_sub})
            print(f"    -> {mv} subtree contributes {n_cards_sub} training card(s)")
        cards = build_cards(tree_rows, memo)
        write_atomic(pl.DataFrame(tree_rows, schema=TREE_SCHEMA),
                     pack_dir / f"{color}_tree.parquet")
        write_atomic(pl.DataFrame(cards), pack_dir / f"{color}_cards.parquet")

        # Full-book deviation lookup: every our-turn position with a booked move.
        # Forced overrides apply here too, so a game where you played the OLD
        # move at a forced node now counts as a deviation from the forced line.
        dev = (pl.read_parquet(rep_path, columns=["position_hash", "side_to_move",
                                                  "best_move"])
                 .filter((pl.col("side_to_move") == color) &
                         pl.col("best_move").is_not_null())
                 .select(["position_hash", "best_move"]))
        if ov:
            dev = dev.with_columns(
                pl.when(pl.col("position_hash").is_in(list(ov)))
                  .then(pl.col("position_hash").replace_strict(
                      {h: mv for h, (mv, _, _) in ov.items()}, default=None))
                  .otherwise(pl.col("best_move"))
                  .alias("best_move"))
        dev = dev.sort("position_hash")
        write_atomic(dev, pack_dir / f"dev_lookup_{color}.parquet")

        # Provenance: rep sidecar (crush_weight etc.) if present.
        sidecar = {}
        sc_path = Path(str(rep_path) + ".meta.json")
        if sc_path.exists():
            try:
                sidecar = json.loads(sc_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                pass
        meta["colors"][color] = {
            "rep": str(rep_path), "tree_nodes": len(tree_rows),
            "cards": len(cards), "dev_lookup_rows": dev.height,
            "rep_meta": {k: sidecar.get(k) for k in
                         ("crush_weight", "eval_weight", "built") if k in sidecar},
        }
        print(f"  tree {len(tree_rows):,} nodes | cards {len(cards):,} | "
              f"dev_lookup {dev.height:,} rows ({time.time()-t0:.0f}s)", flush=True)

    meta_tmp = pack_dir / "pack_meta.json.tmp"
    meta_tmp.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    meta_tmp.replace(pack_dir / "pack_meta.json")
    print(f"\nPack written to {pack_dir} in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
