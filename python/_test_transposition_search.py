"""Position scores, graph decisions and fresh child searches survive reordered proposals."""
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import chess
import chess.engine
import polars as pl

import budget_core as bc
import cloud_feedback as fb
import engine_candidates as ec
import transposition_search as ts
from hybrid_candidates import CLOUD
from stage3_backwards_induction import cp_to_expected_score
from zobrist import zobrist_int64


def edge(line, san, cp, engine, depth, nodes):
    board = chess.Board()
    for move in line.split():
        board.push_san(move)
    parent, epd = zobrist_int64(board), board.epd()
    board.push_san(san)
    return (parent, epd, san, zobrist_int64(board), cp, nodes, depth, engine, "test", "unknown")


first = edge("Nf3 d5", "g3", 12, CLOUD, 39, 100_000_000)
second = edge("g3 d5", "Nf3", -40, "Stockfish", 24, 10_000_000)
alternative = edge("Nf3 d5", "d4", 25, "Stockfish", 24, 10_000_000)
child = first[3]
frame = fb.normalize(pl.DataFrame([first, second, alternative], schema=ec.SCHEMA, orient="row"))
assert child == second[3] and child != alternative[3]
assert bc.proposal_cp(frame)[child] == -40
same_source = frame.with_columns(pl.lit(CLOUD).alias("engine"))
assert bc.proposal_cp(same_source)[child] == 12
same_depth = same_source.with_columns(pl.lit(24).alias("depth"))
assert bc.proposal_cp(same_depth)[child] == 12
tied = same_depth.with_columns(pl.lit(100).alias("nodes"))
assert bc.proposal_cp(tied)[child] == -14
assert bc.proposal_cp(tied.with_columns(pl.lit(None).alias("child_cp_white"))) == {}
assert ts.material_children(frame, 52).is_empty()


class Engine:
    calls = 0

    def configure(self, options):
        pass

    def analyse(self, board, limit, game):
        assert zobrist_int64(board) == game == child and limit.nodes == 30_000_000
        self.calls += 1
        return {"score": chess.engine.PovScore(chess.engine.Cp(-37), chess.BLACK),
                "nodes": limit.nodes, "depth": 31}

    def quit(self):
        pass


with TemporaryDirectory() as tmp:
    folder = Path(tmp)
    binary = folder / "engine"
    binary.write_bytes(b"fixture")
    engine = Engine()
    with patch.object(ts, "SF", binary), patch.object(chess.engine.SimpleEngine, "popen_uci", return_value=engine):
        resolved, report = ts.resolve(frame, folder / "cache")
        reverse, repeated = ts.resolve(frame.reverse(), folder / "cache")
        assert report == repeated and resolved.equals(reverse) and engine.calls == 1
        assert report["positions"][0]["low"] == -40 and report["positions"][0]["high"] == 12
        try:
            ts.resolve(frame, folder / "cache", nodes=1000)
            raise AssertionError("changed cache contract accepted")
        except ValueError as exc:
            assert "settings changed" in str(exc)
        wrong = frame.with_columns(pl.when(pl.col("move_san") == "g3").then(pl.lit("e4"))
                                   .otherwise(pl.col("move_san")).alias("move_san"))
        try:
            ts.resolve(wrong, folder / "cache")
            raise AssertionError("wrong child accepted")
        except ValueError as exc:
            assert "child mismatch" in str(exc)
    results = []
    for shuffled in (resolved, resolved.reverse(), resolved.sample(fraction=1, shuffle=True, seed=8)):
        path = folder / "overlay.parquet"
        shuffled.write_parquet(path)
        values = {child: cp_to_expected_score(-70)}
        moves, conflicts = bc.load_engine_candidates(path, values)
        parent = first[0]
        assert values[child] == cp_to_expected_score(37) and len(conflicts) == 1
        assert bc.verified_cp_overrides(shuffled)[child] == 37
        edges = {parent: [dict(move_san="d4", child_hash=alternative[3], total=100,
                               white_score_avg=.5, parent_epd=first[1])]}
        graph = bc.build_graph(edges, parent, True, rep_moves={parent:"d4"}, eval_ws=values,
                               engine_moves=moves, gated_reach=True, max_eval_loss=30)
        graph.our[parent].l_node = 0
        curves, _ = bc.build_curves(graph, bmax=1)
        book = bc.extract_book(graph, curves, 1)["booked"]
        assert parent in book
        results.append((list(moves.items()), conflicts, values, graph.our[parent].cands, book))
    assert results[0] == results[1] == results[2]
    # A position score can change across incoming routes. The verifier's edge-local
    # veto still applies at the parent whose restricted search lost over the limit.
    veto = resolved.with_columns(
        pl.when(pl.col("position_hash") == second[0]).then(90).alias("parent_cp_white"),
        pl.when(pl.col("position_hash") == second[0]).then(50).alias("verified_loss_cp"))
    veto.write_parquet(path)
    assert bc.proposal_cp(veto)[child] == 37
    assert bc.verified_exclusions(path, 30) == {second[0]: {"Nf3"}}
print("PASS deterministic scores, reordered graph and book, material child search and cache")
