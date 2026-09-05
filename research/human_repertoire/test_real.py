"""Small adapter/provenance contracts. Frozen-data checks run after extraction."""
from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest

import chess
from python.zobrist import zobrist_int64
from .extract_real import bounded_rows, extract, transition
from .io import ARTIFACTS, DEFAULT_CONFIG, output_path, sha256
from .model import Config, evaluate, graph_from_dict
from .oracle import path_evaluate


class AdapterTests(unittest.TestCase):
    def test_legal_moves_and_hash_contract(self):
        board = chess.Board()
        for san in ("e4", "e5", "Nf3", "Nc6", "Bb5", "a6"):
            direct = board.copy()
            direct.push_san(san)
            board, key = transition(board.fen(), san, zobrist_int64(direct))
            self.assertEqual(board.fen(), direct.fen())
            self.assertEqual(key, str(zobrist_int64(direct)))
        with self.assertRaises(ValueError):
            transition(chess.STARTING_FEN, "e5")
        with self.assertRaises(ValueError):
            transition(chess.STARTING_FEN, "e4", 0)

    def test_exact_terminal_chess_transition(self):
        board = chess.Board()
        for san in ("f3", "e5", "g4", "Qh4#"):
            board, _ = transition(board.fen(), san)
        self.assertTrue(board.is_checkmate())
        self.assertEqual(board.outcome().winner, chess.BLACK)

    def test_output_confinement_and_source_cap(self):
        with self.assertRaises(ValueError):
            output_path("../../escaped.json")
        with tempfile.TemporaryDirectory(dir=ARTIFACTS) as directory:
            path = Path(directory) / "too_big.parquet"
            path.write_bytes(b"not actually parquet")
            with self.assertRaisesRegex(ValueError, "byte cap"):
                bounded_rows(path, {"max_source_bytes": 1, "max_source_rows": 10})

    def test_frozen_identity_drift_fails_before_extracting(self):
        path = output_path("real/provenance.json")
        if not path.exists():
            self.skipTest("run bounded extraction first")
        # Same inputs, different recipe: no output is accepted or overwritten.
        config = json.loads(DEFAULT_CONFIG.read_text())["real"]
        config["wdl_prior_strength"] += 1
        before = sha256(path)
        with self.assertRaisesRegex(ValueError, "drift"):
            extract(config)
        self.assertEqual(sha256(path), before)

    def test_every_frozen_edge_and_all_exit_mass(self):
        path = output_path("real/graph.json")
        if not path.exists():
            self.skipTest("run bounded extraction first")
        data = json.loads(path.read_text())
        g = graph_from_dict(data["graph"])
        for key, n in g.nodes.items():
            for e in n.edges:
                _, child = transition(data["positions"][key]["fen"], e.move, int(e.child))
                self.assertEqual(child, e.child)
        for reentry in (True, False):
            for horizon in range(4):
                c = Config(horizon=horizon, recall=.65, reentry=reentry)
                m = evaluate(g, data["reference"], c)
                oracle = path_evaluate(g, data["reference"], c)
                self.assertAlmostEqual(m.points, oracle.points)
                self.assertAlmostEqual(m.win + m.draw + m.loss, 1)
                self.assertAlmostEqual(m.frontier_mass + m.horizon_mass + m.terminal_mass + m.repetition_mass, 1)
        manifest = json.loads(output_path("real/provenance.json").read_text())
        for relative, digest in manifest["snapshot_sha256"].items():
            self.assertEqual(sha256(output_path(relative)), digest)


if __name__ == "__main__":
    unittest.main()
