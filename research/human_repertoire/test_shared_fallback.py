import json
from math import log
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import chess
import pyarrow as pa
import pyarrow.parquet as pq

from . import date_cohort, shared_fallback
from .cohort import replay, rank_id
from .date_cohort import choose_frames, validate_frames
from .io import ARTIFACTS, fingerprint
from .shared_fallback import distribution, fit, move_token, spec, score, choose, paired_bootstrap
from .test_cohort import row


class SharedFallbackTests(unittest.TestCase):
    def setUp(self):
        self.c = json.loads(date_cohort.CONFIG.read_text())

    def game(self, moves="1.e4 e5 2.Nf3 Nc6 3.Bb5 a6", game_id="one"):
        return {**replay(row(game_id, moves), self.c), "event": "Rapid"}

    def test_hand_calculated_prior_and_posterior(self):
        # Token weights 4 and 1; alpha=2, observed counts 3 and 0.
        args = (["a", "b"], ["ta", "tb"], {"a": 3}, {"ta": 3}, spec("token", 2, 1))
        for actual, expected in zip(distribution(*args), [.92, .08]):
            self.assertAlmostEqual(actual, expected)
        self.assertEqual(distribution(["a", "b"], ["ta", "tb"], {}, {"ta": 3}, spec("token", 2, 1)), [.8, .2])
        # Two legal moves with the same token each retain their own probability.
        self.assertEqual(distribution(["a", "b", "c"], ["t", "t", "u"], {}, {"t": 3}, spec("token", 1, 1)), [4/9, 4/9, 1/9])
        with self.assertRaisesRegex(ValueError, "legal support"):
            distribution(["a"], ["t"], {"illegal": 1}, {}, spec("uniform"))

    def test_token_color_symmetry_and_capture(self):
        board = chess.Board()
        move = board.parse_san("Nf3")
        mirrored = board.mirror()
        reverse = chess.Move(chess.square_mirror(move.from_square), chess.square_mirror(move.to_square))
        self.assertEqual(move_token(board, move), move_token(mirrored, reverse))
        board = chess.Board("4k3/8/8/3p4/4P3/8/8/4K3 w - - 0 1")
        self.assertEqual(move_token(board, board.parse_san("exd5")).split(":")[2], "1")
        board = chess.Board("4k3/P7/8/8/8/8/8/4K3 w - - 0 1")
        self.assertNotEqual(move_token(board, board.parse_san("a8=Q+")), move_token(board, board.parse_san("a8=N")))

    def test_fit_ignores_outcome_and_scoring_does_not_mutate_training(self):
        game = self.game()
        fitted = fit([game])
        changed = {**game, "wdl": [0, 0, 1], "result": "0-1", "white_elo": 2400}
        self.assertEqual(fit([changed]), fitted)
        before = json.dumps(fitted, sort_keys=True)
        models = {"baseline": spec("uniform"), "shared": spec("token", 1, .5)}
        summary, _ = score(fitted, [self.game("1.e4 e5 2.Nf3 Nc6 3.Bc4 Bc5")], models, self.c)
        self.assertEqual(json.dumps(fitted, sort_keys=True), before)
        self.assertEqual(summary["baseline"]["pooled"]["unseen_position"], .5)
        self.assertGreater(summary["shared"]["support:unseen"]["log_loss"], 0)

    def test_brier_and_logloss_match_independent_legal_enumeration(self):
        game = self.game("1.e4 e5 2.Nf3 Nc6 3.Bb5")
        fitted = fit([game, game])
        models = {"m2": spec("uniform")}
        summary, _ = score(fitted, [game], models, self.c)
        board = chess.Board(game["positions"][0]["fen"])
        n = board.legal_moves.count()
        actual = (2+1/n)/3
        expected_brier = (1-actual)**2 + (n-1)*(1/n/3)**2
        self.assertAlmostEqual(summary["m2"]["pooled"]["log_loss"], -log(actual))
        self.assertAlmostEqual(summary["m2"]["pooled"]["brier"], expected_brier)

    def test_selection_uses_tuning_loss_and_declared_tiebreak(self):
        models = {"high": spec("token", 5, 1), "low": spec("token", .1, .5), "uniform": spec("uniform")}
        summary = {name: {"pooled": {"log_loss": 1}} for name in models}
        self.assertEqual(choose(models, summary, "token"), "low")
        summary["high"]["pooled"]["log_loss"] = .99
        self.assertEqual(choose(models, summary, "token"), "high")

    def test_component_bootstrap_preserves_pairs_and_decision_weight(self):
        rows = [{"white": "a", "black": "b", "models": {"x": {"log_loss": 2, "decisions": 1}, "y": {"log_loss": 1, "decisions": 1}}},
                {"white": "b", "black": "c", "models": {"x": {"log_loss": 3, "decisions": 3}, "y": {"log_loss": 6, "decisions": 3}}}]
        result = paired_bootstrap(rows, "x", "y", 100, 1)
        self.assertEqual(result["components"], 1)
        self.assertEqual(result["estimate"], -.5)
        self.assertEqual(result["percentile_95"], [-.5, -.5])

    def test_final_test_checks_frozen_identity_before_opening_test(self):
        with tempfile.TemporaryDirectory(dir=ARTIFACTS) as directory:
            path = Path(directory)/"frozen.json"
            path.write_text(json.dumps({"identity": {"old": True}}))
            with patch.object(shared_fallback, "output_path", return_value=path), patch.object(shared_fallback, "identity", return_value={"new": True}), patch.object(shared_fallback, "inputs") as reader:
                with self.assertRaisesRegex(ValueError, "identity drift"):
                    shared_fallback.final_test(self.c)
                reader.assert_not_called()


class DateFrameTests(unittest.TestCase):
    def setUp(self):
        self.c = json.loads(date_cohort.CONFIG.read_text())

    def group(self, index, n=100):
        return {"path": "source", "row_group": index, "row_group_rows": n, "projected_uncompressed_bytes": 100}

    def test_spread_selection_and_limits(self):
        selected = choose_frames([self.group(i) for i in range(9)], 3, 10)
        self.assertEqual([f["row_group"] for f in selected], [1, 4, 7])
        self.assertEqual(validate_frames(selected, self.c), 30)
        small = choose_frames([self.group(0)], 3, 10)
        self.assertEqual([f["start"] for f in small], [11, 45, 78])
        self.assertEqual(validate_frames(small, self.c), 88)
        with self.assertRaisesRegex(ValueError, "overlapping"):
            validate_frames([small[0], small[0]], self.c)
        with self.assertRaisesRegex(ValueError, "bounds"):
            validate_frames([{**small[0], "rows": 101}], self.c)
        with self.assertRaisesRegex(ValueError, "decoded-row"):
            validate_frames(small, {**self.c, "max_decoded_rows": 50})

    def test_window_extraction_across_batch_edges_and_frozen_reuse(self):
        with tempfile.TemporaryDirectory(dir=ARTIFACTS) as directory:
            root = Path(directory)
            source = root/"source.parquet"
            records = [row(str(i)) for i in range(20)]
            pq.write_table(pa.Table.from_pylist(records), source)
            c = {**self.c, "months": {"train": 1}, "batch_rows": 4, "games_per_frame": 2}
            frames = [{**self.group(0, 20), "path": str(source), "start": start,
                       "rows": 5, "frame_id": str(i), "split": "train", "event": "Rapid"} for i, start in enumerate((3, 13))]
            def path(relative):
                p = root/relative
                p.parent.mkdir(parents=True, exist_ok=True)
                return p
            def write(relative, data):
                p = path(relative)
                p.write_text(json.dumps(data))
                return p
            write("m3/frames.json", {"config": c, "frames": frames, "inventory": [fingerprint(source)]})
            with patch.object(date_cohort, "output_path", side_effect=path), patch.object(date_cohort, "write_json", side_effect=write):
                date_cohort.extract(c)
                games = json.loads(path("m3/train_games.json").read_text())
                expected = []
                for start in (3, 13):
                    expected.extend(sorted(map(str, range(start, start+5)), key=lambda k: rank_id(k, c["rank_seed"]))[:2])
                self.assertEqual([g["game_id"] for g in games], expected)
                date_cohort.extract(c)
                path("m3/train_games.json").write_text("[]")
                with self.assertRaisesRegex(ValueError, "identity drift"):
                    date_cohort.extract(c)


if __name__ == "__main__":
    unittest.main()
