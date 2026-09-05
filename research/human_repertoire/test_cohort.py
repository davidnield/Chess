import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from .cohort import CONFIG, COLUMNS, extract_one, rank_id, replay, select_reason
from .io import ARTIFACTS


def row(game_id="a", moves="1. e4 e5 2. Nf3 Nc6 3. Bb5 a6 4. Ba4"):
    return {"game_id": game_id, "white": "one", "black": "two", "white_elo": 1800,
            "black_elo": 2000, "utc_date": "2024-01-01", "time_control": "600+0",
            "termination": "Normal", "result": "1-0", "movetext": moves}


class CohortTests(unittest.TestCase):
    def setUp(self):
        self.c = json.loads(CONFIG.read_text())

    def test_individual_rating_and_gap_boundaries(self):
        self.assertIsNone(select_reason(row(), self.c))
        self.assertEqual(select_reason({**row(), "white_elo": 1799}, self.c), "rating")
        self.assertEqual(select_reason({**row(), "black_elo": 2001}, self.c), "rating_gap")
        self.assertEqual(select_reason({**row(), "result": "*"}, self.c), "unfinished_result")

    def test_selection_does_not_choose_winners_or_termination(self):
        for result in ("1-0", "0-1", "1/2-1/2"):
            for termination in ("Normal", "Time forfeit", "Abandoned"):
                self.assertIsNone(select_reason({**row(), "result": result, "termination": termination}, self.c))

    def test_horizon_and_observed_end(self):
        c = {**self.c, "horizon": 2}
        game = replay(row(), c)
        self.assertEqual(game["moves"], ["Bb5", "a6"])
        self.assertFalse(game["observed_end"])
        game = replay(row(moves="1.e4 e5 2.Nf3 Nc6 3.Bb5 a6 1-0"), c)
        self.assertTrue(game["observed_end"])
        self.assertTrue(game["reached_horizon"])
        # An invalid move outside the requested horizon cannot exclude a game.
        game = replay(row(moves="1.e4 e5 2.Nf3 Nc6 3.Bb5 a6 invalid"), c)
        self.assertFalse(game["observed_end"])

    def test_replay_prefix_and_annotations(self):
        game = replay(row(moves="1. e4! {comment} e5 2. Nf3 Nc6 3. Bb5?! a6 1-0"), self.c)
        self.assertEqual(game["moves"], ["Bb5", "a6"])
        with self.assertRaises(ValueError):
            replay(row(moves="1.e4 e5 2.Nf3 d6 3.Bb5"), self.c)

    def test_bounded_frame_and_rank_sample(self):
        c = {**self.c, "frame_rows": 5, "batch_rows": 2, "max_games_per_cohort": 2}
        rows = [row(str(i)) for i in range(8)]
        with tempfile.TemporaryDirectory(dir=ARTIFACTS) as directory:
            path = Path(directory) / "input.parquet"
            pq.write_table(pa.Table.from_pylist(rows), path)
            games, audit = extract_one(path, c)
            self.assertEqual(audit["counts"]["scanned"], 5)
            expected = sorted([r["game_id"] for r in rows[:5]], key=lambda k: rank_id(k, c["rank_seed"]))[:2]
            self.assertEqual([g["game_id"] for g in games], expected)
            with self.assertRaisesRegex(ValueError, "resource cap"):
                extract_one(path, {**c, "max_projected_row_group_bytes": 1})


if __name__ == "__main__":
    unittest.main()
