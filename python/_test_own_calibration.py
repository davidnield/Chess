"""sweep.own_calibration: panel replay, count vectors, scoring against the native own policy,
the pre-registered selection, confirmation and the contamination flags."""
import random
import unittest

import chess
import numpy as np
import pandas as pd

from consumer_keys import stored_key
from sweep import own_calibration as oc
from zobrist128 import zobrist128


def random_game(rng, plies):
    b, sans = chess.Board(), []
    for _ in range(plies):
        moves = sorted(b.legal_moves, key=lambda m: m.uci())
        if not moves:
            break
        # a few popular first moves, so boards repeat across games
        m = moves[rng.randrange(len(moves))] if b.ply() >= 2 else moves[rng.randrange(min(3, len(moves)))]
        sans.append(b.san(m))
        b.push(m)
    text = " ".join((f"{i // 2 + 1}. " if i % 2 == 0 else "") + s for i, s in enumerate(sans))
    return text, sans


def records_for(board, counts):
    out = []
    for san, total in counts.items():
        child = board.copy(stack=False)
        child.push_san(san)
        out.append(dict(parent_hash=stored_key(board, True), move_san=san,
                        child_hash=stored_key(child, True), total=total))
    return out


class OwnCalibration(unittest.TestCase):
    def test_panel_keeps_only_in_band_movers_to_the_cap(self):
        games = [dict(game_id="g1", split="development", movetext="1. e4 e5 2. Nf3 Nc6 3. Bb5 a6 4. Ba4",
                      white_elo=1500, black_elo=2000, event="Rapid", opening_family="Ruy Lopez")]
        rows, bad = oc.panel_rows(games, cap=6, lo=1000, hi=1800)
        self.assertEqual(bad, 0)
        self.assertEqual([r["ply"] for r in rows], [1, 3, 5])
        self.assertEqual({r["colour"] for r in rows}, {"white"})
        b = chess.Board()
        self.assertEqual((rows[0]["lo"], rows[0]["hi"]), zobrist128(b))
        self.assertEqual(rows[0]["move"], "e2e4")
        b.push_san("e4"); b.push_san("e5")
        self.assertEqual(rows[1]["fen"], oc.fen4(b))
        bad_game = dict(games[0], game_id="g2", movetext="1. e4 e5 2. Qxf7")
        rows, bad = oc.panel_rows([bad_game], cap=30, lo=1000, hi=1800)
        self.assertEqual((len(rows), bad), (1, 1))     # rows before the illegal token survive

    def test_count_vectors_are_checked(self):
        start = chess.Board()
        boards = pd.DataFrame(dict(board=[0], lo=[0], hi=[0], fen=[oc.fen4(start)]))
        got = oc.count_stage(boards, lambda bs: [records_for(bs[0], {"e4": 7, "d4": 3})])
        legal, counts = got.legal[0], got.counts[0]
        self.assertEqual(sum(counts), 10)
        self.assertEqual(counts[legal.index("e2e4")], 7)
        wrong = records_for(start, {"e4": 7})
        wrong[0]["child_hash"] = dict(wrong[0]["child_hash"], hi=wrong[0]["child_hash"]["hi"] ^ 1)
        with self.assertRaisesRegex(ValueError, "child"):
            oc.count_stage(boards, lambda bs: [wrong])

    def test_score_select_confirm_contamination(self):
        rng = random.Random(3)
        games = []
        for i in range(160):
            text, sans = random_game(rng, 14)
            games.append(dict(game_id=f"g{i}", split=oc.SPLITS[i % 2], movetext=text,
                              white_elo=1000 + rng.randrange(800), black_elo=1000 + rng.randrange(800),
                              event="Blitz", opening_family=f"fam{sans[0]}"))
        # one position by a popular route (1.e4 e5 2.Nf3 Nc6) and by a rare one (1.Nf3 Nc6 2.e4 e5)
        for i in range(40):
            order = "1. e4 e5 2. Nf3 Nc6" if i % 2 else "1. Nf3 Nc6 2. e4 e5"
            games.append(dict(game_id=f"t{i}", split=oc.SPLITS[i % 4 // 2], event="Rapid",
                              movetext=order + (" 3. Bb5 a6" if i % 3 else " 3. Bc4 Bc5"),
                              white_elo=1500, black_elo=2100, opening_family="Open Game"))
        rows, _ = oc.panel_rows(games, cap=12, lo=1000, hi=1800)
        panel = pd.DataFrame(rows)
        boards = oc.boards_of(panel)
        special = {}
        for line, counts in (("", {"e4": 50, "d4": 30, "Nf3": 2}), ("e4 e5", {"Nf3": 30}),
                             ("e4 e5 Nf3 Nc6", {"Bb5": 20, "Bc4": 20})):
            b = chess.Board()
            for san in line.split():
                b.push_san(san)
            special[oc.fen4(b)] = counts
        # weak counts: those boards as given, every other board 25 games on its first legal move,
        # so most random moves are rare and movers go off book
        def fetch(bs):
            out = []
            for b in bs:
                if oc.fen4(b) in special:
                    out.append(records_for(b, special[oc.fen4(b)]))
                else:
                    first = min(b.legal_moves, key=lambda m: m.uci())
                    out.append(records_for(b, {b.san(first): 25}))
            return out
        boards = oc.count_stage(boards.assign(fen=boards.fen), fetch)
        # planted: the 1450/1800 model puts 0.9 on the move the panel plays most at each board
        top = panel.groupby(["lo", "hi"]).move.agg(lambda s: s.value_counts().index[0])
        raw = {}
        for pair in oc.PAIRS:
            vecs = []
            for lo, hi, legal in zip(boards.lo, boards.hi, boards.legal):
                L = len(legal)
                if pair == (1450, 1800) and L > 1:
                    v = [0.1 / (L - 1)] * L
                    v[legal.index(top[(lo, hi)])] = 0.9
                else:
                    v = [1.0 / L] * L
                vecs.append(v)
            raw[pair] = vecs
        grid, nll, n_row, c_row, bi, mi = oc.score(panel, boards, raw, check_native=400)
        table = oc.summarise(panel, grid, nll, n_row)
        self.assertEqual(len(table), 108)
        result = oc.select_and_confirm(panel, grid, nll, n_row, table)
        self.assertEqual(result["recipe"]["ratings"], [1450, 1800])
        best1700 = result["chosen_minus_alternative"]["best 1700"]
        self.assertLess(best1700["mean"], 0)          # chosen fits held-out moves better
        self.assertLess(best1700["ci95"][1], 0)
        cont = oc.contamination(panel, boards, n_row, c_row, bi, mi, nll[:, result["chosen"]])
        self.assertEqual(cont["rows"], len(panel))
        # a mover's first own move is never off book; off-book flags follow any earlier rare move
        p = panel.assign(n=n_row, c=c_row).sort_values(["game_id", "colour", "ply"])
        first = p.groupby(["game_id", "colour"]).head(1)
        self.assertGreater(cont["off_book_rows"], 0)
        self.assertLessEqual(cont["off_book_rows"], len(panel) - len(first))
        share = np.where(p.n > 0, p.c / np.maximum(p.n, 1), 0) < oc.OFF_BOOK_SHARE
        expect = p.assign(r=share.astype(int)).groupby(["game_id", "colour"]).r.transform(
            lambda s: s.shift(fill_value=0).cummax()).sum()
        self.assertEqual(cont["off_book_rows"], expect)
        self.assertGreater(cont["matched_positions"], 0)
        self.assertIn("matched_off_minus_on", cont["modal_hit"])

    def test_vectorised_own_probability_matches_native(self):
        from book_builder_rs import own_policy
        counts = [0., 3., 1200., 7.]
        q = [0.1, 0.2, 0.3, 0.4]
        n = np.array([sum(counts)] * 4)
        for cap in (None, 100., 1000.):
            native = own_policy(counts, q, 30., cap)
            mine = oc.own_probability(np.array(counts), n, np.array(q), 30., cap)
            self.assertTrue(np.allclose(native, mine, rtol=1e-13, atol=0))


if __name__ == "__main__":
    unittest.main()
