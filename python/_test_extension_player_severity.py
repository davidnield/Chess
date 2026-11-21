"""Player severity for extensions (2026-11-21): max(engine gaps, our players' loss), never lower.

David's concern: players who reach a mid-sequence position mostly chose the tactic and know its
next move; we arrive by book. So the player term may only ADD extensions. Covered here: the loss
is a lower bound (unevaluated mass counts as no loss), combining never lowers the engine
severity, engine mode is an exact no-op (identity, state and report), thin data falls back to
the engine, ranked moves keep their engine scores, and the book reader matches both key halves
and only the own population's slices.
"""
import random
import tempfile
import unittest
from pathlib import Path
import chess
import pyarrow as pa
import pyarrow.parquet as pq
from sweep.continuations import Search, token
from sweep.extensions import player_loss, combined_severity, severity
from sweep.own_moves import BookOwnMoves, population
from sweep.quality import score
from sweep.spec import digest
from zobrist128 import zobrist128

CONTRACT = dict(absolute_floor=.0, relative_limit=1.)


class Kernel(unittest.TestCase):
    def test_loss_is_a_lower_bound(self):
        r = player_loss(.8, [(60, .8), (30, .5), (10, None)])
        self.assertAlmostEqual(r["loss"], 30 * .3 / 100)      # unevaluated 10 games: no loss, still counted
        self.assertAlmostEqual(r["evaluated_share"], .9)
        self.assertEqual(player_loss(.5, [(5, .9)])["loss"], 0.)  # a lookup above the engine best is no gain
        for bad in ([(1, 1.2)], [(-1, .5)], []):
            with self.assertRaises(ValueError):
                player_loss(.5, bad)

    def test_combining_never_lowers_the_engine(self):
        rng = random.Random(7)
        for _ in range(2000):
            e, p = rng.random(), rng.choice([None, rng.random()])
            c = combined_severity(e, p, "engine-or-player")
            self.assertGreaterEqual(c, e)
            self.assertEqual(combined_severity(e, p, "engine"), e)
        with self.assertRaises(ValueError):
            combined_severity(.1, .2, "player-only")


def fixture(players, engine_gap_cp, lookup):
    """One our-turn position at the start: engine top three, our players' counts, lookup cps."""
    moves = ["e2e4", "d2d4", "g1f3"]

    def analyse(b, required):
        return dict(top=[dict(uci=moves[0], cp_white=0), dict(uci=moves[1], cp_white=-engine_gap_cp),
                         dict(uci=moves[2], cp_white=-engine_gap_cp)],
                    independent=True, nodes=20_000_000, receipt="fixture")

    def reply(b):
        return dict(moves={m.uci(): 0. for m in b.legal_moves}, stop=1.)

    def own(b):
        return dict(games=sum(players.values()), moves=dict(players))

    def child_score(b):       # the child board carries exactly our move on its stack
        return lookup.get(b.peek().uci())
    return analyse, reply, own, child_score


class SearchSeverity(unittest.TestCase):
    def run_one(self, mode, players, gap_cp, lookup, min_games=20):
        analyse, reply, own, child = fixture(players, gap_cp, lookup)
        kw = dict(base={}, white=True, analyse=analyse, reply=reply, contract=CONTRACT, threshold=.001)
        if mode != "engine":
            kw.update(severity_mode=mode, own=own, own_contract=dict(fixture=1), child_score=child,
                      player_min_games=min_games)
        s = Search(**kw)
        s.run(3)
        return s, s.state["risks"][token(chess.Board())]

    def test_engine_mode_is_an_exact_no_op(self):
        analyse, reply, own, child = fixture({"e2e4": 30}, 100, {})
        s = Search(base={}, white=True, analyse=analyse, reply=reply, contract=CONTRACT, threshold=.001)
        old = digest(dict(base={}, white=True, contract=CONTRACT, threshold=.001, allowed=None,
                          root_fen=chess.STARTING_FEN, metric="remaining-maximum",
                          algorithm="prefix-union-greedy-v1"))
        self.assertEqual(s.identity, old)                      # existing checkpoints stay valid
        report = s.run(3)
        self.assertNotIn("severity_parts", s.state)
        self.assertNotIn("player_severity", report)
        with self.assertRaisesRegex(ValueError, "no player inputs"):
            Search(base={}, white=True, analyse=analyse, reply=reply, contract=CONTRACT, own=own)
        with self.assertRaisesRegex(ValueError, "needs our players"):
            Search(base={}, white=True, analyse=analyse, reply=reply, contract=CONTRACT,
                   severity_mode="engine-or-player")

    def test_player_raises_an_engine_quiet_position(self):
        # three engine moves equal (engine severity 0); our players mostly play h2h4, -300 by lookup
        s, risk = self.run_one("engine-or-player", {"e2e4": 20, "h2h4": 80}, 0, {"h2h4": -300})
        expect = .8 * (score(0) - score(-300))
        self.assertAlmostEqual(risk, expect)
        part = s.state["severity_parts"][token(chess.Board())]
        self.assertEqual(part["engine"], 0.)
        self.assertEqual(s.player_summary()["player_raised"], 1)

    def test_player_never_lowers_a_critical_position(self):
        # one good move (engine gap 300 cp); our players all find it: the player loss is 0
        s, risk = self.run_one("engine-or-player", {"e2e4": 100}, 300, {})
        engine = severity([score(0), score(-300), score(-300)], 20)["severity"]
        self.assertAlmostEqual(risk, engine)
        self.assertGreater(risk, 0.)

    def test_ranked_moves_keep_engine_scores_and_thin_data_falls_back(self):
        # d2d4 is engine-ranked at -100; a stale lookup claiming -900 must not be used
        _, risk = self.run_one("engine-or-player", {"d2d4": 50}, 100, {"d2d4": -900})
        self.assertAlmostEqual(risk, max(severity([score(0), score(-100), score(-100)], 20)["severity"],
                                         score(0) - score(-100)))
        s, risk = self.run_one("engine-or-player", {"h2h4": 19}, 0, {"h2h4": -900})
        self.assertEqual(risk, 0.)                               # 19 < 20 games: engine only
        self.assertIsNone(s.state["severity_parts"][token(chess.Board())]["player"])

    def test_modes_do_not_share_checkpoints(self):
        analyse, reply, own, child = fixture({"e2e4": 30}, 100, {})
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "c.json"
            Search(base={}, white=True, analyse=analyse, reply=reply, contract=CONTRACT, checkpoint=path).run(2)
            with self.assertRaisesRegex(ValueError, "recipe changed"):
                Search(base={}, white=True, analyse=analyse, reply=reply, contract=CONTRACT, checkpoint=path,
                       severity_mode="engine-or-player", own=own, own_contract={}, child_score=child,
                       player_min_games=20)


class BookReader(unittest.TestCase):
    def test_full_key_and_population_slices(self):
        board = chess.Board()
        lo, hi = zobrist128(board)
        bucket = lo % 512
        pop = population("historical-1000-1799")
        with tempfile.TemporaryDirectory() as td:
            book = Path(td)
            (book / "_book.meta.json").write_text("{}", encoding="utf8")
            (book / "_BOOK.DONE").write_text("", encoding="utf8")

            def write(event, band, rows):
                d = book / "ps" / f"event={event}" / f"elo_band={band}"
                d.mkdir(parents=True)
                t = pa.table({"parent_hash": [r[0] for r in rows], "parent_hash_hi": [r[1] for r in rows],
                              "move_san": [r[2] for r in rows], "total": [r[3] for r in rows]},
                             schema=pa.schema([("parent_hash", pa.int64()), ("parent_hash_hi", pa.int64()),
                                               ("move_san", pa.string()), ("total", pa.int64())]))
                pq.write_table(t, d / f"bkt{bucket:03d}.parquet", row_group_size=2)
            write("Blitz", 1000, [(lo - 5, 1, "e4", 7), (lo, hi, "e4", 10), (lo, hi + 1, "d4", 99),  # twin
                                  (lo, hi, "Nf3", 3), (lo + 9, 2, "e4", 8)])
            write("Rapid", 1600, [(lo, hi, "e4", 5)])
            write("Blitz", 1800, [(lo, hi, "e4", 1000)])          # outside the own population
            write("Bullet", 1000, [(lo, hi, "e4", 1000)])         # not a population speed
            r = BookOwnMoves(book, own_policy="historical-1000-1799")
            self.assertEqual(r.moves(board), dict(games=18, moves={"e2e4": 15, "g1f3": 3}))
            self.assertEqual(r.contract()["elo_bands"], pop["elo_bands"])
            with self.assertRaisesRegex(ValueError, "differs"):
                BookOwnMoves(book, own_policy="historical-1000-1799", expected_meta_sha="0" * 64)
            (book / "_BOOK.DONE").unlink()
            with self.assertRaisesRegex(ValueError, "not a finished"):
                BookOwnMoves(book, own_policy="historical-1000-1799")


if __name__ == "__main__":
    unittest.main()
