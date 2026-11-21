"""otb-ensemble-v2: the Maia support gate (David, 2026-11-20).

Maia is consulted only where effective online support is below ``maia_support_gate``; above it
the ensemble is the empirical estimate alone, and the frontier never sends that board to the
model. Without a gate the formula is v1's exactly.
"""
import math
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb
import polars as pl

from sweep import policy, spec


def v1(b, a, r, o, q):
    """The tuning plan's formula, written out (docs/repertoire-tuning-plan-2026-11-18.md)."""
    L = len(b)
    B, A, R, O = map(sum, (b, a, r, o))
    s = R + .10 * (A - R) + .01 * (B - A)
    out = []
    for bb, aa, rr, oo, qq in zip(b, a, r, o, q):
        broad = (bb + 1 / L) / (B + 1)
        backup = (aa - rr + 30 * broad) / (A - R + 30)
        online = (rr + 30 * backup) / (R + 30)
        out.append((oo + 10 * (s * online + 3 * qq) / (s + 3)) / (O + 10))
    return out, s


def online_only(b, a, r, o):
    L = len(b)
    B, A, R, O = map(sum, (b, a, r, o))
    out = []
    for bb, aa, rr, oo in zip(b, a, r, o):
        broad = (bb + 1 / L) / (B + 1)
        backup = (aa - rr + 30 * broad) / (A - R + 30)
        online = (rr + 30 * backup) / (R + 30)
        out.append((oo + 10 * online) / (O + 10))
    return out


MOVES = ["x", "y", "z"]
Q = [.5, .3, .2]
THIN = ([40, 10, 5], [6, 2, 1], [1, 0, 0], [2, 0, 1])          # support well below 300
THICK = ([9000, 3000, 800], [2500, 900, 300], [400, 150, 60], [5, 2, 0])   # support well above


class Formula(unittest.TestCase):
    def close(self, x, y):
        self.assertTrue(all(math.isclose(p, w, rel_tol=0, abs_tol=1e-15) for p, w in zip(x, y)), (x, y))

    def test_no_gate_is_v1(self):
        for counts in (THIN, THICK):
            want, _ = v1(*counts, Q)
            self.close(policy.ensemble(MOVES, *counts, Q), want)
            self.close(policy.ensemble(MOVES, *counts, Q, maia_support_gate=None), want)
            # A gate no board reaches is v1 too.
            self.close(policy.ensemble(MOVES, *counts, Q, maia_support_gate=1e18), want)

    def test_gate_drops_maia_at_high_support(self):
        _, s = v1(*THICK, Q)
        self.assertGreaterEqual(s, 300)
        self.assertFalse(policy.needs_maia(*THICK, maia_support_gate=300.))
        gated = policy.ensemble(MOVES, *THICK, None, maia_support_gate=300.)
        self.close(gated, online_only(*THICK))
        self.assertAlmostEqual(sum(gated), 1.)
        # A supplied Maia vector is ignored above the gate.
        self.assertEqual(policy.ensemble(MOVES, *THICK, Q, maia_support_gate=300.), gated)
        # Exactly at the gate the board is above it.
        self.assertFalse(policy.needs_maia(*THICK, maia_support_gate=s))
        self.assertTrue(policy.needs_maia(*THICK, maia_support_gate=s + 1e-9))

    def test_gate_keeps_maia_at_low_support(self):
        self.assertTrue(policy.needs_maia(*THIN, maia_support_gate=300.))
        want, _ = v1(*THIN, Q)
        self.close(policy.ensemble(MOVES, *THIN, Q, maia_support_gate=300.), want)
        with self.assertRaisesRegex(ValueError, "Maia is required"):
            policy.ensemble(MOVES, *THIN, None, maia_support_gate=300.)

    def test_unseen_board_always_needs_maia(self):
        zero = ([0, 0, 0], [0, 0, 0], [0, 0, 0], [0, 0, 0])
        self.assertTrue(policy.needs_maia(*zero, maia_support_gate=1e-9))
        self.assertEqual(policy.ensemble(MOVES, *zero, Q, maia_support_gate=300.), Q)

    def test_invalid_gate(self):
        for g in (0., -1., float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                policy.ensemble(MOVES, *THICK, Q, maia_support_gate=g)

    def test_recipe_is_v2(self):
        self.assertEqual(spec.POLICY["name"], "otb-ensemble-v2")
        self.assertEqual(spec.POLICY["maia_support_gate"], 300.)


class FrontierSkipsGatedBoards(unittest.TestCase):
    """The start position has support 3.8 in the execution fixture (adult 20, broad 200)."""

    def run_frontier(self, gate):
        from _test_sweep_execution import SelectivePolicy, Model, keys, board
        from build_budget_books import collect_subgraph_edges_rs
        from sweep.frontier import Counts, FrontierOverlay
        with tempfile.TemporaryDirectory() as td, patch.dict(spec.POLICY, maia_support_gate=gate):
            root = Path(td)
            sources, aux, inputs = SelectivePolicy.prepare(None, root)
            k, model = keys(), Model()
            provider = FrontierOverlay(colour="black", keys=k, counts=Counts(sources, aux), predict=model,
                                       cache=root / "answers.sqlite", recipe=spec.POLICY,
                                       input_artifacts=inputs, pool=sources["adult"], aux=aux)
            with duckdb.connect(config={"threads": 1}) as db:
                edges = collect_subgraph_edges_rs(db, sources["adult"], k.board(board()), False,
                                                  .00001, 2, 0., 1, reply_overlay=provider, keys=k)
            stats = dict(provider.stats)
            provider.close()
            start = edges.filter(pl.col("parent_hash") == k.board(board()))
            return model.seen, stats, start

    def test_gate_skips_inference_and_uses_counts(self):
        import chess
        start_fen = chess.Board().fen()
        seen, stats, start = self.run_frontier(300.)
        self.assertIn(start_fen, seen)                  # 3.8 < 300: the model is asked
        self.assertEqual(stats.get("maia_skipped", 0), 0)
        seen, stats, start = self.run_frontier(1.)
        self.assertNotIn(start_fen, seen)               # 3.8 >= 1: never sent to the model
        self.assertGreaterEqual(stats["maia_skipped"], 1)
        # The start position's replies are then the counts-only estimate (20 legal moves).
        self.assertEqual(start.height, 20)
        shares = dict(zip(start["move_san"], start["reach_share"]))
        self.assertGreater(shares["e4"], shares["Nf3"])
        self.assertAlmostEqual(shares["e4"], shares["d4"])


if __name__ == "__main__":
    unittest.main()
