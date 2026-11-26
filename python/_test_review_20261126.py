"""Decisions from the 26 November review (docs/claude-review-of-chatgpt-2026-11-26.md, "David's decisions").

- otb-ensemble-v3: an explicit OTB/online population mix; v2 stays bit-identical when the mix is absent.
- own-policy observation cap: the model keeps a share at popular positions; uncapped stays bit-identical.
- referee (F1): one fixed referee binding scores every arm's leaves, never the arm's construction lookup.
"""
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import book_builder_rs as rs
from eval_lookup128 import ROW, SCHEMA
from month_archive import sha, write_json
from sweep import policy
from sweep.model_contract import calibration_grid, validate
from sweep.spec import POLICIES, POLICY, POLICY_V3
from sweep.stages import atomic_json, file_sha
from _test_sweep_execution import board, keys
from _test_model_contract import Fixed, recipe
from sweep.human_providers import SharedPolicies


def random_counts(rng, n):
    r = rng.integers(0, 10, n); a = r + rng.integers(0, 40, n); b = a + rng.integers(0, 100, n)
    return b, a, r


class OpponentMix(unittest.TestCase):
    def test_recipes(self):
        self.assertEqual(set(POLICIES), {"otb-ensemble-v2", "otb-ensemble-v3"})
        self.assertNotIn("otb_mix", POLICY)
        self.assertEqual({k: v for k, v in POLICY_V3.items() if k not in ("name", "otb_mix")},
                         {k: v for k, v in POLICY.items() if k != "name"})
        self.assertEqual(POLICY_V3["otb_mix"], .5)

    def test_native_equals_reference_and_v2_unchanged(self):
        rng = np.random.default_rng(26)
        for gate in (None, 300.):
            for mix in (None, 0., .3, .5, 1.):
                for _ in range(40):
                    b, a, r = random_counts(rng, 20)
                    o = rng.integers(0, 2000, 20)
                    q = rng.dirichlet(np.ones(20))
                    args = [list(range(20)), b.tolist(), a.tolist(), r.tolist(), o.tolist(), q.tolist()]
                    kw = dict(maia_support_gate=gate, otb_mix=mix)
                    native = policy.ensemble(*args, **kw)
                    np.testing.assert_allclose(native, policy.ensemble_reference(*args, **kw), rtol=0, atol=1e-14)
                    if mix is None:
                        # v2 bit-identical: the default call and an explicit None agree exactly
                        self.assertEqual(native, policy.ensemble(*args, maia_support_gate=gate))

    def test_mix_is_a_population_weight(self):
        legal = ["a", "b"]
        b, a, r = [9000, 1000], [5000, 500], [2000, 200]
        o = [100, 900]
        q = [.5, .5]
        v2 = policy.ensemble(legal, b, a, r, o, q, maia_support_gate=300.)
        online = policy.ensemble(legal, b, a, r, [0, 0], q, maia_support_gate=300.)
        v3 = policy.ensemble(legal, b, a, r, o, q, maia_support_gate=300., otb_mix=.5)
        np.testing.assert_allclose(v3, [.5 * x + .5 * y for x, y in zip(v2, online)], atol=1e-15)
        # at a main-line board (O = 1,000 >> 10) v2 is ~99% OTB; v3 keeps half the weight online
        self.assertGreater(v2[1], .89)
        self.assertLess(abs(v3[1] - (.5 * v2[1] + .5 * online[1])), 1e-15)
        with self.assertRaises(ValueError):
            policy.ensemble(legal, b, a, r, o, q, otb_mix=1.5)
        with self.assertRaises(ValueError):
            policy.ensemble_reference(legal, b, a, r, o, q, otb_mix=-.1)


class OwnCap(unittest.TestCase):
    def test_uncapped_bit_identical_and_cap_formula(self):
        rng = np.random.default_rng(7)
        for _ in range(100):
            c = rng.integers(0, 5000, 12).astype(float)
            q = rng.dirichlet(np.ones(12)).tolist()
            for k in (10., 30., 100.):
                self.assertEqual(rs.own_policy(c.tolist(), q, k), rs.own_policy(c.tolist(), q, k, None))
                for cap in (100., 1000.):
                    n = c.sum()
                    s = min(1., cap / n) if n else 1.
                    want = [(ci * s + k * qi) / (n * s + k) for ci, qi in zip(c, q)]
                    np.testing.assert_allclose(rs.own_policy(c.tolist(), q, k, cap), want, rtol=0, atol=1e-14)
        with self.assertRaises(ValueError):
            rs.own_policy([1., 0.], [.5, .5], 30., 0.)

    def test_contract_grid(self):
        grid = calibration_grid()
        self.assertEqual(len(grid), 108)
        self.assertEqual(sum("observation_cap" not in g for g in grid), 36)
        base = recipe()
        for cap in (100., 1000.):
            validate(dict(base, own=dict(base["own"], observation_cap=cap)))
        with self.assertRaises(ValueError):
            validate(dict(base, own=dict(base["own"], observation_cap=50.)))


def lookup_root(root, scores):
    """A 512-bucket eval-lookup root with {(lo, hi): cp} rows."""
    root.mkdir(parents=True)
    by = {}
    for (lo, hi), cp in scores.items():
        by.setdefault(int(lo) % 512, []).append((lo, hi, cp, 1))
    files = {}
    for i in range(512):
        rows = np.array(sorted(by.get(i, [])), dtype=ROW)
        f = root / f"bkt{i:03d}.npy"
        np.save(f, rows)
        files[f.name] = {"rows": len(rows), "sha256": sha(f)}
    write_json(root / "lookup.json", {"schema": SCHEMA, "book_meta_sha256": "fixture", "buckets": files})
    return root


class FixedReferee(unittest.TestCase):
    def test_paired_arms_are_scored_by_one_referee_binding(self):
        from sweep.referee import play
        k = keys()
        own, op = Fixed("e4"), Fixed("e5")
        op.colour = "white"; op.conditional = op.distributions
        p = SharedPolicies(own, op, k)
        start = board()
        starts = [(k.board(start), start.epd(), 1., 0, 12.75, 0., 1.)]
        a = SimpleNamespace(rollouts=1, max_ply=2, position_keys=k, our_elo=1450, opp_elo=2200, recall=1.)
        probe = play(starts, {}, white=True, args=a, seed=3, policies=p, lookup=lambda ids: np.full(len(ids), 0.))
        self.assertTrue(probe["test_lookup_override"])
        leaf = probe["leaves"][0]["key"]
        lo, hi = (int(x[0]) for x in k.halves(np.array([leaf], np.int64)))
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            ref = lookup_root(root / "ref", {(lo, hi): 120})
            other = lookup_root(root / "other", {(lo, hi): -400})
            binding = root / "referee.json"
            atomic_json(binding, dict(schema="eval-binding-v1", key_bits=128, lookup=str(ref),
                                      lookup_sha256=file_sha(ref / "lookup.json")))
            # The arm's own construction lookup must never be consulted under a referee binding.
            k.scores = lambda ids: (_ for _ in ()).throw(AssertionError("arm construction lookup used"))
            k.binding = dict(lookup=str(other))
            got = play(starts, {}, white=True, args=a, seed=3, policies=p, referee_binding=binding)
        self.assertEqual(got["referee_role"], "fixed-referee")
        self.assertFalse(got["test_lookup_override"])
        self.assertEqual(got["evaluation_binding"]["lookup"], str(ref))
        self.assertEqual(got["leaves"][0]["cp"], 120.)
        self.assertAlmostEqual(got["lower"], rs.continuation_utility([120.], True)[0])
        with self.assertRaises(ValueError):
            play(starts, {}, white=True, args=a, seed=3, policies=p, referee_binding=binding,
                 lookup=lambda ids: np.zeros(len(ids)))


if __name__ == "__main__":
    unittest.main()
