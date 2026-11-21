"""Regressions for the 19 November review (docs/claude-review-of-codex-2026-11-19.md).

  1  Black builds DP + match-distinct + polish at match-inflate 1.5; the manifest validates
  2  a parent stored above every child (the relative gate's reference) raises a request, both
     colours; a verified parent does not
  3  requests come from the book's policy reach above a threshold, ranked and capped, with
     deferred / below-threshold accounting
  4  an unverified rare reference that cuts a rival is requested; a verified one is not
  5  build_weak_edges requires a cap and refuses full-key inputs
  6  the ensemble's strengths come from the recipe; the defaults are the approved formula
  7  an endings-only board keeps one pseudo-continuation when another source saw moves
"""
import json
import math
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import polars as pl

import budget_core as bc
from stage3_backwards_induction import cp_to_expected_score as ws
from sweep import builder, candidates, policy, spec
from sweep.frontier import continuation_mass

HERE = Path(__file__).resolve().parent
WHITE, BLACK = spec.BASELINES["white"], spec.BASELINES["black"]


def frame(rows):
    return pl.DataFrame(rows, schema={"parent_hash": pl.Int64, "parent_epd": pl.String, "move_san": pl.String,
                                      "child_hash": pl.Int64, "total": pl.Int64})


def lookup_from(cps):
    return lambda arr: np.array([cps.get(int(h), -32768) for h in arr])


class Solver(unittest.TestCase):
    def test_black_polish_and_inflate(self):
        m = spec.manifest()
        pilot = next(a for a in m["primary"] if a["id"] == m["pilot"])
        r = pilot["recipe"]
        self.assertEqual((r["solver"], r["match_inflate"]), ("dp+match-distinct+polish", 1.5))
        inputs = dict(source="s", stats="t", aux="a", weak="w", opponent_overlay="o")
        cmd = builder.command(pilot, python="py", code=HERE, inputs=inputs, out_prefix="p", tmp="t")
        self.assertIn("--polish", cmd)
        self.assertIn("--match-distinct", cmd)
        self.assertEqual(cmd[cmd.index("--match-inflate") + 1], "1.5")
        self.assertEqual(cmd[cmd.index("--method") + 1], "dp")
        for flag, key in (("--quality-min-reach", "min_reach"), ("--quality-max-parents", "max_parents"),
                          ("--quality-rare-trust", "rare_trust")):
            self.assertEqual(cmd[cmd.index(flag) + 1], str(r["verification"][key]))
        white = next(a for a in m["primary"] if a["recipe"]["colour"] == "white")
        wcmd = builder.command(white, python="py", code=HERE, inputs=inputs, out_prefix="p", tmp="t")
        self.assertNotIn("--polish", wcmd)          # price search always polishes
        self.assertEqual(wcmd[wcmd.index("--method") + 1], "price")
        bad = dict(pilot, recipe=dict(r, solver="dp+match-distinct"))
        bad["id"] = spec.digest(bad["recipe"])[:20]
        with self.assertRaisesRegex(ValueError, "unknown solver"):
            builder.command(bad, python="py", code=HERE, inputs=inputs, out_prefix="p", tmp="t")

    def test_current_manifest_is_the_generator(self):
        # v4 (2026-11-20): otb-ensemble-v2, the Maia support gate. v3 is kept for audit and refused.
        docs = HERE.parent / "docs"
        path = docs / "sweep-manifest-2026-11-20-v4.json"
        if path.exists():
            self.assertEqual(json.loads(path.read_text(encoding="utf8")), spec.manifest())
        old = docs / "sweep-manifest-2026-11-19-maia-v3.json"
        if old.exists():
            with self.assertRaisesRegex(ValueError, "opponent ensemble recipe"):
                spec.validate(json.loads(old.read_text(encoding="utf8")))


class ParentReference(unittest.TestCase):
    def check(self, white):
        sign = 1 if white else -1
        # parent +150 own side, children +60 and +40: gap 0.078 > 0.06, the menu is empty
        cps = {1: sign * 150, 11: sign * 60, 12: sign * 40}
        eval_ws = {h: ws(c) for h, c in cps.items()}
        edges = [{"move_san": "a", "child_hash": 11, "total": 900}, {"move_san": "b", "child_hash": 12, "total": 500}]
        prior = WHITE if white else BLACK
        self.assertEqual(bc.score_candidates(1, edges, eval_ws, white, prior, .05, .06, 0, {}, None), [])
        side = "w" if white else "b"
        f = frame([dict(parent_hash=1, parent_epd=f"x {side} - -", move_san=m, child_hash=c, total=t)
                   for m, c, t in (("a", 11, 900), ("b", 12, 500))])
        floor = (prior if white else 1 - prior) - .05
        rows = candidates.requests(f, {1: .3}, lookup_from(cps), white=white, floor=floor, limit=.06)
        self.assertEqual(len(rows), 1)
        self.assertIn("parent-above-children", rows[0]["reason"])
        self.assertEqual(rows[0]["moves"], ["a", "b"])
        # once the parent has a 5M+ verification its score stands: no repeated request
        verified = pl.DataFrame({"position_hash": [1], "move_san": ["a"], "verification_nodes": [5_000_000]})
        again = candidates.requests(f, {1: .3}, lookup_from(cps), white=white, floor=floor, limit=.06,
                                    verified_frame=verified)
        self.assertFalse(any("parent-above-children" in r["reason"] for r in again))

    def test_white(self):
        self.check(True)

    def test_black(self):
        self.check(False)


class Scope(unittest.TestCase):
    def test_policy_reach_threshold_rank_and_cap(self):
        rows, cps = [], {}
        for i in range(1, 7):
            rows.append(dict(parent_hash=i, parent_epd="x w - -", move_san="m", child_hash=100 + i, total=50))
        f = frame(rows)          # no child evals at all: every parent has missing evidence
        reach = {1: .2, 2: .1, 3: .05, 4: .0004, 5: .0001}       # 6 is off the book's policy
        out = candidates.requests(f, reach, lookup_from(cps), white=True, floor=.4683, limit=.06,
                                  min_reach=.0005)
        self.assertEqual(sorted(r["position_hash"] for r in out), [1, 2, 3])
        kept, summary = candidates.select(out, reach, min_reach=.0005, max_parents=2, graph_parents=6)
        self.assertEqual([r["position_hash"] for r in kept], [1, 2])
        self.assertEqual((summary["requested"], summary["deferred"]), (2, 1))
        self.assertAlmostEqual(summary["deferred_reach"], .05)
        self.assertEqual(summary["below_threshold_positions"], 2)
        self.assertAlmostEqual(summary["below_threshold_reach"], .0005)
        self.assertEqual(summary["off_policy_graph_parents"], 1)
        self.assertEqual(summary["reasons"], {"missing-candidate-evidence": 3})
        with self.assertRaises(ValueError):
            candidates.select(out, reach, min_reach=.0005, max_parents=0)


class RareReference(unittest.TestCase):
    def test_unverified_rare_reference_cuts_a_rival(self):
        # rare move r (2 games) stored at +120, the parent agreeing at +115; the common move c
        # (900 games) at +40 is cut (loss 0.072 > 0.06) but would pass against the trusted best
        # (c itself). The 10 November pattern.
        cps = {1: 115, 11: 120, 12: 40}
        f = frame([dict(parent_hash=1, parent_epd="x w - -", move_san="r", child_hash=11, total=2),
                   dict(parent_hash=1, parent_epd="x w - -", move_san="c", child_hash=12, total=900)])
        out = candidates.requests(f, {1: .1}, lookup_from(cps), white=True, floor=.4683, limit=.06, rare_trust=20)
        self.assertEqual(len(out), 1)
        self.assertIn("unverified-rare-reference", out[0]["reason"])
        self.assertEqual(out[0]["moves"], ["c", "r"])
        verified = pl.DataFrame({"position_hash": [1], "move_san": ["r"], "verification_nodes": [5_000_000]})
        out = candidates.requests(f, {1: .1}, lookup_from(cps), white=True, floor=.4683, limit=.06,
                                  rare_trust=20, verified_frame=verified)
        self.assertFalse(any("unverified-rare-reference" in r["reason"] for r in out))
        # without the rule (rare_trust None) nothing is asked: the old blind spot
        out = candidates.requests(f, {1: .1}, lookup_from(cps), white=True, floor=.4683, limit=.06)
        self.assertEqual(out, [])


class WeakEdges(unittest.TestCase):
    def test_requires_cap_and_refuses_full_keys(self):
        py = sys.executable
        script = str(HERE / "build_weak_edges.py")
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            pool = td / "pool.parquet"
            pool.write_text("x")
            (td / "pool.parquet.meta.json").write_text(json.dumps({"key_bits": 128}))
            base = [py, script, "--pool", str(pool), "--book", str(td / "book"), "--bands", "1000",
                    "--out", str(td / "o.parquet"), "--work", str(td / "w")]
            r = subprocess.run(base, capture_output=True, text=True)
            self.assertEqual(r.returncode, 2)
            self.assertIn("--max-ply", r.stderr)
            r = subprocess.run(base + ["--max-ply", "30"], capture_output=True, text=True)
            self.assertEqual(r.returncode, 2)
            self.assertIn("64-bit hash only", r.stderr)


class Ensemble(unittest.TestCase):
    def test_constants_from_recipe(self):
        q = [.6, .3, .1]
        b, a, r, o = [500, 200, 10], [300, 80, 2], [100, 20, 0], [40, 5, 1]
        default = policy.ensemble(["x", "y", "z"], b, a, r, o, q)
        L, B, A, R, O = 3, sum(b), sum(a), sum(r), sum(o)
        support = R + .10 * (A - R) + .01 * (B - A)
        want = []
        for bb, aa, rr, oo, qq in zip(b, a, r, o, q):
            broad = (bb + 1 / L) / (B + 1)
            backup = (aa - rr + 30 * broad) / (A - R + 30)
            online = (rr + 30 * backup) / (R + 30)
            pre = (support * online + 3 * qq) / (support + 3)
            want.append((oo + 10 * pre) / (O + 10))
        self.assertTrue(all(math.isclose(x, y, rel_tol=0, abs_tol=1e-15) for x, y in zip(default, want)))
        P = spec.POLICY
        same = policy.ensemble(["x", "y", "z"], b, a, r, o, q, otb_strength=P["otb_strength"],
                               empirical_strength=P["empirical_strength"], maia_strength=P["maia_strength"],
                               backup_discount=P["backup_discount"], broad_discount=P["broad_discount"])
        self.assertEqual(same, default)
        changed = policy.ensemble(["x", "y", "z"], b, a, r, o, q, maia_strength=300.)
        self.assertNotEqual(changed, default)
        self.assertAlmostEqual(sum(changed), 1.)
        with self.assertRaises(ValueError):
            policy.ensemble(["x", "y", "z"], b, a, r, o, q, backup_discount=1.5)


class Mass(unittest.TestCase):
    def test_endings_only_board(self):
        self.assertEqual(continuation_mass(40, 7, 100), 40.)
        self.assertEqual(continuation_mass(0, 0, 0), 1.)       # model-only continuation
        self.assertEqual(continuation_mass(0, 12, 0), 0.)      # every source only ended here
        self.assertEqual(continuation_mass(0, 12, 300), 1.)    # others continued: keep replies


if __name__ == "__main__":
    unittest.main()
