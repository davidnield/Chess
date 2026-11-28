"""Screen-then-confirm for the extension comparison (sweep.screening): the drop rule on synthetic
families, disjoint seeds, paired inputs, and the flag-off manifest unchanged (v5 byte-for-byte)."""
import json
import random
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
import polars as pl
from sweep import spec
from sweep.screening import SCREENING, screen, read_scores, control_run

HERE = Path(__file__).resolve().parent
V5 = HERE.parent / "docs" / "sweep-manifest-2026-11-21-v5.json"
DEFAULTS = dict(default_metric=spec.DEFAULT_EXTENSION_METRIC, default_threshold=spec.EXTENSIONS["threshold"])


def synthetic(comparison, offsets, n=4096, seed=7):
    """Paired scores: a common per-trajectory draw (the shared random stream) plus each run's offset
    and a small run-specific noise, so paired SEs are small and unpaired ones would not be."""
    rng = random.Random(seed)
    scores = {}
    for base in sorted({r["recipe"]["base_arm"] for r in comparison["runs"]}):
        members = [r["id"] for r in comparison["runs"] if r["recipe"]["base_arm"] == base]
        for s in SCREENING["screen_seeds"]:
            common = [rng.random() for _ in range(n)]
            for rid in members:
                scores.setdefault(rid, {})[s] = [c + offsets.get(rid, 0.0) + rng.gauss(0, 0.01) for c in common]
    return scores


class Screening(unittest.TestCase):
    def setUp(self):
        self.comp = spec.manifest(True)["extension_comparison"]
        self.bases = sorted({r["recipe"]["base_arm"] for r in self.comp["runs"]})

    def test_drop_rule_on_synthetic_families(self):
        base = self.bases[0]
        members = sorted(r["id"] for r in self.comp["runs"] if r["recipe"]["base_arm"] == base)
        control = control_run(self.comp, base, **DEFAULTS)
        others = [m for m in members if m != control]
        leader, close, far = others[0], others[1], others[2]
        offsets = {rid: -0.02 for rid in members}            # everyone else well behind: dropped
        offsets.update({leader: 0.0, close: -0.0002, far: -0.01, control: -0.02})
        got = screen(self.comp, synthetic(self.comp, offsets), **DEFAULTS)
        fam = got["families"][base]
        self.assertEqual(fam["leader"], leader)
        self.assertEqual(fam["runs"][close]["decision"], "confirm")   # inside 2.33 SE + .001
        self.assertEqual(fam["runs"][far]["decision"], "drop")
        self.assertEqual(fam["runs"][control]["decision"], "control")  # behind, but never dropped
        self.assertIn(control, got["confirm"])
        self.assertNotIn(far, got["confirm"])
        self.assertAlmostEqual(fam["runs"][far]["diff"], -0.01, delta=0.002)
        self.assertLess(fam["runs"][far]["se"], 0.001)                # paired: shared stream cancels
        # each family keeps exactly its leader and control when all else trails badly
        self.assertEqual(sum(d["decision"] in ("leader", "control", "confirm") for d in fam["runs"].values()), 3)
        n = len(self.comp["runs"])
        self.assertEqual(got["referee_runs"]["unscreened"], n * 6)
        self.assertEqual(got["referee_runs"]["screen"], n * 2)
        self.assertEqual(got["referee_runs"]["confirm"], len(got["confirm"]) * 4)

    def test_equal_variants_are_all_kept_and_decisions_repeat(self):
        got = screen(self.comp, synthetic(self.comp, {}, seed=3), **DEFAULTS)
        # identical offsets: differences are pure noise, far inside the margin
        self.assertEqual(got["dropped"], [])
        again = screen(self.comp, synthetic(self.comp, {}, seed=3), **DEFAULTS)
        self.assertEqual(got, again)

    def test_seeds_are_disjoint_and_screening_seeds_only(self):
        self.assertFalse(set(SCREENING["screen_seeds"]) & set(SCREENING["confirm_seeds"]))
        scores = synthetic(self.comp, {})
        rid = next(iter(scores))
        bad = deepcopy(scores); bad[rid][3] = bad[rid][1]
        with self.assertRaisesRegex(ValueError, "screening seeds"):
            screen(self.comp, bad, **DEFAULTS)
        rule = dict(SCREENING, confirm_seeds=[2, 3, 4, 5])
        with self.assertRaisesRegex(ValueError, "not queued with the screening rule"):
            screen(self.comp, scores, rule=rule, **DEFAULTS)

    def test_inputs_must_be_complete_and_paired(self):
        scores = synthetic(self.comp, {})
        rid = sorted(scores)[0]
        missing = {k: v for k, v in scores.items() if k != rid}
        with self.assertRaisesRegex(ValueError, "every queued run"):
            screen(self.comp, missing, **DEFAULTS)
        short = deepcopy(scores); short[rid][1] = short[rid][1][:-1]
        with self.assertRaisesRegex(ValueError, "unpaired"):
            screen(self.comp, short, **DEFAULTS)

    def test_v5_comparison_is_refused(self):
        v5 = spec.manifest()["extension_comparison"]
        with self.assertRaisesRegex(ValueError, "manifest v6"):
            screen(v5, synthetic(v5, {}), **DEFAULTS)

    def test_flag_off_is_an_exact_no_op(self):
        v5 = json.loads(V5.read_text(encoding="utf8"))
        self.assertEqual(spec.manifest(), v5)
        spec.validate(v5)
        v6 = spec.manifest(True)
        spec.validate(v6)
        self.assertEqual({k for k in v6 if v6[k] != v5[k]}, {"extension_comparison"})
        diff = {k for k in v6["extension_comparison"] if v6["extension_comparison"][k] != v5["extension_comparison"][k]}
        self.assertEqual(diff, {"evaluation"})
        drifted = deepcopy(v6); drifted["extension_comparison"]["evaluation"]["z"] = 1.0
        with self.assertRaisesRegex(ValueError, "drifted"):
            spec.validate(drifted)

    def test_read_scores_parquet(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "s.parquet"
            pl.DataFrame(dict(run=["a"] * 4, seed=[1, 1, 2, 2], trajectory=[0, 1, 0, 1],
                              score=[.1, .2, .3, .4])).write_parquet(path)
            self.assertEqual(read_scores(path), {"a": {1: [.1, .2], 2: [.3, .4]}})
            pl.DataFrame(dict(run=["a"] * 2, seed=[1, 1], trajectory=[0, 2], score=[.1, .2])).write_parquet(path)
            with self.assertRaisesRegex(ValueError, "0..n-1"):
                read_scores(path)


if __name__ == "__main__":
    import sys
    result = unittest.main(exit=False, verbosity=2).result
    sys.exit(0 if result.wasSuccessful() else 1)
