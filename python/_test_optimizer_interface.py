"""Small FFI/serialization checks only; every search and evaluation runs in Rust.

Build the opt-in extension with rust/book_builder/build.ps1 -Optimizer first.
"""
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np
import book_builder_rs as rs
import budget_core as bc
import budget_rs


class OptimizerInterface(unittest.TestCase):
    def setUp(self):
        self.ids = rs.RsPositionIds()
        self.a = self.ids.encode_one(7, 1)
        self.b = self.ids.encode_one(7, 2)
        self.root = self.ids.encode_one(9, 3)
        self.graph = bc.Graph(
            self.root,
            {self.a: bc.OurNode(.2, [("e4", None, False, .8)]),
             self.b: bc.OurNode(.3, [("d4", None, False, .9)])},
            {self.root: bc.OppNode(0., [(.5, self.a, True, .2), (.5, self.b, True, .3)])},
            {}, {}, {self.root: 1., self.a: .5, self.b: .5})
        self.csr, self.sans, _ = budget_rs.graph_to_csr(self.graph)
        self.order = budget_rs.set_order(list(self.graph.our), list(self.graph.opp))
        self.native = rs.RsGraph(self.csr, self.order, self.root, False, self.ids)
        limits = dict(milliseconds=2000, states=10000, max_nodes=2000, max_variables=20000)
        self.config = json.dumps(dict(schema=1, budget=2, caps=[256], oracle_max_own=12,
            max_input_bytes=64 * 1024 * 1024, limits=limits,
            neighborhood=dict(width=12, rounds=2, discovery_nodes=2000, limits=limits)))
        self.receipts = json.dumps({k: "0" * 64 for k in (
            "candidate-graph", "engine-overlay", "opponent-policy", "stop-values",
            "horizon-and-cuts", "price-source-policy")})  # explicitly synthetic bindings
        scratch = Path(__file__).resolve().parents[1] / "scratch" / "optimizer-interface-tests"
        scratch.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(self.temp.cleanup)
        self.out = Path(self.temp.name)

    def test_native_compare_resume_full_keys_sans_and_source_filter(self):
        keys = np.array([self.a, self.b, 123456], np.int64)
        sans = np.array([self.sans.index("e4"), 999, 0], np.int64)
        args = (self.config, self.receipts, str(self.out), keys, sans, self.sans)
        a = json.loads(self.native.optimizer_compare(*args))
        b = json.loads(self.native.optimizer_compare(*args))
        self.assertEqual(a, b)
        self.assertEqual(a, json.loads((self.out / "portable-results.json").read_text()))
        self.assertEqual(a["source_filter"], dict(input=3, accepted=1, absent=1, inadmissible=1))
        self.assertEqual(len(set(a["book_content_sha256"].values())), 1)
        for cell in a["report"]["cells"]:
            self.assertAlmostEqual(cell["outcome"]["value"], .85)
        for book in a["full_key_books"].values():
            self.assertEqual({(m["lo"], m["hi"], m["san"]) for m in book},
                             {(7, 1, "e4"), (7, 2, "d4")})
        with self.assertRaisesRegex(ValueError, "identity drift"):
            changed = list(self.sans)
            changed.reverse()
            self.native.optimizer_compare(*args[:-1], changed)

    def test_bounded_snapshot_decodes_registry_and_retains_boundary(self):
        k, s = budget_rs.rs_moves({self.a: "e4", self.b: "d4"}, self.sans)
        path = self.out / "cone.json"
        self.native.optimizer_snapshot(self.root, k, s, 2, 1, str(path), self.sans)
        cone = json.loads(path.read_text())
        self.assertEqual(cone["root"], f"{(3 << 64) | 9:032x}")
        self.assertEqual({n["key"] for n in cone["nodes"]},
                         {f"{(3 << 64) | 9:032x}", f"{(1 << 64) | 7:032x}"})
        self.assertEqual(json.loads(cone["provenance"]["move_names_json"]), self.sans)
        root = next(n for n in cone["nodes"] if n["key"] == cone["root"])
        self.assertEqual(root["edges"][1]["leaf"], .9)
        self.assertIsNone(root["edges"][1]["child"])
        with self.assertRaisesRegex(ValueError, "refuse overwrite"):
            self.native.optimizer_snapshot(self.root, k, s, 2, 1, str(path), self.sans)

    def test_rejects_missing_registry_names_and_unknown_settings(self):
        k, s = budget_rs.rs_moves({}, self.sans)
        missing = rs.RsGraph(self.csr, self.order, self.root, False)
        with self.assertRaisesRegex(ValueError, "original full-key registry"):
            missing.optimizer_compare(self.config, self.receipts, str(self.out), k, s, self.sans)
        with self.assertRaisesRegex(ValueError, "SAN dictionary"):
            self.native.optimizer_compare(self.config, self.receipts, str(self.out), k, s, ["e4", "e4"])
        cfg = json.loads(self.config)
        cfg["typo"] = 1
        with self.assertRaisesRegex(ValueError, "unknown field"):
            rs.RsGraph.optimizer_validate_config(json.dumps(cfg))
        self.assertEqual(rs.RsGraph.optimizer_validate_config(self.config), 2)


if __name__ == "__main__":
    unittest.main()
