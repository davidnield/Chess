"""lookup_floors.py on the uncut lookup fixture, both colours.

floor:   games conserved position by position, opponent rows untouched, and at our positions the
         sidecar is exactly what rebucket_pool.py writes for the same rows (one definition of the
         other bucket's arithmetic).
addback: with every our-turn position chosen, the result is the full lookup again, row for row.
"""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import polars as pl

import lookup_floors as lf
import rebucket_pool
import uncut_own_moves as lookup
from _test_uncut_own_moves import fixture

FAKE_CP = 37            # every child evaluates to the same score: coverage and means are checked exactly


def fake_evals(keys, *_):
    return np.full(len(keys), FAKE_CP, dtype=np.int16)


class TestLookupFloors(unittest.TestCase):
    def run_colour(self, colour, folder):
        stats, aux, _, _ = fixture(folder, colour)
        full = folder / "full"
        lookup.prepare(stats, aux, colour, full, memory="256MB")
        ours = "w" if colour == "white" else "b"
        g = pl.read_parquet(full / "graph.parquet")
        own = g.filter(pl.col("parent_epd").str.split(" ").list.get(1) == ours)
        floor = 13                                   # below it: the 12-, 11- and 2-game moves
        self.assertTrue(((own["total"] < floor).sum() > 0) and ((own["total"] >= floor).sum() > 0))
        with patch.object(lf, "open_eval_arrays", return_value=()), \
                patch.object(lf, "lookup_evals", side_effect=fake_evals):
            lf.floor(full, colour, floor, folder / "floored", threads=1, memory="256MB")
        ng = pl.read_parquet(folder / "floored" / "graph.parquet")
        nx = pl.read_parquet(folder / "floored" / "aux.parquet")
        fx = pl.read_parquet(full / "aux.parquet")
        dropped = own.filter(pl.col("total") < floor)
        self.assertEqual(ng.height, g.height - dropped.height)
        # games conserved at every position
        def mass(graph, side):
            named = graph.group_by("parent_hash").agg(pl.col("total").sum()).rename({"parent_hash": "h"})
            other = side.select(pl.col("position_hash").alias("h"), pl.col("other_total").alias("total"))
            return pl.concat([named, other]).group_by("h").agg(pl.col("total").sum()).sort("h")
        self.assertTrue(mass(g, fx).equals(mass(ng, nx)))
        # opponent rows untouched
        theirs = lambda f: f.filter(pl.col("parent_epd").str.split(" ").list.get(1) != ours).sort("parent_hash", "move_san")
        self.assertTrue(theirs(g).equals(theirs(ng)))
        # the other bucket at our positions is rebucket_pool's, column for column
        pool = folder / "pool"
        pool.mkdir()
        own.write_parquet(pool / "stats.parquet")
        fx.filter(pl.col("position_hash").is_in(own["parent_hash"].unique().to_list())).write_parquet(pool / "aux.parquet")
        for p in ("stats.parquet", "aux.parquet"):
            (pool / (p + ".meta.json")).write_text(json.dumps({"min_games": 1, "population": {"test": colour}}))
        with patch.object(rebucket_pool, "open_eval_arrays", return_value=()), \
                patch.object(rebucket_pool, "lookup_evals", side_effect=fake_evals):
            rebucket_pool.build(pool / "stats.parquet", pool / "aux.parquet", floor, pool / "out")
        theirs_aux = pl.read_parquet(pool / "out" / "aux.parquet").sort("position_hash")
        ours_aux = nx.filter(pl.col("position_hash").is_in(theirs_aux["position_hash"].to_list())).sort("position_hash")
        self.assertGreater(theirs_aux.height, 0)
        self.assertTrue(theirs_aux.select(ours_aux.columns).equals(ours_aux), f"\n{theirs_aux}\n{ours_aux}")
        # addback everywhere -> the full lookup again
        run = folder / "run"
        run.mkdir()
        book = run / "book.parquet"
        pl.DataFrame({"position_hash": own["parent_hash"].unique().to_list()}).with_columns(
            pl.lit(colour).alias("side_to_move")).write_parquet(book)
        (run / "final.json").write_text(json.dumps({"book": str(book)}))
        lf.addback(full, folder / "floored", [run], colour, folder / "back", threads=1, memory="256MB")
        bg = pl.read_parquet(folder / "back" / "graph.parquet").sort("parent_hash", "move_san")
        bx = pl.read_parquet(folder / "back" / "aux.parquet").sort("position_hash")
        self.assertTrue(bg.equals(g.sort("parent_hash", "move_san")))
        self.assertTrue(bx.equals(fx.sort("position_hash")))
        # both results load like any lookup
        for d in ("floored", "back"):
            r = lookup.read_json(folder / d / "ready.json")
            self.assertEqual(r["recipe_sha256"], lookup.digest(folder / d / "recipe.json"))

    def test_both_colours(self):
        for colour in ("white", "black"):
            with self.subTest(colour=colour), tempfile.TemporaryDirectory(dir=Path(__file__).parent) as path:
                self.run_colour(colour, Path(path))


if __name__ == "__main__":
    result = unittest.main(exit=False).result
    sys.exit(0 if result.wasSuccessful() else 1)
