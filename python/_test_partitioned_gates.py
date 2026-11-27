"""pool_from_book_gates --partitioned and sweep.partitioned_pool's acceptance assembler.

A real 512-bucket book (the _test_pool128 fixture, re-bucketed), a real uncut pool,
the real per-bucket sorter, then all four gates on the sorted pairs: every verdict
bound to the inventory, a partial sort refused, damage caught per bucket, and the
assembler refusing failed or unbound verdicts before publication accepts its output.
"""
import json
import math
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import chess
import polars as pl

import pool_from_book as pool
import pool_from_book_gates as gates
from _test_pool128 import make_inputs
from eval_lookup128 import SCHEMA as LOOKUP_SCHEMA
from month_archive import sha, write_json
from sweep import partitioned_pool as pub
from sweep.stages import file_sha
from zobrist128 import zobrist128


def pinned(path):
    return dict(path=str(path), sha256=file_sha(path))


def rebucket(book, lookup):
    """The fixture's two-bucket book as a 512-bucket one (rem_euclid(lo, 512))."""
    slices = book / "ps/event=Rapid/elo_band=1800"
    moves = pl.concat([pl.read_parquet(f) for f in sorted(slices.glob("bkt*.parquet"))])
    terms = pl.concat([pl.read_parquet(f) for f in sorted((book / "term").glob("bkt*.parquet"))])
    for f in [*slices.glob("*.parquet"), *(book / "term").glob("*.parquet"), *(book / "_done").glob("*")]:
        f.unlink()
    rem = lambda c: ((pl.col(c) % 512) + 512) % 512
    size = n_files = 0
    for i in range(512):
        part, files = moves.filter(rem("parent_hash") == i), []
        if part.height:
            f = slices / f"bkt{i:03d}.parquet"
            part.write_parquet(f)
            size, n_files = size + f.stat().st_size, n_files + 1
            files.append({"path": f.relative_to(book).as_posix(), "bytes": f.stat().st_size,
                          "event": "Rapid", "elo_band": 1800})
        write_json(book / "_done" / f"bkt{i:03d}.DONE", {"files": files})
        terms.filter(rem("position_hash") == i).write_parquet(book / "term" / f"bkt{i:03d}.parquet")
    games = int(moves.filter(pl.col("ply") == 1)["total"].sum())
    pl.DataFrame([dict(event="Rapid", elo_band=1800, files=n_files, bytes=size, ply1_games=games),
                  dict(event="Rapid", elo_band=2000, files=0, bytes=0, ply1_games=0)]).write_parquet(book / "_slices.parquet")
    settings = {"events": ["Rapid"], "buckets": 512, "max_ply": 6, "key_bits": 128, "term_slices": True}
    write_json(book / "_merge_params.json", {"schema": "book-v3", "input_params": settings})
    write_json(book / "_book.meta.json", {"input": {"params": settings}, "term_event_ids": ["Rapid"]})
    meta = json.loads((lookup / "lookup.json").read_text())
    write_json(lookup / "lookup.json", dict(meta, schema=LOOKUP_SCHEMA, book_meta_sha256=sha(book / "_book.meta.json")))


def serial(fn, tasks, workers, fmt):
    return [fn(t) for t in tasks]


ROOT_B = zobrist128(chess.Board())[0] % 512
SAMPLE = sorted({1, 2, 4, 495, ROOT_B})   # planted-twin buckets, an empty one, the root's


class PartitionedGates(unittest.TestCase):
    def test_isolated_runner(self):
        got = gates._isolated_run(math.sqrt, [4, 9, 16], 2, str)
        self.assertEqual(sorted(got), [2.0, 3.0, 4.0])
        with self.assertRaises(ValueError):
            gates._isolated_run(math.sqrt, [4, -1, 9], 2, str)

    def test_gates_bind_assemble_publish(self):
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            book, lookup = make_inputs(folder)
            rebucket(book, lookup)
            work = folder / "work"
            args = ["pool_from_book", "--book", str(book), "--out-dir", str(folder / "exports"),
                    "--work", str(work), "--eval-lookup128", str(lookup), "--events", "Rapid",
                    "--elo-bands", "1800", "2000", "--max-ply", "5", "--min-games", "1",
                    "--tag", "part", "--tmp-dir", str(folder / "spill")]
            with patch.object(pool, "GROUP_SIZE", 512), patch.object(pool, "_pool_run", serial), \
                    patch("sys.argv", args):
                pool.main()

            # The real sorter over all 512 pairs.
            proof = folder / "source-proof.txt"
            proof.write_text("fixture source parity")
            built = work / "buckets"
            cfg = dict(params=pinned(work / "_params.json"), output=str(folder / "partitioned"),
                       tmp=str(folder / "sort-spill"), reserve_bytes=0, threads=1, memory="256MB",
                       term_reasons="split",
                       sources={str(b): dict(edges=pinned(built / f"b{b:03d}.edges.parquet"),
                                             aux=pinned(built / f"b{b:03d}.aux.parquet"),
                                             stats=pinned(built / f"b{b:03d}.stats.json"),
                                             acceptance=[pinned(proof)]) for b in range(512)})
            part, gd = Path(cfg["output"]), folder / "gates"

            def run(gate, *extra, partitioned=part, gates_dir=gd):
                argv = ["gates", gate, "--work", str(work), "--partitioned", str(partitioned),
                        "--gates-dir", str(gates_dir), "--tmp-dir", str(folder / "gate-spill"),
                        "--threads", "1", "--mem", "256MB", "--workers", "1", *extra]
                with patch.object(pool, "_pool_run", serial), patch.object(gates, "_isolated_run", serial), \
                        patch("sys.argv", argv):
                    return gates.main()

            for b in range(511):
                pub.prepare_bucket(cfg, b)
            with self.assertRaises(FileNotFoundError):        # a partial sort is never gated
                run("conservation")
            self.assertFalse(gd.exists())
            pub.prepare_bucket(cfg, 511)
            with self.assertRaisesRegex(SystemExit, "gates-dir"):
                with patch("sys.argv", ["gates", "evals", "--work", str(work), "--partitioned", str(part)]):
                    gates.main()

            buckets = ["--buckets", *map(str, SAMPLE)]
            self.assertEqual(run("conservation", *buckets), 0)
            self.assertEqual(run("reimpl", *buckets), 0)
            self.assertEqual(run("term-vs-book", *buckets), 0)
            self.assertEqual(run("evals", "--sample-buckets", "512", "--n", "100", "--seed", "11"), 0)
            gates._BINDING.clear()

            inv = pub.digest(pub.inventory(part, cfg["params"]["sha256"]))
            cons = json.loads((gd / "gate-conservation.json").read_text())
            self.assertEqual((cons["inventory_sha256"], cons["params_sha256"]), (inv, cfg["params"]["sha256"]))
            self.assertEqual(cons["pool_total"] + cons["other_total"] + cons["outside_mass_non_pool_parents"],
                             cons["population_le_cap_excl_collisions"])
            self.assertGreater(cons["non_root_parents"], 0)
            self.assertEqual(cons["per_parent_buckets"], SAMPLE)
            self.assertEqual(cons["per_parent_mismatches"], 0)
            self.assertFalse((gd / "conservation" / "arrivals").exists())   # the split arrivals are removed
            self.assertFalse((work / "_gates.json").exists())  # verdicts stay out of the build's work dir

            # The assembler refuses failed and unbound verdicts.
            for field, value, message in (("pass", False, "did not pass"),
                                          ("inventory_sha256", "0" * 64, "does not bind")):
                bad = folder / f"gates-{field}"
                shutil.copytree(gd, bad)
                v = json.loads((bad / "gate-reimpl.json").read_text())
                (bad / "gate-reimpl.json").write_text(json.dumps(dict(v, **{field: value})))
                with self.assertRaisesRegex(ValueError, message):
                    pub.assemble(cfg, bad)
            doc = pub.assemble(cfg, gd)
            self.assertEqual(set(doc["checks"]), pub.GATES)
            self.assertEqual(pub.assemble(cfg, gd), doc)        # rerun: same document
            published = pub.publish(cfg, gd / "acceptance.json")
            self.assertTrue(published["complete"])

            # Damage in one sorted bucket fails that bucket's mass identity.
            damaged = folder / "damaged"
            shutil.copytree(part, damaged, ignore=shutil.ignore_patterns("*.lock"))
            f = damaged / "buckets" / f"b{ROOT_B:03d}.edges.parquet"
            frame = pl.read_parquet(f)
            frame.head(frame.height - 1).write_parquet(f)
            self.assertEqual(run("conservation", *buckets, partitioned=damaged, gates_dir=folder / "gates-damaged"), 1)
            gates._BINDING.clear()
            verdict = json.loads((folder / "gates-damaged" / "gate-conservation.json").read_text())
            self.assertEqual(verdict["mass_mismatch_buckets"], [ROOT_B])
            self.assertFalse(verdict["root_ok"])

            # One parent's aux row off by a game in a sampled non-root bucket: the per-parent identity,
            # read from the split arrivals, names exactly that parent.
            off = folder / "damaged-aux"
            shutil.copytree(part, off, ignore=shutil.ignore_patterns("*.lock"))
            f = off / "buckets" / "b495.aux.parquet"
            frame = pl.read_parquet(f)
            frame.with_columns(pl.when(pl.int_range(frame.height) == 0).then(pl.col("other_total") + 1)
                               .otherwise(pl.col("other_total")).alias("other_total")).write_parquet(f)
            self.assertEqual(run("conservation", *buckets, partitioned=off, gates_dir=folder / "gates-aux"), 1)
            gates._BINDING.clear()
            verdict = json.loads((folder / "gates-aux" / "gate-conservation.json").read_text())
            self.assertEqual(verdict["per_parent_mismatches"], 1)
            self.assertEqual(verdict["mass_mismatch_buckets"], [495])


if __name__ == "__main__":
    unittest.main()
