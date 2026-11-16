"""Exercise the pool and uncut public paths with planted low-half twins of both colours."""
import json
import math
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import chess
import numpy as np
import polars as pl

import pool_from_book as pool
import pool_from_book_gates as gates
import uncut_own_moves as uncut
from eval_lookup128 import CLOUD, ROW, SCHEMA
from month_archive import sha, write_json
from stage3_backwards_induction import LICHESS_CP_SCALE
from zobrist128 import zobrist128


def board(line):
    pos = chess.Board()
    for san in line.split():
        pos.push_san(san)
    return pos


# Only these six boards have fabricated keys. All move orders, EPDs and other
# keys are real, including the return to the starting position after four plies.
TWINS = {board(line).epd(): (lo, hi) for line, lo, hi in (
    ("e4", -17, -3), ("d4", -17, 4), ("e4 e5", 4, -5), ("d4 d5", 4, 7),
    ("e4 c5", 1, -2), ("d4 Nf6", 1, 11),
)}


def key(pos):
    return TWINS.get(pos.epd(), zobrist128(pos))


def make_inputs(folder):
    book, lookup = folder / "book", folder / "eval"
    move_rows, term_rows = [], []
    for n, line, result, reason in (
        (11, "e4 e5 Nf3", "white_wins", 0), (2, "e4 e5 Nc3", "draws", 0),
        (2, "e4 c5", "black_wins", 0), (3, "e4", "draws", 1),
        (9, "d4 d5 c4", "black_wins", 0), (1, "d4 d5 Nf3", "white_wins", 0),
        (1, "d4 Nf6", "white_wins", 1), (4, "d4", "black_wins", 0),
        (2, "c4 e5", "draws", 0), (5, "c4", "white_wins", 1),
        (2, "Nf3 Nf6 Ng1 Ng8 e4 e6", "white_wins", 0),
        (1, "Nf3 Nf6 Ng1 Ng8 d4 d6", "draws", 0),
    ):
        pos = chess.Board()
        counts = {c: n if c in ("total", result) else 0 for c in pool.SUMS}
        for ply, san in enumerate(line.split(), 1):
            lo, hi = key(pos)
            epd = pos.epd()
            pos.push_san(san)
            cl, ch = key(pos)
            move_rows.append(dict(parent_hash=lo, parent_hash_hi=hi, child_hash=cl,
                                  child_hash_hi=ch, parent_epd=epd, move_san=san, ply=ply,
                                  event="Rapid", elo_band=1800, **counts))
        term_rows.append(dict(position_hash=cl, position_hash_hi=ch, end_ply=ply, kind=0,
                              reason=reason, event_id=0, elo_band=1800, **counts))
    slices = book / "ps/event=Rapid/elo_band=1800"
    slices.mkdir(parents=True)
    (book / "term").mkdir()
    size = 0
    for i in range(2):
        f = slices / f"bkt{i:03d}.parquet"
        pl.DataFrame(move_rows).filter(pl.col("parent_hash") % 2 == i).write_parquet(f)
        size += f.stat().st_size
        write_json(book / "_done" / f"bkt{i:03d}.DONE", {"files": [{
            "path": f.relative_to(book).as_posix(), "bytes": f.stat().st_size,
            "event": "Rapid", "elo_band": 1800}]})
        pl.DataFrame(term_rows).filter(pl.col("position_hash") % 2 == i).write_parquet(
            book / "term" / f"bkt{i:03d}.parquet")
    games = sum(r["total"] for r in move_rows if r["ply"] == 1)
    pl.DataFrame([dict(event="Rapid", elo_band=1800, files=2, bytes=size, ply1_games=games)]).write_parquet(book / "_slices.parquet")
    pl.DataFrame({"parent_hash": [-17, 4, 1]}).write_parquet(book / "_collisions.parquet")
    settings = {"events": ["Rapid"], "buckets": 2, "max_ply": 6, "key_bits": 128, "term_slices": True}
    write_json(book / "_merge_params.json", {"schema": "book-v3", "input_params": settings})
    write_json(book / "_book.meta.json", {"input": {"params": settings}, "term_event_ids": ["Rapid"]})
    (book / "_BOOK.DONE").touch()
    lookup.mkdir()
    files = {}
    for i in range(512):
        scores = [(1, -2, -300, CLOUD), (1, 11, 500, CLOUD)] if i == 1 else []
        f = lookup / f"bkt{i:03d}.npy"
        np.save(f, np.array(scores, dtype=ROW))
        files[f.name] = {"rows": len(scores), "sha256": sha(f)}
    write_json(lookup / "lookup.json", {"schema": SCHEMA, "book_meta_sha256": sha(book / "_book.meta.json"),
                                        "buckets": files})
    return book, lookup


def rows_for(frame, column, line):
    lo, hi = key(board(line))
    return frame.filter((pl.col(column).struct.field("lo") == lo) &
                        (pl.col(column).struct.field("hi") == hi))


class Pool128Test(unittest.TestCase):
    def test_twins(self):
        with tempfile.TemporaryDirectory() as td:
            folder = Path(td)
            book, lookup = make_inputs(folder)
            args = ["pool_from_book", "--book", str(book), "--out-dir", td, "--work", str(folder / "work"),
                    "--eval-lookup128", str(lookup), "--events", "Rapid", "--elo-bands", "1800",
                    "--max-ply", "5", "--min-games", "5", "--tag", "twins", "--tmp-dir", str(folder / "spill")]
            def serial(fn, tasks, workers, fmt):
                return [fn(t) for t in tasks]
            with patch.object(pool, "N_BUCKETS", 2), patch.object(pool, "GROUP_SIZE", 1), \
                    patch.object(pool, "_pool_run", serial), patch("sys.argv", args):
                pool.main()
                pool.main()  # restart through the same entry point
                parsed = pool.build_parser().parse_args()
                stats, aux = pool.out_paths(parsed)
                self.assertEqual(json.loads(pool.meta_path(stats).read_text())["key_bits"], 128)
                self.assertTrue(gates.gate_conservation(parsed))
                parsed.buckets = [0, 1]
                self.assertTrue(gates.gate_term_vs_book(parsed))
                # The independent audits read the same planted scores directly
                # from the bucket arrays rather than calling WideEvalLookup.
                self.assertTrue(gates.gate_reimpl(parsed))
                parsed.seed, parsed.n, parsed.sample_buckets = 11, 100, 2
                self.assertTrue(gates.gate_evals(parsed))
                # Removing only the high-half twin must fail the independent audit.
                damaged = folder / "missing-twin"
                damaged.mkdir()
                shutil.copy2(stats, damaged / stats.name)
                pl.read_parquet(aux).filter(~((pl.col("position_hash").struct.field("lo") == -17) &
                                              (pl.col("position_hash").struct.field("hi") == 4))).write_parquet(damaged / aux.name)
                with patch.object(parsed, "out_dir", damaged):
                    self.assertFalse(gates.gate_reimpl(parsed))
                wrong_floor = pool.build_parser().parse_args(args[1:] + ["--min-games", "4", "--work", str(folder / "different")])
                pool.lock_params(wrong_floor)
                with self.assertRaisesRegex(ValueError, "another pool recipe"):
                    pool.check_export(stats, pool.output_schema(pool.POOL_SCHEMA, True),
                                      {"pool_recipe": sha(wrong_floor.work / "_params.json")})
                # Same work directory cannot resume with a changed eval generation.
                meta = json.loads((lookup / "lookup.json").read_text())
                write_json(lookup / "lookup.json", dict(meta, revision=2))
                with self.assertRaisesRegex(SystemExit, "different parameters"):
                    pool.lock_params(parsed)
            edges, sidecar = pl.read_parquet(stats), pl.read_parquet(aux)
            self.assertEqual(rows_for(edges, "parent_hash", "e4")["move_san"].to_list(), ["e5"])
            self.assertEqual(rows_for(edges, "parent_hash", "d4")["move_san"].to_list(), ["d5"])
            for line, cp, other, normal, flags, horizon in (
                    ("e4", -300, 2, 0, 3, 2), ("d4", 500, 1, 4, 0, 1)):
                row = rows_for(sidecar, "position_hash", line).row(0, named=True)
                self.assertEqual((row["other_total"], row["term_normal_total"], row["term_flag_total"],
                                  row["horizon_total"]), (other, normal, flags, horizon))
                self.assertEqual(row["other_eval_cov"], 1.0)
                self.assertAlmostEqual(row["other_eval_mean"], 1 / (1 + math.exp(-LICHESS_CP_SCALE * cp)))
            for colour, cases in (("black", [("e4", {"e5", "c5"}), ("d4", {"d5", "Nf6"})]),
                                  ("white", [("e4 e5", {"Nf3", "Nc3"}), ("d4 d5", {"c4", "Nf3"})])):
                cache = folder / colour
                uncut.prepare(stats, aux, colour, cache, merge_slices=2)
                graph = pl.read_parquet(cache / "graph.parquet")
                for line, moves in cases:
                    self.assertEqual(set(rows_for(graph, "parent_hash", line)["move_san"]), moves)
                if colour == "black":
                    raw = rows_for(pl.read_parquet(cache / "aux.parquet"), "position_hash", "c4").row(0, named=True)
                    self.assertEqual((raw["term_flag_total"], raw["term_other_total"]), (5, 0))


if __name__ == "__main__":
    unittest.main()
