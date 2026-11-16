"""Native monthly and banded fixtures with transpositions and subfloor-only parents."""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import build_budget_books as builder
import chess
import duckdb
import numpy as np
import polars as pl
import uncut_own_moves as lookup
from zobrist import zobrist_int64


def board(line):
    b = chess.Board()
    for move in line.split():
        b.push_san(move)
    return b


def key(line):
    return zobrist_int64(board(line))


def fixture(folder, colour):
    # The two knight move orders reach the same board. At the next White
    # decision every named move is below 20, while the Black reply Nf6 clears it.
    games = [
        (23, "e4 e5 Nf3 Nc6 Bb5", "white_wins"),
        (12, "e4 e5 Nc3 Nf6 d3", "black_wins"),
        (12, "Nc3 e5 e4 Nf6 Bc4", "draws"),
        (2, "e4 e5 Nc3 Nf6", "draws"),
        (12, "e4 c5 Nf3 d6", "white_wins"),
        (12, "d4 d5 c4", "white_wins"),
        (11, "d4 Nf6 c4", "black_wins"),
        (2, "d4", "draws"),
    ]
    if colour == "black":
        # d4 is reached again exactly at the archive horizon. Its next move
        # contributes three censored draws and must not inflate the move count.
        games.append((3, "Nf3 Nf6 Ng1 Ng8 d4 d5", "draws"))
    raw, ended = [], []
    for n, line, result in games:
        b = chess.Board()
        counts = {c: n if c in (result, "total") else 0 for c in lookup.SUMS}
        for ply, san in enumerate(line.split(), start=1):
            epd, h = b.epd(), zobrist_int64(b)
            b.push_san(san)
            raw.append(
                dict(
                    parent_hash=h,
                    parent_epd=epd,
                    child_hash=zobrist_int64(b),
                    move_san=san,
                    ply=ply,
                    **counts,
                )
            )
        ended.append(dict(position_hash=zobrist_int64(b), kind=0, reason=1, **counts))
    edges = (
        pl.DataFrame(raw)
        .filter(pl.col("ply") <= 5)
        .group_by("parent_hash", "parent_epd", "child_hash", "move_san")
        .agg(pl.col(c).sum() for c in lookup.SUMS)
    )
    kept = edges.filter(pl.col("total") >= 20).with_columns(
        ((pl.col("white_wins") + 0.5 * pl.col("draws")) / pl.col("total")).alias(
            "white_score_avg"
        )
    )
    stats, aux, rep = (folder / (s + ".parquet") for s in ("stats", "aux", "rep"))
    kept.write_parquet(stats)
    aux_rows = []
    for (h,), rows in edges.partition_by("parent_hash", as_dict=True).items():
        if rows["total"].max() < 20:
            continue
        small = rows.filter(pl.col("total") < 20)
        row = {"position_hash": h, "other_edges": small.height, "other_eval_cov": 0.0}
        row.update({"other_" + c: small[c].sum() for c in lookup.SUMS})
        row.update({"other_eval_" + c: None for c in ("mean", "min", "max")})
        for g in lookup.GROUPS:
            for c in lookup.SUMS:
                row[g + "_" + c] = sum(
                    e[c]
                    for e in ended
                    if e["position_hash"] == h
                    and g == ("term_flag" if colour == "white" else "term_other")
                )
        aux_rows.append(row)
    pl.DataFrame(aux_rows).write_parquet(aux)
    lookup.atomic_json(
        Path(str(aux) + ".meta.json"),
        {"term_reasons": "split" if colour == "white" else "pooled"},
    )
    if colour == "white":
        for i in range(2):
            pl.DataFrame(raw[i::2]).write_parquet(
                folder / f"year=2026_month={i + 1}.ps.parquet"
            )
            pl.DataFrame(ended[i::2]).write_parquet(
                folder / f"year=2026_month={i + 1}.term.parquet"
            )
        meta = {
            "producer": "build_pooled_stats",
            "monthly_dir": str(folder),
            "population": {"month_list": ["2026_1", "2026_2"], "n_months": 2},
        }
    else:
        bucket = folder / "ps/event=Rapid/elo_band=2000/bkt000.parquet"
        bucket.parent.mkdir(parents=True)
        pl.DataFrame(raw).write_parquet(bucket)
        (folder / "_BOOK.DONE").touch()
        pl.DataFrame(
            [
                {
                    "event": "Rapid",
                    "elo_band": 2000,
                    "files": 1,
                    "bytes": bucket.stat().st_size,
                }
            ]
        ).write_parquet(folder / "_slices.parquet")
        pl.DataFrame(schema={"parent_hash": pl.Int64}).write_parquet(
            folder / "_collisions.parquet"
        )
        lookup.atomic_json(
            folder / "_book.meta.json", {"input": {"params": {"buckets": 1}}}
        )
        meta = {
            "producer": "pool_from_book",
            "book": str(folder),
            "max_ply": 5,
            "book_meta_sha256": lookup.digest(folder / "_book.meta.json"),
            "collisions_sha256": lookup.digest(folder / "_collisions.parquet"),
            "population": {"events": ["Rapid"], "elo_bands": [2000]},
        }
    lookup.atomic_json(Path(str(stats) + ".meta.json"), dict(meta, min_games=20))
    source = [
        {
            "position_hash": r["parent_hash"],
            "side_to_move": "white" if r["parent_epd"].split()[1] == "w" else "black",
            "best_move": r["move_san"],
            "event": "Pooled",
            "elo_band": 0,
            "value": 0.5,
            "value_robust": 0.5,
            "value_worst": 0.5,
            "eval_score": 0.5,
        }
        for r in kept.unique("parent_hash").to_dicts()
    ]
    pl.DataFrame(source).write_parquet(rep)
    return stats, aux, rep, edges


class TestUncut(unittest.TestCase):
    def verify(self, folder, colour):
        stats, aux, rep, edges = fixture(folder, colour)
        out = folder / "cache"
        done = lookup.prepare(stats, aux, colour, out, memory="256MB")
        self.assertEqual(done, lookup.prepare(stats, aux, colour, out, memory="256MB"))
        with self.assertRaisesRegex(ValueError, "different pool"):
            lookup.load(
                out, stats, aux, "black" if colour == "white" else "white", None
            )
        new = pl.read_parquet(out / "graph.parquet")
        aux_new = pl.read_parquet(out / "aux.parquet")
        enemy = " b " if colour == "white" else " w "
        old = pl.read_parquet(stats)

        def enemy_moves(df):
            return (
                df.filter(pl.col("parent_epd").str.contains(enemy))
                .select(old.columns)
                .sort("parent_hash", "move_san")
            )

        self.assertEqual(enemy_moves(old).rows(), enemy_moves(new).rows())
        own = key("e4 e5 Nc3 Nf6" if colour == "white" else "d4")
        self.assertFalse(old.filter(pl.col("parent_hash") == own).height)
        self.assertEqual(
            aux_new.filter(pl.col("position_hash") == own)[
                "term_" + ("flag" if colour == "white" else "other") + "_draws"
            ].item(),
            2,
        )
        with duckdb.connect() as con:
            df = builder.collect_subgraph_edges(
                con,
                (out / "graph.parquet").as_posix(),
                key(""),
                colour == "white",
                0.0005,
                5,
                0,
                20,
                uncut_own=True,
            )

        def moves(h):
            return dict(
                df.filter(pl.col("parent_hash") == h)
                .select("move_san", "total")
                .iter_rows()
            )

        if colour == "white":
            self.assertEqual(moves(own), {"d3": 12, "Bc4": 12})
            self.assertEqual(moves(key(""))["Nc3"], 12)
            self.assertEqual(moves(key("e4 e5"))["Nc3"], 14)
        else:
            self.assertEqual(moves(own), {"d5": 12, "Nf6": 11})
            self.assertEqual(moves(key("e4"))["c5"], 12)
            self.assertEqual(
                aux_new.filter(pl.col("position_hash") == own)["horizon_draws"].item(),
                3,
            )
        for h in (own, key("" if colour == "white" else "e4")):
            self.assertEqual(
                aux_new.filter(pl.col("position_hash") == h)["other_total"].item(), 0
            )
        # Run the production budget entry point and its actual two gate passes.
        # Supply deterministic evals for the miniature graph, independent of D:.
        hashes = np.array(
            sorted(set(edges["parent_hash"]) | set(edges["child_hash"])), dtype=np.int64
        )
        arrays = (hashes, np.zeros(len(hashes), dtype=np.int16))
        args = [
            "builder",
            "--rep",
            str(rep),
            "--stats",
            str(stats),
            "--aux-stats",
            str(aux),
            "--perspective",
            colour,
            "--uncut-own-moves",
            str(out),
            "--out-prefix",
            str(folder / "book"),
            "--budget",
            "3",
            "--epsilon",
            ".0005",
            "--max-ply",
            "5",
            "--tmp-dir",
            str(folder / "temp"),
            "--gated-reach",
            "--max-eval-loss",
            "30",
            "--threads",
            "1",
            # the weak-player stop model is the only one since 2026-11-12; any table of
            # parent_hash, child_hash and total serves the miniature graph
            "--weak-pool",
            str(stats),
        ]
        with (
            patch.object(sys, "argv", args),
            patch("eval_arrays.open_eval_arrays", return_value=arrays),
        ):
            self.assertEqual(builder.main(), 0)
        book = folder / "book_b3.parquet"
        self.assertTrue(pl.read_parquet(book).height)
        self.assertEqual(
            lookup.read_json(str(book) + ".meta.json")["uncut_own_recipe"],
            done["recipe_sha256"],
        )

    def test_both_sources(self):
        for colour in ("white", "black"):
            with (
                self.subTest(colour=colour),
                tempfile.TemporaryDirectory(dir=Path(__file__).parent) as path,
            ):
                self.verify(Path(path), colour)

    def test_sliced_merge_is_the_same_lookup(self):
        # The cross-month merge built in parent_hash slices, each in its own process, must give
        # byte-identical tables to the single GROUP BY.
        for colour in ("white", "black"):
            with (
                self.subTest(colour=colour),
                tempfile.TemporaryDirectory(dir=Path(__file__).parent) as path,
            ):
                folder = Path(path)
                stats, aux, _, _ = fixture(folder, colour)
                lookup.prepare(stats, aux, colour, folder / "one", memory="256MB")
                lookup.prepare(stats, aux, colour, folder / "sliced", memory="256MB",
                               merge_slices=4, merge_workers=2, merge_memory="256MB")
                for name in ("graph.parquet", "aux.parquet"):
                    one = pl.read_parquet(folder / "one" / name)
                    sliced = pl.read_parquet(folder / "sliced" / name)
                    self.assertGreater(one.height, 0)
                    self.assertTrue(one.equals(sliced), name)

    def test_outcome_mismatch(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as path:
            folder = Path(path)
            stats, aux, _, _ = fixture(folder, "white")
            path = folder / "year=2026_month=1.ps.parquet"
            pl.read_parquet(path).filter(pl.col("move_san") != "Nc3").write_parquet(
                path
            )
            with self.assertRaisesRegex(ValueError, "native Other"):
                lookup.prepare(stats, aux, "white", folder / "cache", memory="256MB")
            self.assertFalse((folder / "cache/ready.json").exists())

    def test_interrupted_months_resume(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as path:
            folder = Path(path)
            stats, aux, _, _ = fixture(folder, "white")
            cache = folder / "cache"
            run_part = lookup.partial

            def interrupt(con, files, terms, cap, target, wide=False):
                if target.name == "part-0001":
                    raise InterruptedError("second month")
                run_part(con, files, terms, cap, target, wide)

            with (
                patch.object(lookup, "partial", side_effect=interrupt),
                self.assertRaises(InterruptedError),
            ):
                lookup.prepare(stats, aux, "white", cache, memory="256MB")
            self.assertFalse((cache / "ready.json").exists())
            first = cache / "parts/part-0000.moves.parquet"
            before = first.stat().st_mtime_ns
            lookup.prepare(stats, aux, "white", cache, memory="256MB")
            self.assertEqual(first.stat().st_mtime_ns, before)
            self.assertTrue((cache / "ready.json").exists())


if __name__ == "__main__":
    unittest.main()
