"""Planted collision through the production pool, gated Black builder and ply-30 referee."""
import gc
import json
import sys
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import chess
import duckdb
import numpy as np
import polars as pl

import build_budget_books as builder
import consumer_keys
import convertibility_eval as cv
import pool_from_book as pool
import side_explorer as side
import uncut_own_moves as uncut
from _test_pool128 import TWINS, board, key, make_inputs
from eval_lookup128 import CLOUD, ROW
from month_archive import sha, write_json
from weak_stop import uniform_score
from zobrist128 import zobrist128


def pair(k):
    return dict(zip(("lo", "hi"), k))


def run(main, *args):
    with patch.object(sys, "argv", [main.__module__, *map(str, args)]):
        code = main()
    if code:
        raise AssertionError(f"{main.__module__} returned {code}")


def add_scores(lookup, scores):
    manifest = json.loads((lookup / "lookup.json").read_text())
    shards = defaultdict(list)
    for (lo, hi), cp in scores.items():
        shards[lo % 512].append((lo, hi, cp, CLOUD))
    for bucket in set(shards) | {1}:
        dest = lookup / f"bkt{bucket:03d}.npy"
        rows = sorted(shards[bucket])
        np.save(dest, np.array(rows, dtype=ROW))
        manifest["buckets"][dest.name] = {"rows": len(rows), "sha256": sha(dest)}
    write_json(lookup / "lookup.json", manifest)


def make_holdout(folder, wide):
    folder.mkdir()
    src = folder / "games.parquet"
    rows = [dict(game_id=str(i), movetext=line, white_score=0.5, white_elo=1850,
                 black_elo=1830, white_title="", black_title="", termination="Normal")
            for i, line in enumerate(["1. e4 c5"] * 3 + ["1. d4 Nf6"])]
    pl.DataFrame(rows).write_parquet(src)
    parts = folder / "parts"
    parts.mkdir()
    args = (str(src), False, 1000, 2200, 1500, 30, 1.0, 2, wide)
    side.file_to_part((args, str(parts / "p0")))
    target = folder / "month=1/bkt=0"
    target.mkdir(parents=True)
    _, _, games = side.merge_parts(parts, target / "moves.parquet", folder / "term.parquet",
                                   str(folder), "1GB", wide)
    assert games == len(rows)
    write_json(folder / "_explorer_params.json", {"folds": 2, "fold_rule": side.FOLD_RULE,
               "sample_rule": side.SAMPLE_RULE, "sample": 1., **({"key_bits": 128} if wide else {})})


LINES = {
    "e4 e5 Nf3 Nc6 Bb5 a6 Ba4 Nf6 O-O Be7 Re1 b5 Bb3 d6 c3 O-O h3 Nb8 d4 Nbd7 Nbd2 Bb7 Bc2 Re8 Nf1 Bf8 Ng3 g6 a4 c5": -100,
    "d4 d5 c4 e6 Nc3 Nf6 Bg5 Be7 e3 O-O Nf3 Nbd7 Bd3 dxc4 Bxc4 c6 O-O b5 Bd3 Bb7 Qe2 a6 e4 c5 e5 Nd5 Bxe7 Qxe7 Nxd5 Bxd5": -200,
    "d4 Nf6 c4 e6 Nc3 Bb4 e3 O-O Bd3 d5 Nf3 c5 O-O Nc6 a3 Bxc3 bxc3 dxc4 Bxc4 Qc7 Bd3 e5 Nxe5 Nxe5 dxe5 Qxe5 Qc2 b6 Bb2 Bb7": 300,
}


class BookTest(unittest.TestCase):
    def test_build_and_score(self):
        # Only the first-ply d4 board is injected. Rust independently hashes each
        # subsequent played move, including the scored boards at the horizon.
        e4, d4 = (zobrist128(board(s)) for s in ("e4", "d4"))
        twins = {board("d4").epd(): (e4[0], d4[1])}
        with tempfile.TemporaryDirectory() as td, patch.dict(TWINS, twins, clear=True), \
                patch.object(consumer_keys, "zobrist128", key), patch("zobrist128.zobrist128", key):
            self.exercise(Path(td))
            gc.collect()

    def test_score_gates_with_lookahead(self):
        e4, d4 = (zobrist128(board(s)) for s in ("e4","d4"))
        twins = {board("d4").epd(): (e4[0], d4[1])}
        with tempfile.TemporaryDirectory() as td, patch.dict(TWINS, twins, clear=True), \
                patch.object(consumer_keys, "zobrist128", key), patch("zobrist128.zobrist128", key):
            self.exercise(Path(td), modern=True)
            gc.collect()

    def exercise(self, path, modern=False):
        book, lookup = make_inputs(path)
        cps = {"": 0, "e4": 0, "d4": 0, "e4 e5": -100, "d4 d5": -200,
               "e4 c5": 400, "d4 Nf6": 300, "e4 e5 Nf3": -100, "d4 d5 c4": -200, **LINES}
        answers = {board("").epd(): {"e4": .75, "d4": .25}, board("d4").epd(): {"Nf6": 1.},
                   board("e4").epd(): {"c5": 1.}}
        for line in LINES:
            b = board("")
            self.assertEqual(len(line.split()), 30)
            for i, san in enumerate(line.split()):
                if i >= 2:
                    answers[b.epd()] = {san: 1.}
                b.push_san(san)
        scores = {key(board(s)): cp for s, cp in cps.items()}
        for line in LINES:
            lo, hi = key(board(line))
            scores[(lo, hi ^ 1)] = 900
        add_scores(lookup, scores)
        with patch.object(pool, "N_BUCKETS", 2), patch.object(pool, "GROUP_SIZE", 1), \
                patch.object(pool, "_pool_run", lambda fn, tasks, *args: [fn(t) for t in tasks]):
            args = ["--book", book, "--out-dir", path, "--work", path/"poolwork", "--eval-lookup128",
                    lookup, "--events", "Rapid", "--elo-bands", "1800", "--max-ply", 5,
                    "--min-games", 5, "--tag", "e2e", "--tmp-dir", path/"spill"]
            run(pool.main, *args)
            stats, aux = pool.out_paths(pool.build_parser().parse_args(list(map(str, args))))
        uncut.prepare(stats, aux, "black", path/"uncut", merge_slices=2)
        source = path / "source.parquet"
        pl.DataFrame([dict(position_hash=pair(key(board(s))), position_epd=board(s).epd(),
                           side_to_move="black", best_move=m, value=.5, value_robust=.5,
                           value_worst=.5, eval_score=.5, event="Rapid", elo_band=1800)
                      for s, m in [("e4", "e5"), ("d4", "d5")]]).write_parquet(source)
        if modern:
            # Independence means the new search does not inherit a legacy prescription menu.
            source.unlink()
            from sweep.builder import empty_source
            empty_source(source, prior=.5195669627673518)
        weak = path / "weak.parquet"
        pl.DataFrame([dict(parent_hash=pair(key(board(s))), child_hash=pair(key(board(s+" "+m))),
                           total=100) for s,m in [("e4", "c5"), ("d4", "Nf6")]]).write_parquet(weak)
        run(builder.main, "--stats", stats, "--aux-stats", aux, "--weak-pool", weak,
            "--weak-min-games", 1, "--rep", source, "--out-prefix", path/"black",
            "--budget", 2, "--max-ply", 2, "--epsilon", .0001, "--eval-weight", 1,
                        "--gated-reach", *(["--max-score-loss", .06, "--robustness-floor", .05,
                                "--lookahead-floor", 2, "--slice-prior", .5195669627673518]
                               if modern else ["--max-eval-loss", 30]),
            "--uncut-own-moves", path/"uncut",
            "--perspective", "black", "--tmp-dir", path/"spill")
        built = path / "black_b2.parquet"
        frame = pl.read_parquet(built)
        booked = frame.filter(pl.col("best_move").is_not_null())
        self.assertEqual({(p["lo"], p["hi"]): m for p,m in booked.select("position_hash","best_move").iter_rows()},
                         {key(board("e4")): "e5", key(board("d4")): "d5"})
        self.assertEqual(json.loads(Path(str(built)+".meta.json").read_text())["eval_lookup_sha256"],
                         sha(lookup/"lookup.json"))
        if modern:
            import lookahead_audit
            meta = json.loads(Path(str(built)+".meta.json").read_text())
            proof = pl.read_parquet(Path(str(built)+".lookahead-inputs.parquet"))
            self.assertEqual(proof.schema["parent"], consumer_keys.PAIR)
            self.assertEqual(proof.schema["child"], consumer_keys.PAIR)
            report = lookahead_audit.audit(built, meta, check_receipt=False)
            self.assertEqual(report["checked_decisions"], 2)
            allowed = path/"allowed.parquet"
            frame.filter(pl.col("best_move")=="e5").write_parquet(allowed)
            run(builder.main, "--stats", stats, "--aux-stats", aux, "--weak-pool", weak,
                "--weak-min-games", 1, "--rep", source, "--out-prefix", path/"reduced",
                "--budget", 1, "--max-ply", 2, "--epsilon", .0001, "--eval-weight", 1,
                "--gated-reach", "--max-score-loss", .06, "--robustness-floor", .05,
                "--slice-prior", .5195669627673518, "--lookahead-floor", 2,
                "--allowed-book", allowed, "--perspective", "black", "--tmp-dir", path/"spill")
            reduced=pl.read_parquet(path/"reduced_b1.parquet")
            self.assertEqual(reduced["best_move"].drop_nulls().to_list(), ["e5"])

        changed = path / "changed.parquet"
        frame.with_columns(pl.when(pl.col("best_move") == "d5")
                           .then(pl.lit(pair((key(board("d4"))[0], 0)), dtype=consumer_keys.PAIR))
                           .otherwise(pl.col("position_hash")).alias("position_hash")).write_parquet(changed)
        hold, legacy = path/"hold", path/"legacy"
        make_holdout(hold, True)
        make_holdout(legacy, False)
        positions = consumer_keys.ConsumerKeys(lookup)
        ids = [positions.board(board(s)) for s in ("e4","d4")]
        with duckdb.connect() as con:
            counts = cv.Counts(con, hold.as_posix(), "hold", positions=positions)
            self.assertEqual(counts.get(ids), [{"c5":3}, {"Nf6":1}])
            for rep in (1, 2, 3):
                e4, d4 = counts.get(ids, rep)
                self.assertTrue(set(e4) <= {"c5"} and set(d4) <= {"Nf6"})
            with self.assertRaisesRegex(ValueError, "full-key holdout"):
                cv.Counts(con, legacy.as_posix(), "old", positions=positions)
            args = SimpleNamespace(position_keys=positions, rollouts=64, max_ply=2,
                                   recall=.5, k=1., min_p=.001, our_elo=1450, opp_elo=2000)
            starts = [(h, board(s).epd(), .5, 1, 0, 0., 1.) for h,s in zip(ids, ("e4","d4"))]
            book_moves = dict(zip(ids, ("e5", "d5")))
            model = SimpleNamespace(probs=lambda epds, *args: [answers[e] for e in epds])
            def play(fn):
                leaves, mass = fn(starts, book_moves, False, 0, args, 1, counts, counts, model, positions.scores)
                return sorted((s,k,h) for _,h,_,_,_,_,_,_,s,k in leaves), mass
            native = play(cv.rollouts)
            self.assertEqual(native, play(cv.rollouts_py))
            outcomes = [h == positions.board(board(("e4 e5", "d4 d5")[s])) for s,k,h in native[0]]
            self.assertNotEqual(outcomes[:64], outcomes[64:])
        cache = cv.MaiaCache(path/"predictions", {}, positions)
        for h,san in zip(ids, ("e5", "d5")):
            cache.put(h, 1450, 2000, {san:1.})
        cache.flush()
        next_run = consumer_keys.ConsumerKeys(lookup)
        reverse = [next_run.board(board(s)) for s in ("d4", "e4")]
        warm = cv.MaiaCache(path/"predictions", {}, next_run)
        self.assertEqual(warm.get(reverse,1450,2000), [{"d5":1.},{"e5":1.}])
        self.assertEqual(warm.get(reverse,1450,2200), [None,None])
        engine = SimpleNamespace(id={"name":"fixture"}, configure=lambda _: None, quit=lambda: None)
        with patch("chess.engine.SimpleEngine.popen_uci", return_value=engine):
            sf = cv.StockfishFill(path/"sf.parquet", 10, 1, positions)
            sf.cache.update(zip(ids, (-100,-200)))
            sf.save()
            sf.close()
            sf = cv.StockfishFill(path/"sf.parquet", 10, 1, next_run)
            self.assertEqual([sf.cache[h] for h in reverse], [-200,-100])
            sf.close()
        # A legacy comparison book carries a board. A mismatching signed low must fail.
        source_low = path/"legacy-source.parquet"
        base = pl.DataFrame({"position_hash":[zobrist128(board("e4"))[0]], "position_epd":[board("e4").epd()]})
        base.write_parquet(source_low)
        self.assertEqual(next_run.book_rows(source_low, ["position_hash"]).item(), reverse[1])
        base.with_columns((pl.col("position_hash")+1)).write_parquet(source_low)
        with self.assertRaisesRegex(ValueError, "disagrees with board"):
            next_run.book_rows(source_low, ["position_hash"])
        pinned = chess.Board("k3r3/8/8/3pP3/8/8/8/4K3 w - d6 0 1")
        self.assertNotEqual(pinned.epd(), pinned.epd(en_passant="xfen"))
        pl.DataFrame({"position_hash":[zobrist128(pinned)[0]], "position_epd":[pinned.epd()]}).write_parquet(source_low)
        self.assertEqual(next_run.book_rows(source_low, ["position_hash"]).item(), next_run.board(pinned))
        curves = path / "curves.parquet"
        pl.DataFrame({"cell": ["stm1000-1799_vs_opp1800+", "stm1800+_vs_opp1000-1799"],
                      "a": [0.,0.], "k": [.003,.003]}).write_parquet(curves)

        class MaiaFixture:
            def __init__(self, **kwargs): pass
            def close(self): pass
            def probs(self, epds, self_elo, opp_elo, keys):
                return [answers[p] for p in epds]

        def score(label, candidate, tau, counts, extra=()):
            dest, leaves = path / (label+".parquet"), path / (label+"-leaves.parquet")
            run(cv.main, "--book", f"{label}=black={candidate}", "--tau", tau, "--max-ply", 30,
                "--k", 1, "--rollouts", 4, "--stockfish-depth", 0, "--no-maia-cache",
                "--our-moves", counts, "--opp-moves", counts, "--out", dest,
                "--leaves-out", leaves, "--exits-out", path/(label+"-exits"), *extra)
            return pl.read_parquet(dest).row(0, named=True), pl.read_parquet(leaves)

        with patch.object(cv, "Maia", MaiaFixture), patch.object(cv, "CURVES", curves):
            for tau in (0, .9):
                for name, candidate, d4_cp, mass in (("both",built,200,1.), ("altered",changed,-300,.75)):
                    extra = ["--eval-lookup128", lookup] if candidate == changed else []
                    row, leaves = score(f"{name}-{tau}", candidate, tau, hold, extra)
                    self.assertAlmostEqual(row["uniform"], .75*uniform_score(100)+.25*uniform_score(d4_cp))
                    self.assertAlmostEqual(row["book_moves_played"], mass)
                    self.assertEqual(set(leaves["ply"].to_list()), {30})
                    self.assertEqual(set(leaves["cp_us"].to_list()), {100,d4_cp})
                    self.assertEqual(leaves["sampled"].all(), bool(tau))
                    for epd, h in leaves.select("epd","hash").iter_rows():
                        self.assertEqual(pair(key(chess.Board(epd))),h)
                    if tau:
                        self.assertEqual({tuple(p.values()) for p in leaves["start_hash"]},
                                         {key(board("e4")), key(board("d4"))})
            narrow = frame.with_columns(pl.Series("position_hash", [
                zobrist128(chess.Board(epd))[0] for epd in frame["position_epd"]]))
            narrow.write_parquet(path/"narrow.parquet")
            low_scores = {zobrist128(board(line))[0]: cp for line,cp in cps.items()}
            with patch.object(cv, "open_eval_arrays", lambda: ()), patch.object(cv, "lookup_evals",
                    lambda hs: np.array([low_scores.get(h, cv.MISSING) for h in hs], dtype=np.int16)):
                row, _ = score("legacy-score", path/"narrow.parquet", .9, legacy)
            wide = pl.read_parquet(path/"both-0.9.parquet").row(0, named=True)
            for col in ("uniform", "fitted", "book_moves_played", "mean_cp_us"):
                self.assertEqual(row[col], wide[col])


if __name__ == "__main__":
    unittest.main()
