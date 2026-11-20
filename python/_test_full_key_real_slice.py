"""Bounded cap-30 parity check on the same real games and cached engine evaluations.

Pass one Lichess source Parquet with the side_explorer schema. This deliberately
uses the same pool as the weak player in both builds to isolate key migration.
"""
import argparse
import gc
import tempfile
from pathlib import Path

import chess
import numpy as np
import polars as pl

import build_budget_books as builder
import side_explorer as side
from _test_full_key_book import add_scores, run
from eval_arrays import MISSING, lookup_evals, open_eval_arrays
from eval_lookup128 import SCHEMA
from month_archive import sha, write_json
from stage1_extract_positions import iter_san_moves
from zobrist128 import zobrist128


def check(raw, folder):
    cols = ["movetext", "white_elo", "black_elo", "white_score", "white_title",
            "black_title", "termination", "game_id"]
    games = (pl.scan_parquet(raw).select(cols).filter(
        (pl.col("white_elo") >= 1800) & (pl.col("black_elo") >= 1800)
        & pl.col("white_score").is_not_null() & ~pl.col("termination").is_in(list(side.EXCLUDED_TERMS))
        & (pl.col("white_title").fill_null("") != "BOT") & (pl.col("black_title").fill_null("") != "BOT"))
        .head(1000).collect())
    source = folder / "games.parquet"
    games.write_parquet(source)
    boards = {}
    for line in games["movetext"]:
        b = chess.Board()
        for ply, san in enumerate(iter_san_moves(line), 1):
            if ply > 30:
                break
            lo, hi = zobrist128(b)
            epd = b.epd(en_passant="xfen")
            if lo in boards and boards[lo] != (hi, epd):
                raise ValueError("legacy keys are ambiguous on this comparison slice")
            boards[lo] = hi, epd
            b.push_san(san)
    args = (str(source), False, 1800, 10000, 1800, 30, 1., 4)
    tables = [side.one_file((*args, mode)) for mode in (False, True)]
    for i, names in enumerate((("parent_hash","child_hash"), ("position_hash",))):
        old, new = (t[i] for t in tables)
        low = new.with_columns(pl.col(c).struct.field("lo").alias(c) for c in names)
        assert low.sort(low.columns).equals(old.sort(old.columns))
    assert tables[0][2:] == tables[1][2:] == (games.height, 0)
    lookup = folder / "lookup"
    lookup.mkdir()
    write_json(lookup/"lookup.json", {"schema": SCHEMA, "buckets": {}})
    wide = tables[1][0]
    keys = ["parent_hash", "move_san", "child_hash"]
    counts = ["white_wins", "draws", "black_wins", "total"]
    wide = (wide.group_by(keys).agg(pl.col(counts).sum()).filter(pl.col("total") >= 3)
            .sort(keys).with_columns(((pl.col("white_wins") + .5*pl.col("draws"))/pl.col("total")).alias("white_score_avg")))
    wide = wide.with_columns(pl.Series("parent_epd", [boards[p["lo"]][1] for p in wide["parent_hash"]]))
    pairs = {tuple(p.values()) for col in ("parent_hash", "child_hash") for p in wide[col]}
    assert len({lo for lo, _ in pairs}) == len(pairs)
    pairs = sorted(pairs)
    actual = lookup_evals(np.array([k[0] for k in pairs], np.int64), *open_eval_arrays())
    scores = {k: int(cp) for k, cp in zip(pairs, actual) if cp != MISSING}
    add_scores(lookup, scores)
    narrow = wide.with_columns(pl.col(c).struct.field("lo").alias(c) for c in ("parent_hash","child_hash"))
    source_book = (narrow.sort(["total","move_san"],descending=[True,False]).unique("parent_hash",keep="first")
                   .select(pl.col("parent_hash").alias("position_hash"), pl.col("parent_epd").alias("position_epd"),
                           pl.col("move_san").alias("best_move")).with_columns(
                       pl.when(pl.col("position_epd").str.contains(" w ")).then(pl.lit("white")).otherwise(
                           pl.lit("black")).alias("side_to_move"), pl.lit("Rapid").alias("event"),
                       pl.lit(1800).alias("elo_band"), *(pl.lit(.5).alias(c) for c in
                                                        ("value","value_robust","value_worst","eval_score"))))
    rep = folder/"rep.parquet"
    source_book.write_parquet(rep)
    books = []
    for mode, edges in (("legacy", narrow), ("wide", wide)):
        stats = folder/f"{mode}.parquet"
        edges.write_parquet(stats)
        if mode == "wide":
            write_json(Path(str(stats)+".meta.json"), {"key_bits":128, "eval_lookup128":str(lookup),
                       "eval_lookup_sha256":sha(lookup/"lookup.json"), "min_games":3})
        prefix = folder/(mode+"-black")
        run(builder.main,"--rep",rep,"--stats",stats,"--weak-pool",stats,"--weak-min-games",3,
            "--perspective","black","--budget",30,"--min-games",3,"--gated-reach","--max-eval-loss",30,
            "--epsilon",.0001,"--max-ply",30,"--out-prefix",prefix,"--tmp-dir",folder)
        book = pl.read_parquet(str(prefix)+"_b30.parquet")
        if mode == "wide":
            book = book.with_columns(pl.col("position_hash").struct.field("lo").alias("position_hash"))
        books.append(book.sort("position_hash"))
    from polars.testing import assert_frame_equal
    assert_frame_equal(books[0].drop("value"), books[1].drop("value"), check_exact=True)
    # Different parallel reductions can change the last bit of a backed-up expectation.
    # Check every prescription and allocation exactly and report the observed score delta.
    assert_frame_equal(books[0].select("value"), books[1].select("value"), rel_tol=0., abs_tol=2e-15)
    delta = (books[0]["value"] - books[1]["value"]).abs().max()
    print(f"Matched {games.height} games, {tables[0][0].height} folded edge rows, "
          f"{wide.height} retained edges, {len(scores)}/{len(pairs)} evals, "
          f"{books[0]['best_move'].drop_nulls().len()} prescriptions and all {books[0].height} book rows; "
          f"largest backed-up score difference {delta:.3g}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, nargs="?",
                        help="opt-in: without it (as under run_tests.py) the check is skipped")
    args = parser.parse_args()
    if args.source is None:
        print("SKIP: pass one Lichess source Parquet to run this real-slice parity check")
        raise SystemExit(0)
    with tempfile.TemporaryDirectory() as tmp:
        check(args.source, Path(tmp))
        gc.collect()
