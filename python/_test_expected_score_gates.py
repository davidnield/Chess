"""Regression tests for hard gates from the builder CLI through verification."""
import json
from contextlib import redirect_stderr
from io import StringIO
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

import chess
import numpy as np
import polars as pl

import budget_core as bc
import build_budget_books as builder
import eval_arrays
import rare_verify
import validate_book
from zobrist import zobrist_int64


def test_build(directory, white):
    """Give the failing candidate the best historical results and the source prescription."""
    colour = "white" if white else "black"
    board = chess.Board()
    edges, source, cps = [], [], {}
    def edge(b, san, white_wins, cp):
        child = b.copy()
        child.push_san(san)
        h, k = zobrist_int64(b), zobrist_int64(child)
        edges.append(dict(parent_hash=h, child_hash=k, parent_epd=b.epd(), move_san=san,
                          total=100, white_wins=white_wins, black_wins=100-white_wins,
                          draws=0, white_score_avg=white_wins/100))
        cps[k] = cp
        return child
    cps[zobrist_int64(board)] = 30
    if not white:
        board = edge(board, "f4", 50, -31)
    h = zobrist_int64(board)
    safe, bad = ("e4", "g4") if white else ("e5", "g5")
    # The Black fixture uses the fresh From's evaluation. At White's root, +30 to
    # -63 likewise passes; the tempting -140 alternative fails for either side.
    edge(board, safe, 70 if white else 30, -63 if white else 62)
    edge(board, bad, 100 if white else 0, -140 if white else 140)
    source.append(dict(position_hash=h, side_to_move=colour, best_move=bad, value=0.5,
                       value_robust=0.5, value_worst=0.5, eval_score=0.5, event="test", elo_band="test"))
    pool, rep = directory/"pool.parquet", directory/"rep.parquet"
    pl.DataFrame(edges).write_parquet(pool)
    pl.DataFrame(source).write_parquet(rep)
    keys = sorted(cps)
    arrays = (np.array(keys, np.int64), np.array([cps[k] for k in keys], np.int16))
    prior = 0.518313799 if white else 0.519566963
    cli = ["build_budget_books.py", "--rep", str(rep), "--stats", str(pool), "--budget", "1",
           "--perspective", colour, "--out-prefix", str(directory/"book"), "--slice-prior", str(prior),
           "--weak-pool", str(pool), "--gated-reach", "--max-ply", "2", "--tmp-dir", str(directory),
           "--eval-weight", "0", "--method", "price" if white else "dp"]
    with patch.object(eval_arrays, "open_eval_arrays", return_value=arrays), patch.object(sys, "argv", cli):
        assert builder.main() == 0
    book = directory/"book_b1.parquet"
    meta = json.loads(Path(str(book)+".meta.json").read_text())
    assert meta["max_score_loss"] == .095 and meta["max_eval_loss"] is None
    assert meta["robustness_floor"] == (.110 if white else .075)
    assert meta["absolute_score_floor"] == bc.score_floor_from_meta(meta, white)
    assert pl.read_parquet(book).filter(pl.col("position_hash") == h)["best_move"].item() == safe
    ev = {k:bc.cp_to_expected_score(c) for k,c in cps.items()}
    menu = [e for e in edges if e["parent_hash"] == h]
    for san, expected in [(safe, "in_set"), (bad, "outside_set")]:
        assert validate_book.classify(h, san, menu, bad, ev, white, meta, None, board.epd()) == expected
    # Refuse to call the same prefix complete under a different gate.
    error = StringIO()
    with patch.object(sys, "argv", cli + ["--max-score-loss", "0.06"]), redirect_stderr(error):
        try:
            builder.main()
        except SystemExit as exc:
            assert exc.code == 2
            assert "different evaluation gates" in error.getvalue()
        else:
            raise AssertionError("reused an incompatible score-policy book")
    with (patch.object(eval_arrays, "open_eval_arrays", return_value=arrays),
          patch.object(sys, "argv", cli + ["--max-score-loss", "0", "--force"])):
        assert builder.main() == 0
    assert pl.read_parquet(book)["best_move"].drop_nulls().is_empty()


def test_thresholds(directory):
    for white in (True, False):
        view = lambda v: v if white else 1-v
        def kept(parent, child, floor=.4):
            return bool(bc.gate_candidates(1, [dict(move_san="e4", child_hash=2)],
                        "e4", {1:view(parent), 2:view(child)}, white, view(.5),
                        robustness_floor=.5-floor, max_score_loss=.095))
        assert kept(.495, .4) and not kept(.495, .4-1e-7)
        assert not kept(.495+1e-7, .4)
        assert not kept(.85, .72)
        # Identical 120 cp concessions are treated differently at equality and +6.
        s = bc.cp_to_expected_score
        assert not kept(s(60), s(-60)) and kept(s(600), s(480))
        sign = 1 if white else -1
        pairs = [(60,-60), (600,480), (-125,-125)]
        overlay = directory/"overlay.parquet"
        pl.DataFrame([dict(position_hash=i, move_san="e4", verified_loss_cp=p-c,
                           parent_cp_white=sign*p, child_cp_white=sign*c)
                      for i,(p,c) in enumerate(pairs)]).write_parquet(overlay)
        assert bc.verified_exclusions(overlay, None, max_score_loss=.095, our_white=white,
                                      score_floor=.41) == {0:{"e4"}, 2:{"e4"}}
        b = chess.Board()
        if not white:
            b.push_san("e4")
        saved = dict(epd=b.epd(), free={"cp":sign*60}, moves={
            "best":{"cp":sign*60}, "near":{"cp":sign*(-44)}, "far":{"cp":sign*(-160)}})
        assert rare_verify.boundary(saved, .095, 10, score_units=True, score_floor=.41) == ["near"]
        colour = "white" if white else "black"
        book = pl.DataFrame([dict(position_hash=1, position_epd=b.epd(), side_to_move=colour, best_move="near")])
        meta = dict(max_score_loss=.095, slice_prior=view(.51), robustness_floor=.10, rare_floor=5)
        seed = pl.DataFrame([dict(position_hash=1, move_san="near", parent_cp_white=sign*60,
                            child_cp_white=-sign*44, verification_free_cp_white=sign*60,
                            verification_nodes=5_000_000)])
        menu = {1: [dict(move_san="near", child_hash=2, total=5)]}
        assert rare_verify.plan(book, meta, seed, colour, {}, menu, [])[1][0] == "score gate boundary"
        assert not rare_verify.plan(book, meta, seed.with_columns(pl.lit(20_000_000).alias("verification_nodes")),
                                    colour, {}, menu, [])


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for white in (True, False):
            folder = root/("white" if white else "black")
            folder.mkdir()
            test_build(folder, white)
        test_thresholds(root)
    print("PASS expected-score gates, both CLI builds, validator, edge vetoes and verification")
