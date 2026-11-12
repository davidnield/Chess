"""Exercise both sides of the loss gate, verification provenance, restart and convergence."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import chess
import chess.engine
import duckdb
import numpy as np
import polars as pl

import budget_core as bc
import cloud_feedback as fb
import engine_candidates as ec
import hybrid_candidates as hybrid
from cloud_cutoff_check import run as spot_check
from validate_book import classify
from zobrist import zobrist_int64


class Engine:
    id = {"name":"test Stockfish"}
    def configure(self, options):
        pass
    def quit(self):
        pass
    def analyse(self, board, limit, root_moves=None):
        chosen = root_moves[0] if root_moves else board.parse_san("d4" if board.turn else "c5")
        bad = board.san(chosen) in ("e4", "e5")
        score = -95 if bad else 20
        return {"pv":[chosen], "score":chess.engine.PovScore(chess.engine.Cp(score), board.turn),
                "depth":25, "nodes":limit.nodes}


with TemporaryDirectory() as tmp:
    folder = Path(tmp)
    binary = folder / "engine"
    binary.write_text("unit test")
    for colour in ["white","black"]:
        parent = folder / colour
        parent.mkdir()
        b = chess.Board()
        if colour == "black":
            b.push_san("e4")
        h, epd = zobrist_int64(b), b.epd()
        bad, best = ("e4","d4") if b.turn else ("e5","c5")
        limit = 60 if b.turn else 30
        def child(san):
            b.push_san(san)
            key = zobrist_int64(b)
            b.pop()
            return key
        chosen = child(bad)
        props = parent / "input.parquet"
        pl.DataFrame([(h,epd,bad,chosen,0,2_000_000,24,hybrid.CLOUD,"test","unknown")],
                     schema=ec.SCHEMA,orient="row").write_parquet(props)
        book = parent / "book.parquet"
        pl.DataFrame({"position_hash":[h],"position_epd":[epd],"side_to_move":[colour],"best_move":[bad]}).write_parquet(book)
        src = parent / "source.parquet"
        pl.read_parquet(book).write_parquet(src)
        stats = parent / "stats.parquet"
        pl.DataFrame({"parent_hash":[h,h],"child_hash":[chosen,child(best)],"move_san":[bad,best],"total":[50,100]}).write_parquet(stats)
        meta = {"stats":str(stats),"source_rep":str(src),"min_games":50,"max_eval_loss":limit,
                "engine_candidates_sha256":hybrid.sha(props),"slice_prior":.5}
        def save_meta():
            Path(str(book)+".meta.json").write_text(json.dumps(meta))
        save_meta()
        out = parent / "out.parquet"
        with patch.object(ec,"SF",str(binary)), patch.object(chess.engine.SimpleEngine,"popen_uci",return_value=Engine()), \
             patch.object(fb,"open_eval_arrays",return_value=()), patch.object(fb,"lookup_evals",side_effect=lambda keys:np.zeros(len(keys))):
            fb.run(book, props, colour, parent/"cache", out)
            frame = pl.read_parquet(out)
            badrow = frame.filter(pl.col("cloud_origin")).row(0,named=True)
            assert badrow["move_san"] == bad and badrow["verified_loss_cp"] == 115
            signed = 1 if b.turn else -1
            assert badrow["parent_cp_white"] == 20*signed and badrow["child_cp_white"] == -95*signed
            ev = {}
            nominations, _ = bc.load_engine_candidates(out, ev)
            excluded = bc.verified_exclusions(out, limit)
            recorded = [{"move_san":bad,"child_hash":chosen,"total":50,"white_score_avg":.5}]
            assert classify(h,bad,recorded,bad,ev,b.turn,meta,nominations,epd,excluded) == "outside_set"
            assert classify(h,best,recorded,bad,ev,b.turn,meta,nominations,epd,excluded) == "in_set"
            # Walk through the two production optimizers with the same score overlay.
            start = chess.Board()
            root_hash = zobrist_int64(start)
            edges = {h:[dict(recorded[0], parent_epd=epd, white_score_avg=.95 if b.turn else .05)]}
            if not b.turn:
                edges[root_hash] = [dict(move_san="e4", child_hash=h, total=100, white_score_avg=.5,
                                         parent_epd=start.epd())]
            graph = bc.build_graph(edges, root_hash, b.turn, rep_moves={h:bad}, eval_ws=ev,
                engine_moves=nominations, excluded=excluded, strict_source=True, gated_reach=True,
                max_eval_loss=limit, prior_strength=1)
            graph.our[h].l_node = .1
            curves, _ = bc.build_curves(graph, bmax=1)
            assert bc.extract_book(graph, curves, 1)["booked"].get(h) == best
            assert bc.price_search(graph, 1, log=lambda m:None)[0]["booked"].get(h) == best
            # A fresh local score for a transposing child wins even if a cloud row occurs last.
            mixed = pl.concat([frame, fb.normalize(pl.read_parquet(props)).with_columns(pl.lit(99,dtype=pl.Int32).alias("child_cp_white"))])
            trans = parent / "transposition.parquet"
            mixed.write_parquet(trans)
            values = {}
            bc.load_engine_candidates(trans,values)
            assert round(bc.ws_to_cp(values[chosen],True)) == -95*signed
            pl.read_parquet(book).with_columns(pl.lit(best).alias("best_move")).write_parquet(book)
            meta["engine_candidates_sha256"] = hybrid.sha(out)
            save_meta()
            final = parent / "stable.parquet"
            with patch.object(fb,"score_moves",side_effect=AssertionError("unnecessary repeat")):
                fb.run(book, out, colour, parent/"cache", final)
            assert hybrid.sha(final) == hybrid.sha(out)
            # Below-floor nomination in the real indexed DB format; explicit old floor still works.
            bucket = h % 512
            board_move = b.parse_san(bad).uci()
            indexed = pl.DataFrame({"position_hash":[h],"epd":[epd],"source":["cloud"],"fishnet_disagrees":[False],
                "hash_ambiguous":[False],"cloud_line":[board_move],"cloud_cp":[0],"cloud_mate":[None],
                "cloud_depth":[22],"cloud_knodes":[505]})
            indexed.write_parquet(parent / f"bkt{bucket:03d}.parquet")
            target = pl.DataFrame({"position_hash":[h],"epd":[epd],"status":["unknown"]})
            with duckdb.connect() as con:
                hits = hybrid.cloud_bucket(con,target,parent,bucket,colour,"test")
                assert hits["nodes"].to_list() == [505000]
                assert hybrid.cloud_bucket(con,target,parent,bucket,colour,"test",10000).is_empty()
            if colour == "white":
                import cloud_cutoff_check as ck
                with patch.object(ck,"SF",str(binary)):
                    spot_check(hits,parent/"study",colour,limit,size=1)
                report = json.loads((parent/"study/summary.json").read_text())
                assert report["below"]["prevented"] == 1 and report["above"]["n"] == 0
print("PASS cloud cutoff, verification, score feedback, restored-source veto and stable reselection")
