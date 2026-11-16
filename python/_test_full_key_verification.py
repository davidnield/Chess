"""Engine verification across two genuine Polyglot collisions, and the Black round coordinator.

Run with --stockfish-smoke to also search the historical boards with the installed engine.
"""
import gc
import io
import json
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import chess
import chess.engine
import polars as pl
from polars.testing import assert_frame_equal

import budget_core as bc
import build_budget_books as builder
import consumer_keys as ck
import engine_candidates as ec
import pool_from_book as pool
import rare_verify as rv
import run_rare_book as driver
import uncut_own_moves as uncut
from _test_full_key_book import add_scores, pair, run
from _test_pool128 import TWINS, board, key, make_inputs
from eval_lookup128 import SCHEMA
from month_archive import sha, write_json
from zobrist128 import zobrist128

# Two Black positions from the 2024-02 and 2024-04 Lichess explorer collision
# manifest. Playing ...h6 also leaves two different boards with the same low.
EPDS = ("1r5r/pppNkppp/4bn2/8/1P2P3/2N2P2/1PPB2PP/R3KB1R b KQ -",
        "r2qr1k1/ppp2ppp/2npb3/3P4/8/2NBP2P/PP1Q2P1/2KR2NR b - -")
PARENTS = [pair(zobrist128(chess.Board(epd))) for epd in EPDS]
MOVES = ("h5", "h6", "g6")
BASE, DEEP = 5_000_000, 20_000_000
REAL = "--stockfish-smoke" in sys.argv
if REAL:
    sys.argv.remove("--stockfish-smoke")


def child(epd, san):
    b = chess.Board(epd)
    b.push_san(san)
    return pair(zobrist128(b))


def read(path):
    return json.loads(path.read_text(encoding="utf8"))


def historical_inputs(root):
    lookup = root / "eval"
    lookup.mkdir()
    write_json(lookup/"lookup.json", {"schema": SCHEMA, "buckets": {}})
    edges, scores = [], {}
    for epd, parent in zip(EPDS, PARENTS):
        for m, n in zip(MOVES, (100, 1, 1)):
            kid = child(epd, m)
            scores[(kid["lo"], kid["hi"])] = -100 if n == 100 else -75
            edges.append(dict(parent_hash=parent, parent_epd=epd, move_san=m,
                              child_hash=kid, total=n, white_score_avg=.5))
    stats = root / "edges.parquet"
    pl.DataFrame(edges).write_parquet(stats)
    add_scores(lookup, scores)
    source = root / "legacy-source.parquet"
    pl.DataFrame({"position_hash": [p["lo"] for p in PARENTS], "position_epd": EPDS,
                  "side_to_move": ["black"]*2, "best_move": ["h5", "h5"]}).write_parquet(source)
    return dict(perspective="black", rare_floor=20, max_eval_loss=30, min_games=1, stats=str(stats),
                source_rep=str(source), key_bits=128, eval_lookup128=str(lookup),
                eval_lookup_sha256=sha(lookup/"lookup.json"))


def book_at(path, meta, moves, prior=None, disputes=()):
    frame = pl.DataFrame({"position_hash":PARENTS, "position_epd":EPDS, "side_to_move":["black"]*2,
                          "best_move":pl.Series(moves, dtype=pl.String)})
    frame.write_parquet(path)
    write_json(Path(str(path)+".meta.json"), {**meta, "engine_candidates":str(prior) if prior else None,
               "engine_candidates_sha256":sha(prior) if prior else None})
    write_json(Path(str(path)+".rare.json"), list(disputes))
    return path


def target_file(root):
    path = root / "targets.parquet"
    pl.DataFrame({"position_hash": PARENTS, "epd": EPDS, "reach":[.7,.3],
                  "colour":["black"]*2, "status":["fallback"]*2}).write_parquet(path)
    return path


class VerificationTest(unittest.TestCase):
    def test_interrupted_and_shared_parent_search(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            binary = root/"engine"
            binary.touch()
            calls = []
            def search(handle, pos, move=None):
                calls.append(move)
                handle.name = "fixture"
                if calls == [None, "g6", "h5"]:
                    raise InterruptedError("test interruption in restricted searches")
                return dict(san=move or "h5", cp=0, depth=8, nodes=5000)
            def request(moves):
                cache = rv.Searches(root/"cache", 5000, 1, wide=True)
                try:
                    return cache.get(PARENTS[0], EPDS[0], moves)
                finally:
                    cache.close()
            with patch.object(ec,"SF",str(binary)), patch.object(rv.Searches,"_one",search):
                with self.assertRaises(InterruptedError):
                    request(["g6","h6"])
                with ThreadPoolExecutor(2) as workers:
                    list(workers.map(request, [["h6"],["a6"]]))
                saved = request(["g6","h5","h6","a6"])
                self.assertEqual(set(saved["moves"]), {"g6","h5","h6","a6"})
                self.assertCountEqual(calls, [None,"g6","h5","h5","h6","a6"])

    def test_proposal_resume(self):
        pinned = chess.Board("k3r3/8/8/3pP3/8/8/8/4K3 w - d6 0 1")
        for wide in (False, True):
            recovered = ck.checked_board(pinned.epd(), ck.stored_key(pinned, wide))
            self.assertEqual(recovered.ep_square, chess.D6)
        self.assertEqual(PARENTS[0]["lo"], PARENTS[1]["lo"])
        self.assertNotEqual(PARENTS[0]["hi"], PARENTS[1]["hi"])
        kids = [child(e, "h6") for e in EPDS]
        self.assertEqual(kids[0]["lo"], kids[1]["lo"])
        self.assertNotEqual(kids[0]["hi"], kids[1]["hi"])
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            targets, out = target_file(root), root/"proposals.parquet"
            args = ("--targets", targets, "--colour", "black", "--nodes", 100, "--threads", 1, "--out", out)
            calls = []
            def analyse(b, limit):
                calls.append(b.epd())
                if len(calls) == 2:
                    raise InterruptedError("test interruption after one completed proposal")
                return {"pv":[b.parse_san("h6")], "score":chess.engine.PovScore(chess.engine.Cp(30), chess.BLACK),
                        "depth":3, "nodes":100}
            fake = SimpleNamespace(id={"name":"fixture"}, analyse=analyse, quit=lambda:None, configure=lambda _:None)
            binary = root/"engine"
            binary.touch()
            with patch.object(ec, "SF", str(binary)), patch.object(chess.engine.SimpleEngine, "popen_uci",
                                                                   return_value=fake) as start:
                with self.assertRaises(InterruptedError):
                    run(ec.main, *args)
                tail = Path(str(out)+".partial.parquet")
                self.assertEqual(pl.read_parquet(tail)["position_hash"].to_list(), PARENTS[:1])
                run(ec.main, *args)
                self.assertEqual(calls, [EPDS[0], EPDS[1], EPDS[1]])
                self.assertFalse(tail.exists())
                first = sha(out)
                run(ec.main, *args)
                self.assertEqual(sha(out), first)
                self.assertEqual(start.call_count, 2)
                rows = pl.read_parquet(out)
                self.assertEqual(rows["position_hash"].to_list(), PARENTS)
                self.assertEqual(rows["child_hash"].to_list(), kids)
                rows.with_columns(pl.lit(kids[0], dtype=ck.PAIR).alias("child_hash")).write_parquet(out)
                with self.assertRaisesRegex(ValueError,"child hash"):
                    run(ec.main, *args)
                split = root/"split.parquet"
                pl.read_parquet(targets).with_columns(
                    pl.col("position_hash").struct.field("lo").alias("position_hash"),
                    pl.col("position_hash").struct.field("hi").alias("position_hash_hi")).write_parquet(split)
                other = root/"split-proposals.parquet"
                run(ec.main,"--targets",split,"--colour","black","--nodes",100,"--threads",1,"--out",other)
                assert_frame_equal(rows, pl.read_parquet(other), check_exact=True)

    def test_verification_rounds(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            meta = historical_inputs(root)
            engine = root/"engine"
            engine.touch()
            cache = root/"searches"
            calls = []
            def search(searches, b, move=None):
                i = EPDS.index(b.epd())
                calls.append((i, searches.nodes, move))
                searches.name = "fixture"
                values = ({"h5":100, "h6":115, "g6":117} if searches.nodes == DEEP else
                          {"h5":100, "h6":70, "g6":117}) if i == 0 else {"h5":100, "h6":40, "g6":-20}
                san = move or "h5"
                score = 116 if i == 0 and move is None else values[san]
                return dict(san=san, cp=-score, depth=10, nodes=searches.nodes)
            def verify(label, moves, prior=None, disputes=(), **options):
                book = book_at(root/f"{label}.parquet", meta, moves, prior, disputes)
                out = root/f"{label}-overlay.parquet"
                rv.run(book, "black", cache, out, BASE, 1, deep_nodes=DEEP, **options)
                return out, read(Path(str(out)+".meta.json"))

            with patch.object(ec,"SF",str(engine)), patch.object(rv.Searches,"_one",search):
                # A legacy search legitimately belongs to the first twin only.
                old = rv.Searches(cache, BASE, 1)
                old.get(PARENTS[0]["lo"], EPDS[0], ["h5","h6"])
                old.close()
                calls.clear()
                o1, r1 = verify("first", ("h6",None))
                self.assertEqual(r1["retried_parents"][0]["position_hash"], PARENTS[0])
                self.assertEqual(r1["searches"]["legacy_parents_reused"], 1)
                self.assertTrue(all(i == 0 and n == DEEP for i,n,_ in calls))
                o2, r2 = verify("second", ("h6","h6"), o1)
                self.assertTrue(any(i == 1 and n == BASE and m == "h6" for i,n,m in calls))
                self.assertEqual(r2["requested_parents"], 1)
                self.assertFalse(r2["retried_parents"])
                rows = pl.read_parquet(o2)
                h6 = {h["hi"]:(loss,n,k) for h,loss,n,k in rows.filter(pl.col("move_san")=="h6").select(
                    "position_hash","verified_loss_cp","verification_nodes","child_hash").iter_rows()}
                self.assertEqual(h6, {PARENTS[0]["hi"]:(0,DEEP,child(EPDS[0],"h6")),
                                      PARENTS[1]["hi"]:(60,BASE,child(EPDS[1],"h6"))})
                # Use the same reducer and veto check as the builder with a fresh registry.
                ids = ck.ConsumerKeys(meta["eval_lookup128"])
                internal = ids.encode(rows, "position_hash", "child_hash")
                buf = io.BytesIO()
                internal.write_parquet(buf)
                veto = bc.verified_exclusions(buf.getvalue(), 30)
                self.assertEqual(veto, {ids.board(chess.Board(EPDS[1])):{"h6"}})
                o3, r3 = verify("new-rival", ("g6","h5"), o2)
                self.assertEqual(r3["retried_parents"][0]["why"], "carried forward")
                self.assertEqual(r3["retried_parents"][0]["position_hash"], PARENTS[0])
                calls.clear()
                repeat, _ = verify("repeat", ("g6","h5"), o2)
                self.assertFalse(calls)
                assert_frame_equal(pl.read_parquet(o3), pl.read_parquet(repeat), check_exact=True)
                closed, report = verify("closed", ("g6","h5"), o3)
                self.assertEqual(report["requested_parents"], 0)
                assert_frame_equal(pl.read_parquet(o3), pl.read_parquet(closed), check_exact=True)
                # The unbooked frontier can name the second twin at the same low.
                dispute = {"position_hash":PARENTS[1], "booked":None, "reach":.3,
                           "threats":[["g6",150]], "at_risk":["h5"]}
                _, front = verify("front", ("h5",None), disputes=[dispute], frontier_reach=.2, frontier=1)
                self.assertEqual(front["by_reason"], {"frontier dispute":1})
                # A copied cache file claiming its twin's key must be rejected.
                def cached(p):
                    return cache/"key128"/f"{p['lo']}_{p['hi']}.json"
                cached(PARENTS[1]).write_bytes(cached(PARENTS[0]).read_bytes())
                with self.assertRaisesRegex(ValueError, "Cached search key"):
                    verify("poisoned", (None,"h6"))
            gc.collect()

    def test_builder_closure_and_resume(self):
        twins = {board("d4").epd():(zobrist128(board("e4"))[0], zobrist128(board("d4"))[1])}
        with tempfile.TemporaryDirectory() as td, patch.dict(TWINS, twins, clear=True), \
                patch.object(ck, "zobrist128", key):
            root = Path(td)
            archive, lookup = make_inputs(root)
            before = {"":0, "e4":0, "d4":0, "e4 c5":-180, "d4 Nf6":-200, "e4 e5":100,
                      "d4 d5":100, "e4 e5 Nf3":100, "d4 d5 c4":100, "e4 g5":700, "d4 a5":700}
            add_scores(lookup, {key(board(s)):cp for s,cp in before.items()})
            with patch.object(pool, "N_BUCKETS", 2), patch.object(pool, "GROUP_SIZE", 1), \
                    patch.object(pool, "_pool_run", lambda fn,tasks,*args:[fn(t) for t in tasks]):
                args = ["--book",archive,"--out-dir",root,"--work",root/"work","--eval-lookup128",lookup,
                        "--events","Rapid","--elo-bands",1800,"--max-ply",5,"--min-games",5,
                        "--tag","rounds","--tmp-dir",root/"spill"]
                run(pool.main,*args)
                stats, aux = pool.out_paths(pool.build_parser().parse_args(list(map(str,args))))
            uncut.prepare(stats, aux, "black", root/"uncut", merge_slices=2)
            rep, weak = root/"source.parquet", root/"weak.parquet"
            pl.DataFrame([dict(position_hash=pair(key(board(s))), position_epd=board(s).epd(),
                               best_move=m, side_to_move="black", value=.5, value_robust=.5,
                               value_worst=.5, eval_score=.5, event="Rapid", elo_band=1800)
                          for s,m in (("e4","c5"), ("d4","Nf6"))]).write_parquet(rep)
            pl.DataFrame([dict(parent_hash=pair(key(board(s))), child_hash=pair(key(board(s+" "+m))),
                               total=100) for s,m in (("e4","g5"),("d4","a5"))]).write_parquet(weak)
            cmd = ["--rep",rep,"--perspective","black","--stats",stats,"--aux-stats",aux,"--weak-pool",weak,
                   "--weak-min-games",1,"--max-ply",2,"--budget",2,"--eval-weight",1,"--epsilon",.0001,
                   "--gated-reach","--max-eval-loss",30,"--tmp-dir",root/"spill","--out-prefix",root/"first"]
            write_json(root/"baseline/stages/black-cp30.json", {"command":list(map(str,cmd))})
            a = driver.parser().parse_args(list(map(str, ["--baseline",root/"baseline","--cap",30,
                "--lookup",root/"uncut","--out",root/"rounds","--searches",root/"cache",
                "--colour","black","--frontier",0,"--threads",1])))
            fresh = {board("e4").epd():("e5", {"e5":-150,"c5":300}),
                     board("d4").epd():("Nf6", {"d5":300,"Nf6":-200})}
            def search(handle, pos, move=None):
                best, scores = fresh[pos.epd()]
                handle.name = "fixture"
                san = move or best
                return dict(san=san, cp=scores[san], depth=10, nodes=handle.nodes)
            def build(args, log):
                run(builder.main,*args)
            def verify(book, out, front, log):
                rv.run(book,"black",a.searches,out,a.nodes,1,deep_nodes=a.deep_nodes)
            with patch.object(rv.Searches,"_one",search):
                final = driver.drive(a, build=build, verify=verify)
            def prescriptions(path):
                df = pl.read_parquet(path).filter(pl.col("best_move").is_not_null())
                return dict(df.select("position_epd","best_move").iter_rows())
            self.assertEqual(prescriptions(a.out/"round-00/book_b2.parquet"),
                             {board("e4").epd():"c5",board("d4").epd():"Nf6"})
            self.assertEqual(prescriptions(final["book"]),
                             {board("e4").epd():"e5",board("d4").epd():"Nf6"})
            self.assertEqual(final["applied_overlay_sha256"], sha(Path(final["overlay"])))
            def unexpected(*args):
                self.fail("completed build or verification stage ran again on resume")
            self.assertEqual(driver.drive(a, build=unexpected, verify=unexpected)["book"], final["book"])
            gc.collect()

    @unittest.skipUnless(REAL, "pass --stockfish-smoke to run the installed binary")
    def test_installed_stockfish(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            meta = historical_inputs(root)
            props = root/"proposals.parquet"
            args = ("--targets",target_file(root),"--colour","black","--nodes",5000,"--threads",1,"--out",props)
            run(ec.main,*args)
            rows = pl.read_parquet(props)
            ec.check_edges(rows, True)
            self.assertEqual(rows["position_hash"].to_list(), PARENTS)
            book = book_at(root/"book.parquet", meta, ("h6","h6"), props)
            out = root/"verified.parquet"
            rv.run(book,"black",root/"cache",out,5000,1,deep_nodes=20000,band=10000)
            info = read(Path(str(out)+".meta.json"))
            self.assertEqual(len(info["retried_parents"]),2)
            self.assertEqual(info["searches"]["parents_searched"],2)
            again = root/"repeat.parquet"
            rv.run(book,"black",root/"cache",again,5000,1,deep_nodes=20000,band=10000)
            warm = read(Path(str(again)+".meta.json"))
            self.assertEqual((warm["searches"]["nodes"],warm["deep_searches"]["nodes"]), (0,0))
            assert_frame_equal(pl.read_parquet(out), pl.read_parquet(again), check_exact=True)
            print("Installed engine", rows["engine"][0], "base/deep nodes", info["searches"]["nodes"],
                  info["deep_searches"]["nodes"], "verified move rows", pl.read_parquet(out).height)


if __name__ == "__main__":
    unittest.main()
