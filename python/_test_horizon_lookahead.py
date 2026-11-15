"""Black pilot regressions through the production collector, native menu, and receipt."""
import json
import tempfile
import unittest
from pathlib import Path

import chess
import duckdb
import numpy as np
import polars as pl

import budget_core as bc
import budget_rs
import build_budget_books as builder
import lookahead_audit as audit
import lookahead_policy as policy
import run_rare_book as runner
from _test_book_builder_rs import reach_case
from build_budget_books import zobrist_int64


def board(path):
    b = chess.Board()
    for move in path.split():
        b.push_san(move)
    return b


def key(path):
    return zobrist_int64(board(path))


def row(path, san, count, named=None):
    b = board(path)
    h, epd = zobrist_int64(b), b.epd()
    b.push_san(san)
    return dict(parent_hash=h, parent_epd=epd, child_hash=zobrist_int64(b),
                move_san=san, total=count, white_score_avg=.5, white_wins=0, draws=count,
                black_wins=0, reply_mass=named, reach_share=named, additional_reply=count == 0)


def build(frame, rep, scores, root, white, cap, depth, path, *, modern=False, floor=.10):
    fi = budget_rs.frame_inputs(frame.sort("parent_hash", "move_san"), rep_moves=rep, strict_source=True)
    ek = np.array(list(scores), np.int64)
    ev = np.array(list(scores.values()), np.float64)
    def assemble():
        return budget_rs.run_csr(fi["edges"], fi["rows"], fi["aux"], ek, ev, root, white,
            slice_prior=.52, prior_strength=500., eval_weight=.5, eval_weight_min=.1,
            eval_weight_k=100., eps=.0001, max_ply=cap, share_floor=.002,
            robustness_floor=floor, gate_rel_floor=bc.GATE_REL_FLOOR,
            gate_rel_own_margin=bc.GATE_REL_OWN_MARGIN, require_eval=True,
            gates=True, max_eval_loss=None if modern else 30., max_score_loss=.095 if modern else None,
            rare_floor=None, strict_source=True, other_cov_blend=False, gated_reach=True,
            crack_at_parents=True, engine_moves_given=False, track_route_ply=True, track_other=False)
    pre = assemble()
    codes, bans, reach, stats = policy.prepare(pre, fi["san_list"], ek, ev, white=white, prior=.52,
        floor=floor, depth=depth, score_gate=modern, max_ply=cap, path=path)
    assert set(reach) == set(bans)
    fi["rows"]["lookahead_veto"] = codes
    final = assemble()
    return bans, stats, policy.evidence(final, fi["san_list"], ek, ev)


def menu(frame, h):
    return set(frame.filter((pl.col("parent") == h) & pl.col("ours"))["san"].drop_nulls())


class Pilot(unittest.TestCase):
    def test_horizon_and_collector(self):
        # 8 is reached at ply 5 with probability 1 and ply 3 with probability .2.
        # Its successor can take only the short route's .2. A cycle cannot borrow more depth.
        forks = {1:[(2,1),(3,1)], 2:[(4,2),(20,8)], 3:[(5,10)], 4:[(8,1)],
                 5:[(6,1)], 6:[(7,10)], 7:[(8,1)], 8:[(9,3),(21,7)], 9:[(8,1),(22,1)],
                 21:[(23,1)], 22:[(24,1)], 23:[(25,1)]}
        for black in (False, True):
            g = dict(forks)
            if black:
                g[31] = g.pop(1)
                g[1] = [(31,1)]
            cap = 5 + int(black)
            for eps in (.001,.1):
                # Independent oracle explicitly enumerates every eligible walk to the cap.
                best, earliest, stack = {}, {}, [(1,0,1.)]
                while stack:
                    h,d,r = stack.pop()
                    best[h] = max(best.get(h,0),r)
                    earliest[h] = min(earliest.get(h,d),d)
                    if d == cap:
                        continue
                    children = g.get(h,[])
                    for child, n in children:
                        q = r if (d % 2 == 0) != black else r*n/sum(v for _,v in children)
                        if q >= eps:
                            stack.append((child,d+1,q))
                ebp = {h:[dict(child_hash=c,total=n) for c,n in kids] for h,kids in g.items()}
                got, ply = bc.optimistic_reach(ebp,1,not black,eps,cap,.002)
                self.assertEqual(earliest,ply)
                self.assertEqual(set(best),set(got))
                for h,q in best.items():
                    self.assertAlmostEqual(got[h],q)
                self.assertTrue(reach_case(ebp,not black,eps,cap,.002,None,True)[0])
                self.assertEqual((5 if black else 4) if eps < .06 else None,ply.get(9))
                self.assertAlmostEqual(best.get(9,0),.06 if eps < .06 else 0)
                with tempfile.TemporaryDirectory() as td, duckdb.connect(config={"threads":1}) as db:
                    pool = Path(td)/"pool.parquet"
                    pl.DataFrame([dict(parent_hash=h,parent_epd="test",child_hash=c,
                        move_san=str(c),total=n,white_wins=0,draws=n,black_wins=0,white_score_avg=.5)
                        for h,kids in g.items() for c,n in kids]).write_parquet(pool)
                    # Each reached parent with rows is fetched even at the terminal layer.
                    for collect in (builder.collect_subgraph_edges, builder.collect_subgraph_edges_rs):
                        out = collect(db,pool.as_posix(),1,not black,eps,cap,.002,1)
                        self.assertEqual(set(out["parent_hash"]),set(best)&set(g))

    def test_handoff_forced_reply(self):
        # Reproduce finding 6 exactly, including the most-likely and earliest
        # route to X, the probability of Y, and the route ply used for stopping.
        graph = {1: [(2, 1), (3, 1)], 2: [(4, 2), (20, 8)], 3: [(5, 1)],
                 4: [(8, 1)], 5: [(6, 1)], 6: [(7, 1)], 7: [(8, 1)], 8: [(9, 1)]}
        edges = {h: [dict(child_hash=c, total=n) for c, n in children]
                 for h, children in graph.items()}
        reach, ply = bc.optimistic_reach(edges, 1, True, .001, 5, .002)
        route = bc.route_plies(edges, 1, True, reach, ply, 5, .002)
        self.assertEqual((reach[8], ply[8], route[8]), (1., 3, 5))
        self.assertEqual((reach[9], ply[9], route[9]), (.2, 4, 4))
        self.assertTrue(reach_case(edges, True, .001, 5, .002, None, True)[0])

    def test_source_reply_frequencies_and_receipt(self):
        # Historical response missing 1.d4 is nominated by the source. Black's new
        # zero-game 1...e5 after it is supplied by the reply prior, and reaches a void.
        root, d4, void = key(""), key("d4"), key("d4 e5")
        frame = pl.DataFrame([row("","e4",50),row("d4","e5",0,.8),row("d4","d5",10,.2),
                              row("d4 e5","Nf3",10)])
        scores = {root:.53,key("e4"):.53,d4:.52,void:.51,key("d4 e5 Nf3"):.30}
        with tempfile.TemporaryDirectory() as td:
            path = Path(td)
            evidence = path/"book.lookahead-inputs.parquet"
            book = path/"book_b2000.parquet"
            mpath = Path(str(book)+".meta.json")
            veto = Path(str(book)+".lookahead.json")
            report = Path(str(book)+".lookahead-audit.json")
            receipt = path/"build.receipt.json"
            parts = [book,mpath,veto,evidence,report]
            def produce():
                bans, stats, final = build(frame,{root:"d4"},scores,root,True,6,99,evidence)
                self.assertEqual(menu(pl.read_parquet(evidence),root),{"e4","d4"})
                self.assertEqual(bans.get(root),["d4"])
                self.assertEqual(menu(final,root),{"e4"})
                meta = dict(perspective="white",slice_prior=.52,robustness_floor=.10,
                            max_ply=6,lookahead_floor=stats)
                stats["veto_sha256"] = policy.write_veto(book,bans,{root:1})
                mpath.write_text(json.dumps(meta),encoding="utf8")
                pl.DataFrame(dict(position_hash=[root],side_to_move=["white"],best_move=["e4"])).write_parquet(book)
                result = audit.audit(book,meta)
                self.assertEqual(result["checked_decisions"],1)
                self.assertEqual(result["verified_vetoes"],1)
                report.write_text(json.dumps(result),encoding="utf8")
            self.assertFalse(runner.stage(receipt,{"depth":99},parts,produce)[1])
            self.assertTrue(runner.stage(receipt,{"depth":99},parts,produce)[1])
            meta = json.loads(mpath.read_text())
            audit.audit(book,meta)
            for p in (evidence,veto,report):
                raw = p.read_bytes()
                for remove in (True,False):
                    if remove:
                        p.unlink()
                    else:
                        p.write_bytes(raw+b"changed")
                    with self.assertRaises(runner.StaleRound):
                        runner.stage(receipt,{"depth":99},parts,produce)
                    with self.assertRaises(ValueError):
                        audit.audit(book,meta)
                    p.write_bytes(raw)
            original = book.read_bytes()
            pl.DataFrame(dict(position_hash=[root],side_to_move=["white"],best_move=["d4"])).write_parquet(book)
            with self.assertRaisesRegex(ValueError, "plays a vetoed move"):
                audit.audit(book, meta, check_receipt=False)
            book.write_bytes(original)
            original = evidence.read_bytes()
            changed = pl.read_parquet(evidence).with_columns(
                pl.when(pl.col("parent")==void).then(1).otherwise(pl.col("dead_at")).alias("dead_at"))
            changed.write_parquet(evidence)
            meta["lookahead_floor"]["evidence_sha256"] = policy.sha256(evidence)
            with self.assertRaisesRegex(ValueError, "propagation disagrees"):
                audit.audit(book, meta, check_receipt=False)
            evidence.write_bytes(original)
            meta["lookahead_floor"]["evidence_sha256"] = policy.sha256(evidence)
            veto.write_text("[]",encoding="utf8")
            meta["lookahead_floor"]["veto_sha256"] = policy.sha256(veto)
            with self.assertRaisesRegex(ValueError,"disagrees"):
                audit.audit(book,meta,check_receipt=False)
            # Falls below the followed-reply share threshold; the trap must disappear.
            thin = frame.with_columns(pl.when(pl.col("move_san")=="e5").then(.001)
                                      .otherwise(pl.col("reach_share")).alias("reach_share"))
            bans,_,_ = build(thin,{root:"d4"},scores,root,True,6,99,evidence)
            self.assertNotIn(root,bans)
            # Compare the real collectors: count-zero restoration fetches descendants but
            # never fabricates an observation in the emitted pool frame.
            pool = path/"pool.parquet"
            frame.drop("reply_mass","reach_share","additional_reply").write_parquet(pool)
            with duckdb.connect(config={"threads":1}) as db:
                for collect in (builder.collect_subgraph_edges,builder.collect_subgraph_edges_rs):
                    result = collect(db,pool.as_posix(),root,True,.0001,5,.002,1,source_moves={root:"d4"})
                    self.assertIn(d4,list(result["parent_hash"]))
                    self.assertEqual(list(result.filter(pl.col("parent_hash")==root)["move_san"]),["e4"])

    def test_exact_handoff_and_score_gate_reference(self):
        root = key("")
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "inputs.parquet"
            frame = pl.DataFrame([row("", "e4", 50)])
            scores = {root: .425, key("e4"): .425, key("d4"): .415}
            bans, _, final = build(frame, {root: "d4"}, scores, root, True, 5, 99, path)
            self.assertEqual(menu(pl.read_parquet(path), root), {"e4", "d4"})
            self.assertEqual(bans, {root: ["d4"]})
            self.assertEqual(menu(final, root), {"e4"})

            # The strongest candidate sets the original 9.5-point reference. Its veto
            # must not admit Nf3, which would pass relative to the surviving d4.
            frame = pl.DataFrame([row("", m, 50) for m in ("Nf3", "e4", "d4")]
                                 + [row("e4", "e5", 50), row("e4 e5", "Nc3", 50)])
            scores = {root: .52, key("e4"): .60, key("d4"): .52, key("Nf3"): .49,
                      key("e4 e5"): .50, key("e4 e5 Nc3"): .30}
            bans, _, final = build(frame, {}, scores, root, True, 5, 99, path, modern=True)
            self.assertEqual(menu(pl.read_parquet(path), root), {"e4", "d4"})
            self.assertEqual(bans[root], ["e4"])
            self.assertEqual(menu(final, root), {"d4"})

    def test_shared_continuation_fits_only_the_earlier_parent(self):
        # The long and short parents enter the same subtree. A move from ply
        # eight cannot borrow the earlier arrival and reach a failure on ply twelve.
        edges = pl.DataFrame([
            (1,True,"a",2,True,.5,0), (1,True,"safe",30,False,.5,0),
            (9,True,"a",2,True,.5,8), (9,True,"safe",31,False,.5,8),
            (2,False,None,3,True,None,1), (3,True,"b",4,True,.5,2),
            (4,False,None,5,True,None,3), (5,True,"bad",6,False,.2,4)],
            schema=["parent","ours","san","child","expanded","score","pre_ply"], orient="row")
        veto, ranks, stats = policy.propagate(edges, True, .5, .1, 99, True, max_ply=10)
        self.assertEqual(veto.rows(), [(1,"a")])
        self.assertEqual(dict(ranks.rows()), {5:0, 3:1})
        self.assertTrue(stats["fixed_point"])

    def test_builder_entry_point(self):
        import sys
        from unittest.mock import patch
        import eval_arrays

        # All file reads, budget allocation, final serialization and standalone auditing
        # go through production. Supply a tiny in-memory eval generation for portability.
        with tempfile.TemporaryDirectory() as td:
            path = Path(td)
            frame = pl.DataFrame([row("", "e4", 1000), row("e4", "e5", 40), row("e4", "c5", 960)])
            pool, rep, weak, prefix = [path / n for n in ("pool.parquet", "rep.parquet", "weak.parquet", "book")]
            frame.drop("reply_mass", "reach_share", "additional_reply").write_parquet(pool)
            frame.filter(pl.col("move_san") == "c5").write_parquet(weak)
            pl.DataFrame(dict(position_hash=[key("e4")], side_to_move=["black"], event=["pooled"],
                        elo_band=["2000+"], best_move=["e5"], value=[.5], value_robust=[.5],
                        value_worst=[.5], eval_score=[.5])).write_parquet(rep)
            cp = {key(""): 0, key("e4"): 0, key("e4 e5"): -180, key("e4 c5"): 500}
            mm = (np.array(sorted(cp), np.int64), np.array([cp[h] for h in sorted(cp)], np.int16))
            args = ["build_budget_books.py", "--stats", str(pool), "--rep", str(rep),
                    "--perspective", "black", "--budget", "1", "--budget", "2", "--gated-reach",
                    "--min-games", "1", "--max-ply", "3", "--threads", "1", "--max-eval-loss", "30",
                    "--lookahead-floor=99", "--weak-pool", str(weak), "--out-prefix", str(prefix)]
            with patch.object(sys, "argv", args), patch.object(eval_arrays, "open_eval_arrays", return_value=mm):
                self.assertEqual(builder.main(), 0)
                self.assertEqual(builder.main(), 0)  # authenticated skip
            books = [path / f"book_b{budget}.parquet" for budget in (1, 2)]
            for book in books:
                meta = json.loads(Path(str(book)+".meta.json").read_text())
                self.assertEqual(meta["lookahead_floor"]["evidence_file"], book.name + ".lookahead-inputs.parquet")
                self.assertEqual(audit.audit(book, meta)["checked_decisions"], 1)
                self.assertEqual(pl.read_parquet(book)["best_move"].drop_nulls().to_list(), ["e5"])
            # Rebuilding one budget leaves its sibling proof readable.
            sibling = Path(str(books[1]) + ".lookahead-inputs.parquet")
            sibling_hash = policy.sha256(sibling)
            i = args.index("2")
            del args[i-1:i+1]
            args.append("--force")
            with patch.object(sys, "argv", args), patch.object(eval_arrays, "open_eval_arrays", return_value=mm):
                self.assertEqual(builder.main(), 0)
            self.assertEqual(sibling_hash, policy.sha256(sibling))

            # Exercise the round coordinator's real audit subprocess and resume rules
            # on the first book. Only the expensive search and repeated build are stubbed.
            import shutil
            source = path / "code"
            source.mkdir()
            for module in (policy, audit):
                shutil.copy(module.__file__, source)
            stages = path / "stages"
            stages.mkdir()
            (stages / "black-cp30.json").write_text(json.dumps({"command": [
                "py", "--rep", str(rep), "--budget", "1", "--max-eval-loss", "30",
                "--out-prefix", "book", "--tmp-dir", "tmp"]}))
            (path / "ready.json").write_text(json.dumps({"recipe_sha256": "fixture"}))
            opts = runner.parser().parse_args(["--colour", "black", "--cap", "30", "--baseline", str(path),
                "--lookup", str(path), "--out", str(path/"pilot"), "--searches", str(path/"cache"),
                "--build-arg=--lookahead-floor=99"])
            def copy_build(args, log):
                for f in path.glob(books[0].name+"*"):
                    shutil.copy(f, log.parent)
                (log.parent / (books[0].name+".rare.json")).write_text("[]")
            def verify(book, overlay, _, log):
                pl.DataFrame({"n": [0]}).write_parquet(overlay)
                Path(str(overlay)+".meta.json").write_text(json.dumps({"requested_parents":0,"by_reason":{}}))
            result = runner.drive(opts, copy_build, verify, source=source)
            saved = Path(result["book"])
            artifacts = json.loads((saved.parent/"build.receipt.json").read_text())["outputs"]
            self.assertEqual(set(artifacts), {saved.name+suffix for suffix in (
                "", ".meta.json", ".rare.json", ".lookahead-inputs.parquet", ".lookahead.json", ".lookahead-audit.json")})
            self.assertEqual(runner.drive(opts, copy_build, verify, source=source), result)
            Path(str(saved)+".lookahead.json").unlink()
            with self.assertRaises(runner.StaleRound):
                runner.drive(opts, copy_build, verify, source=source)

    def test_black_direct_source_and_relative_reference(self):
        # For Black, a lower White score is better. A restored move near the
        # legacy Black absolute bar (.62) can pass the relative source exception.
        root = key("e4")
        frame = pl.DataFrame([row("e4","e5",50),row("e4","c5",30)])
        scores = {root:.613,key("e4 e5"):.615,key("e4 c5"):.68,key("e4 d5"):.623}
        with tempfile.TemporaryDirectory() as td:
            path = Path(td)/"inputs.parquet"
            # Production always starts on White's turn. Prefix Black's decision.
            frame = pl.concat([pl.DataFrame([row("","e4",50)]),frame])
            bans,_,final = build(frame,{root:"d5"},scores,key(""),False,4,3,path)
            self.assertEqual(menu(pl.read_parquet(path),root),{"e5","d5"})
            self.assertEqual(bans[root],["d5"])
            self.assertEqual(menu(final,root),{"e5"})

if __name__ == "__main__":
    unittest.main()
