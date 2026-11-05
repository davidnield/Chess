"""Regression checks for nomination, recall, ease-of-play risk, and personal game review."""
import io
import csv
from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace
import random
import tempfile

import chess
import chess.pgn
import duckdb
import numpy as np
import polars as pl

import budget_core as bc
import convertibility_eval as cv
from build_budget_books import collect_subgraph_edges
from engine_candidates import target_rows
from exit_difficulty import edge_risks, read_exits
from exit_practice import deck, departures, draw_positions, practice_results, review_games, worksheet
from practical_scenarios import paired_summary
from stage3_backwards_induction import cp_to_expected_score
from zobrist import zobrist_int64


def make_edge(b, san, total=100):
    child = b.copy()
    child.push_san(san)
    return {"parent_hash":zobrist_int64(b), "parent_epd":b.epd(), "move_san":san,
            "total":total, "child_hash":zobrist_int64(child), "white_wins":total//2,
            "draws":0, "black_wins":total-total//2, "white_score_avg":.5}


def qga_rows():
    board = chess.Board()
    rows = []
    for san in "d4 d5 c4 dxc4 Nc3 e5 dxe5 Qxd1+ Nxd1 Nc6 f4 Nb4 Ne3".split():
        rows.append(make_edge(board,san))
        board.push_san(san)
    recorded = make_edge(board,"Bc5",195)
    proposal = make_edge(board,"Nh6")
    next_board = board.copy()
    next_board.push_san("Nh6")
    reply = make_edge(next_board, next_board.san(next(iter(next_board.legal_moves))))
    return rows+[recorded,reply], recorded, proposal


def test_nomination():
    rows, recorded, proposal = qga_rows()
    root = zobrist_int64(chess.Board())
    h = recorded["parent_hash"]
    graph_edges = {r["parent_hash"]:[r] for r in rows}
    values = {r["parent_hash"]:cp_to_expected_score(0) for r in rows}
    values[h] = cp_to_expected_score(-110)
    values[recorded["child_hash"]] = cp_to_expected_score(8)
    kw = dict(gates=False,prior_strength=1,eval_weight=0,eps=0,share_floor=0,
              max_ply=16,max_eval_loss=30,strict_source=True,eval_ws=values)
    g = bc.build_graph(graph_edges, root, False, **kw)
    targets = target_rows(g,"black")
    assert targets.filter(pl.col("position_hash")==h)["status"].to_list() == ["fallback"]
    moves = {h:[("Nh6",proposal["child_hash"])]}
    values[proposal["child_hash"]] = cp_to_expected_score(-110)
    with tempfile.TemporaryDirectory() as folder, duckdb.connect() as con:
        stats = Path(folder)/"pool.parquet"
        pl.DataFrame(rows).write_parquet(stats)
        fetched = collect_subgraph_edges(con,str(stats),root,False,0.,16,0.,20,engine_moves=moves)
        assert proposal["child_hash"] in fetched["parent_hash"].to_list()
    g = bc.build_graph(graph_edges,root,False,engine_moves=moves,**kw)
    assert [(san,linked) for san,_,linked,_ in g.our[h].cands] == [("Nh6",True)]
    assert h not in g.meta["loss_exceptions"]
    # When preparing this forced line is beneficial, both solvers can book the nomination.
    bc.apply_stop_values(g, {key: .01 for key in g.our}, {})
    curves, _ = bc.build_curves(g, bmax=8)
    assert bc.extract_book(g, curves, 8)["booked"][h] == "Nh6"
    assert bc.price_search(g, 8, log=lambda _: None)[0]["booked"][h] == "Nh6"


class Policy:
    def probs(self, epds, _se, _oe, _keys):
        return [{"d4":1.} if epd == chess.Board().epd() else
                {chess.Board(epd+" 0 1").san(next(iter(chess.Board(epd+" 0 1").legal_moves))):1.}
                for epd in epds]

    def get(self, keys, rep):
        return [{} for _ in keys]


def test_recall():
    root = chess.Board()
    h = zobrist_int64(root)
    book = {h:"e4"}
    human = Policy()
    a = SimpleNamespace(tau=0.,rollouts=20,max_ply=1,k=1.,min_p=0.,
                        our_elo=1450,opp_elo=2000,recall=.75)
    zero = lambda hs: np.zeros(len(hs),dtype=int)
    exits = {}
    leaves, starts, known, _ = cv.enumerate_book(book,True,0,a,human,human,human,zero,exits)
    assert not starts and known == .75 and exits[(h,0)][0] == .25
    assert sum(l[0] for l in leaves)==1 and sum(l[7] for l in leaves)==.25
    start = [(h,root.epd(),1.,0,None,0.,1.)]
    rates = []
    for seed in range(50):
        rolled, known = cv.rollouts(start,book,True,0,a,seed,human,human,human,zero)
        rates.append(known)
        assert abs(known + sum(l[7] for l in rolled)-1) < 1e-12
    assert abs(np.mean(rates) - .75) < .05
    a.recall = 0.
    misses = {}
    rolled,known = cv.rollouts(start,book,True,0,a,2,human,human,human,zero,misses)
    assert known == 0 and abs(misses[h,0][0]-1)<1e-12


def test_risk_and_exits():
    board = chess.Board()
    row = make_edge(board,"e4")
    a = row["parent_hash"]
    b = row["child_hash"]
    board.push_san("e4")
    edge = (0,0,a,row["parent_epd"],b,board.epd())
    def lookup(hashes):
        return np.array([200 if h==a else -200 for h in hashes])
    risk,loss,se = edge_risks([edge,edge],2,True,.1,lookup,None)
    assert risk==.5 and loss>.1 and se==.5
    assert edge_risks([edge],2,False,.1,lookup,None)[0]==0
    with tempfile.TemporaryDirectory() as tmp:
        base = pl.DataFrame({"hash":[a],"ply":[0],"epd":[row["parent_epd"]],"mass":[.2],
                             "book":["A"],"seed":[1],"colour":["white"],"config":["c"]})
        first,second = [Path(tmp)/f"{n}.parquet" for n in (1,2)]
        base.write_parquet(first)
        base.head(0).write_parquet(second, metadata={"exit_job": '["A","white",2,"c"]'})
        assert read_exits([first,second])["mass"][0]==.1
        assert draw_positions({"A":[first,second]},{"A":{}},1,random.Random(0),"white")[0]["epd"]==row["parent_epd"]
    game=chess.pgn.read_game(io.StringIO("1. d4 d5 *"))
    assert departures(game,True,{a:"e4"})["event"]=="departed from book"
    assert departures(game,True,{})["event"]=="first uncovered position"


def test_comparison():
    df=pl.DataFrame({"label":["A","B","A","B"],"seed":[1,1,2,2],"replicate":[0]*4,
                     "uniform":[.5,.6,.4,.6]})
    result=next(r for r in paired_summary(df,"A",{"A":{1:"e4"},"B":{1:"d4"}},{})
                if r["label"]=="B")
    assert abs(result["mean_diff"]-.15)<1e-12 and result["new_or_changed"]==1


def test_practice_roundtrip():
    with tempfile.TemporaryDirectory() as folder:
        root = Path(folder)
        board = chess.Board()
        exits = []
        for ply in (0, 2):
            exits.append({"hash": zobrist_int64(board), "ply": ply, "epd": board.epd(), "mass": .5})
            if not ply:
                board.push_san("e4")
                board.push_san("e5")
        pl.DataFrame(exits).write_parquet(root/"exits.parquet")
        pl.DataFrame({"position_hash": [exits[0]["hash"]], "side_to_move": ["white"],
                      "best_move": ["e4"]}).write_parquet(root/"book.parquet")
        args = SimpleNamespace(exits=[f"blind-label={root/'exits.parquet'}"],
                               book=[f"blind-label={root/'book.parquet'}"], colour="white",
                               count=2, seed=12, start=date(2026,11,4), fast="600+5", slow="3600+30",
                               out=root/"deck")
        deck(args)
        key = json.loads((args.out/"private/key.json").read_text())
        with (args.out/"observations.csv").open() as stream:
            observations = list(csv.DictReader(stream))
        assert len(observations) == 4
        for tag in key:
            conditions = {r["time_control"] for r in observations if r["position"] == tag}
            assert conditions == {args.fast, args.slow}
        for session in (1,2):
            content = (args.out/f"session-{session}.pgn").read_text()
            assert "blind-label" not in content
            with io.StringIO(content) as stream:
                for _ in range(2):
                    game = chess.pgn.read_game(stream)
                    tag = game.headers["Event"].split()[-1]
                    assert game.board().epd() == key[tag]["epd"]
        first = observations[0]
        board = chess.Board(key[first["position"]]["epd"]+" 0 1")
        first.update(first_move=board.san(next(iter(board.legal_moves))), result="1-0")
        worksheet(args.out/"observations.csv", observations)
        practice_results(SimpleNamespace(deck=args.out, out=root/"answers.csv"))
        with (root/"answers.csv").open() as stream:
            answer, = csv.DictReader(stream)
        assert answer["our_score"] == "1.0" and answer["exit_sources"] == "blind-label"
        pgn = root/"example.pgn"
        pgn.write_text('[White "Learner"]\n[Black "Partner"]\n\n1. d4 {[%clk 0:09:58]} d5 *\n')
        review_games(SimpleNamespace(book=[f"white={root/'book.parquet'}"], pgn=[pgn],
                                     player="learner", out=root/"game.csv"))
        with (root/"game.csv").open() as stream:
            departure, = csv.DictReader(stream)
        assert departure["actual"] == "d4" and departure["prescribed"] == "e4"
        assert float(departure["clock_after_seconds"]) == 598


if __name__=="__main__":
    for test in (test_nomination,test_recall,test_risk_and_exits,test_comparison,test_practice_roundtrip):
        test()
        print("PASS",test.__name__)
