"""Blind replay, held-out folds, score gates, mass conservation and the White decision."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from contextlib import ExitStack

import chess
import numpy as np
import polars as pl

import budget_core as bc
import engine_candidates as ec
import nomination_trial as trial
from build_budget_books import GateAdmit
import rebucket_pool as pool
import sampled_stop
from nomination_evidence import summarize
from targeted_candidates import replay
from tune_maia_shrinkage import grade
from validate_book import classify
from zobrist import zobrist_int64


def edge(line,san,cp):
    b=chess.Board()
    for s in line:
        b.push_san(s)
    h,epd=zobrist_int64(b),b.epd()
    b.push_san(san)
    return (h,epd,san,zobrist_int64(b),cp,10_000_000,25,"Stockfish","test","fallback")


with TemporaryDirectory() as td:
    root=Path(td)
    one=edge(["e4"],"c5",10)
    hidden=edge(["d4"],"d5",-900)
    frame=pl.DataFrame([one,hidden],schema=ec.SCHEMA,orient="row")
    file=root/"raw.parquet"
    frame.write_parquet(file)
    ask=pl.DataFrame({"position_hash":[one[0]],"epd":[one[1]]})
    a=replay(ask,[file])
    frame.with_columns(pl.when(pl.col("position_hash")==hidden[0]).then(999)
                       .otherwise(pl.col("child_cp_white")).alias("child_cp_white")).write_parquet(file)
    assert replay(ask,[file]).equals(a) and a.height==1
    assert replay(ask.with_columns(pl.lit(hidden[1]).alias("epd")),[file]).is_empty()
    frame.with_columns(pl.lit(1).alias("verification_nodes")).write_parquet(file)
    try:
        replay(ask,[file])
        raise AssertionError("A corrected overlay was admitted")
    except ValueError:
        pass
    book=pl.DataFrame({"position_hash":[one[0]],"position_epd":[one[1]],"best_move":["c5"]})
    graph=pl.DataFrame({"position_hash":[hidden[0]],"epd":[hidden[1]],"colour":["black"],
                        "reach":[.1],"status":["fallback"],"ply":[1]})
    audit=pl.DataFrame({"position_hash":[one[0]],"cached_loss":[29],"status":["in_set"],"flagged":[False]})
    with patch.object(trial,"edges_for",side_effect=[[(one[0],"e5",10,30)],[(10,"d4",hidden[0],10)]]):
        requested=trial.requested(book,audit,graph,set(),0,"unused",{"games":20,"loss":30,"colour":"black"})
    assert set(requested["request_reason"])=={"book","frontier"}
    # Different absolute gates actually change admission and the recipe checked by the auditor.
    board=chess.Board()
    h=zobrist_int64(board)
    rows=[]
    vals={h:.5}
    for san,v in [("e4",.5),("d4",.42)]:
        board.push_san(san)
        child=zobrist_int64(board)
        board.pop()
        rows.append(dict(move_san=san,child_hash=child,total=30,white_score_avg=.5))
        vals[child]=v
    meta={"slice_prior":.5,"gate_rel_floor":1.}
    assert classify(h,"d4",rows,None,vals,True,{**meta,"robustness_floor":.1},None,board.epd())=="in_set"
    assert classify(h,"d4",rows,None,vals,True,{**meta,"robustness_floor":.04},None,board.epd())=="outside_set"
    assert classify(h,"d4",rows,None,vals,True,{**meta,"gate_rel_floor":.04},None,board.epd())=="outside_set"
    with patch("eval_arrays.open_eval_arrays",return_value=()):
        admit=GateAdmit({},True,.5,robustness_floor=.04,gate_rel_floor=1.)
    admit.eval_ws.update(vals)
    assert admit(h,rows)=={rows[0]["child_hash"]}
    assert "gate_policy" in sampled_stop.table_problems({"sim":{}},{"gate_policy":{
        "robustness_floor":.04,"gate_rel_floor":.1,"gate_rel_own_margin":.02}})
    counts={"a":5,"b":5}
    n,logloss,removed=grade(counts,{"a":7},{"a":.5,"b":.5},100)
    assert n==7 and np.isclose(logloss,7*np.log(2)) and removed==0
    s=root/"stats.parquet"
    aux=root/"aux.parquet"
    pl.DataFrame({"parent_hash":[1,1],"child_hash":[4,5],"total":[20,5],"white_wins":[10,1],
                  "draws":[4,2],"black_wins":[6,2]}).write_parquet(s)
    pl.DataFrame({"position_hash":[1],"other_total":[4],"other_white_wins":[1],"other_draws":[1],
                  "other_black_wins":[2],"other_edges":[1],"other_eval_mean":[.5],"other_eval_min":[.5],
                  "other_eval_max":[.5],"other_eval_cov":[.5]}).write_parquet(aux)
    for p in (s,aux):
        Path(str(p)+".meta.json").write_text(json.dumps({"min_games":5,"population":{"rating":2000}}))
    with patch.object(pool,"open_eval_arrays",return_value=()),patch.object(pool,"lookup_evals",side_effect=lambda keys:np.zeros(len(keys))):
        pool.build(s,aux,10,root/"pooled")
    out=pl.read_parquet(root/"pooled/aux.parquet").to_dicts()[0]
    assert out["other_total"]==9 and out["other_eval_mean"]==.5 and np.isclose(out["other_eval_cov"],7/9)
    def scores(seeds,reps,delta):
        return pl.DataFrame([{"seed":s,"replicate":r,"label":k,"uniform":v}
            for r in reps for s in seeds for k,v in [("TARGET",.5+delta),("BROAD",.5)]])
    for val,expected in [(0,"targeted"),(-.01,"broad")]:
        assert summarize(scores(range(1,5),[0],val),scores([1],range(17),val),{"rows":[]})["method"]==expected
    risk={"rows":[dict(arm="targeted",hash=10,reach=.01,loss_cp=80,unstable=False)]}
    assert summarize(scores(range(1,5),[0],0),scores([1],range(17),0),risk)["method"]=="broad"
    # Exercise the actual loop and resume. The provisional book controls which of
    # two graph targets receives a local answer; a later feedback pass converges.
    original=root/"broad"
    exp=root/"trial"
    (original/"black").mkdir(parents=True)
    targets=pl.concat([graph,graph.with_columns(pl.lit(one[0]).alias("position_hash"),pl.lit(one[1]).alias("epd"))])
    targets.write_parquet(original/"black/targets-01.parquet")
    def fake_query(colour,label,kind,targets_path,dest):
        if kind=="cloud":
            frame.clear().write_parquet(dest)
        else:
            requested=pl.read_parquet(targets_path)
            assert requested["position_hash"].to_list()==[one[0]]
            frame.head(1).write_parquet(dest)
    def provisional(colour,work,label,base,props):
        path=work/"book.parquet"
        book.write_parquet(path)
        Path(str(path)+".meta.json").write_text(json.dumps({"min_games":20,"max_eval_loss":30}))
        return path
    def fake_stage(label,cmd,outputs):
        if label.endswith("-graph"):
            targets.write_parquet(outputs[0])
        elif label.endswith("-screen"):
            audit.with_columns(pl.lit("fallback").alias("status")).write_parquet(outputs[0])
            outputs[1].write_text("{}")
        elif label.endswith("-feedback"):
            import shutil
            src=Path(cmd[cmd.index("--proposals")+1])
            shutil.copyfile(src,outputs[0])
            outputs[1].write_text(json.dumps({"searched_positions":0}))
        else:
            raise AssertionError(label)
    with ExitStack() as stack:
        for obj,name,value in [(trial,"BROAD",original),(trial,"TRIAL",exp),(trial,"query",fake_query),
                (trial,"exact_provisional",provisional),(trial,"edges_for",lambda *a:[]),
                (trial.r,"stage",fake_stage),(trial.r,"resolve_proposals",lambda c,l,p:p),
                (trial.r,"audit",lambda c,b:{"NEW":{"ok":True}})]:
            stack.enter_context(patch.object(obj,name,value))
        assert trial.build("black")==trial.build("black")
        final=json.loads((exp/"black/selected.json").read_text())
        assert final["local_requests"]==1 and final["frontier_requests"]==0 and final["cloud_queries"]==2
print("PASS targeted replay, frontier requests, gates, shrinkage, pooled mass, paired decision")
