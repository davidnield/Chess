"""Budget-graph regression for reified rare replies and Maia pseudogames."""
import math
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import polars as pl
import budget_core as bc
from build_budget_books import collect_subgraph_edges
from opponent_prior import ReplyOverlay


def all_subfloor_parent():
    with TemporaryDirectory() as tmp:
        path = Path(tmp) / "overlay.parquet"
        pool, aux = (Path(tmp)/f"{s}.parquet" for s in ("pool","aux"))
        # 1 (White) has no retained reply. The existing graph starts again
        # at Black's reply positions 2 and 3. Position 4 has no outgoing edge.
        rows = pl.DataFrame({"parent_hash":[2,3],"move_san":["x","y"],"child_hash":[12,13],
                             "parent_epd":["test2","test3"],"total":[20,20],"white_wins":[10,8],
                             "draws":[0,2],"black_wins":[10,10],"white_score_avg":[.5,.45]})
        rows.write_parquet(pool)
        pl.DataFrame({"parent_hash":[1]*3,"parent_epd":["test1"]*3,"move_san":["a","b","c"],
                      "child_hash":[2,3,4],"total":[4,2,0],"white_wins":[4,0,0],"draws":[0,0,0],
                      "black_wins":[0,2,0],"white_score_avg":[1.,0.,.5],"reply_observed":[4,2,0],
                      "maia_p":[.2,.3,.5],"nonmove":[2]*3}).write_parquet(path)
        extra = Path(str(path)+".aux.parquet")
        pl.DataFrame({"position_hash":[1],"term_other_total":[2],"term_other_black_wins":[2]}).write_parquet(extra)
        meta = {"sha256":hashlib.sha256(path.read_bytes()).hexdigest(),"schema":2,"colour":"black",
                "source_pool":str(pool),"source_aux":str(aux),"min_games":20,"scope":"fixture",
                "extra_aux_sha256":hashlib.sha256(extra.read_bytes()).hexdigest()}
        Path(str(path)+".json").write_text(json.dumps(meta))
        for k in (0,30):
            overlay = ReplyOverlay(path,k,pool,aux,"black",20)
            with duckdb.connect() as con:
                result = collect_subgraph_edges(con,str(pool),1,False,.001,2,.01,20,reply_overlay=overlay)
            first = result.filter(pl.col("parent_hash")==1)
            assert first["additional_reply"].all()
            assert dict(first.select("child_hash","total").iter_rows()) == ({2:4,3:2,4:0} if k else {2:4,3:2})
            for row in first.to_dicts():
                expected = (row["total"] + k*{2:.2,3:.3,4:.5}[row["child_hash"]])/(6+k)
                assert math.isclose(row["reach_share"],expected*6/8)
            assert first.filter(pl.col("child_hash")==2)["white_score_avg"][0] == 1
            edges = {1:first.to_dicts(),2:[rows.row(0,named=True)],3:[rows.row(1,named=True)]}
            g = bc.build_graph(edges,1,False,eps=.001,max_ply=2,share_floor=.01,gates=False,
                               require_eval=False,aux_rows=overlay.extra_aux)
            assert math.isclose(g.opp[1].const_base,2/8)
            assert math.isclose(sum(p for p,*_ in g.opp[1].kids),6/8)
            assert {2,3} <= g.our.keys()


def run():
    all_subfloor_parent()
    # R had 20 retained moves, 5 OTHER, 4 terminal wins for White, and
    # 1 horizon draw. K=30 adds 3, 3, and 24 to three legal White replies.
    edges = {1: [
        dict(move_san="A",child_hash=2,total=20,reply_mass=23.,reach_share=23/60,white_score_avg=.55),
        dict(move_san="B",child_hash=3,total=0,reply_mass=8.,reach_share=8/60,white_score_avg=.5),
        dict(move_san="C",child_hash=4,total=0,reply_mass=24.,reach_share=24/60,white_score_avg=.5)]}
    for h in (2,3,4):
        edges[h] = [dict(move_san="b",child_hash=10+h,total=25,white_score_avg=.4)]
    aux = {1:dict(other_total=5,other_black_wins=5,term_normal_total=4,
                  term_normal_white_wins=4,horizon_total=1,horizon_draws=1)}
    g=bc.build_graph(edges,1,False,eps=.01,max_ply=2,share_floor=.15,gates=False,require_eval=False,
                     eval_ws={},aux_rows=aux,track_route_ply=True,track_other=True)
    assert 4 in g.our and 3 not in g.our and g.route_ply[4]==1
    assert math.isclose(g.reach[4],24/60)
    node=g.opp[1]
    assert {h:round(p*60) for p,h,_,_ in node.kids}=={2:23,4:24}
    assert [(round(p*60),h) for p,h,_ in node.floor]==[(8,3)]
    assert 1 not in g.other
    assert math.isclose(node.const_base-node.floor[0][0]*node.floor[0][2],.5/60)
    assert g.meta["named_opponent_replies"]["additional_edges"]==2
    # The same raw count rows without a model use the historical reach rule.
    assert bc.reply_share({"total":4},20)==.2
    assert bc.reply_share({"total":0,"reach_share":.4},20)==.4
    print("PASS opponent prior: reach, route, floor, backups, real terminal mass, no double counted OTHER")


if __name__=="__main__":
    run()
