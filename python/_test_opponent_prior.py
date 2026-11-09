"""Budget-graph regression for reified rare replies and Maia pseudogames."""
import math
import budget_core as bc


def run():
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
