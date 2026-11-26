"""Small reproductions of current behavior; no production data/model/engine jobs."""
from pathlib import Path
import json
import sys
import math
import hashlib

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'python'))
import duckdb
import numpy as np
import polars as pl
import chess
from weak_stop import weak_stop_values, _shares, uniform_score, EvalSource, MISSING
from sweep.policy import ensemble
import convertibility_eval as cv
from types import SimpleNamespace
from zobrist import zobrist_int64

OUT = Path(__file__).parent

def write(path, rows):
    pl.DataFrame(rows, schema={'parent_hash': pl.Int64, 'child_hash': pl.Int64,
                              'total': pl.Int64}, orient='row').write_parquet(path)

def stop(tag, weak, adult, scores, xs=(1,), ys=()):
    wp, ap = OUT / (tag + '-weak.parquet'), OUT / (tag + '-adult.parquet')
    write(wp, weak); write(ap, adult)
    with duckdb.connect(config={'threads': 1, 'memory_limit': '256MB'}) as con:
        w, r, c = weak_stop_values(con, xs, ys, pool=ap.as_posix(), weak_pool=wp.as_posix(),
                                  evals=lambda hs: np.array([scores.get(int(h), np.nan) for h in hs]),
                                  log=lambda _: None)
    return dict(W=w, R=r, counters=dict(c))

res = {}
res['weak19_adult1'] = stop('n19', [(1,2,19)], [(1,3,1)], {1:.5,2:.8,3:.2})
res['weak20_adult1'] = stop('n20', [(1,2,20)], [(1,3,1)], {1:.5,2:.8,3:.2})
res['weak19_adult0'] = stop('n19zero', [(1,2,19)], [], {1:.5,2:.8})
assert res['weak19_adult1']['W'][1] == .2
assert res['weak20_adult1']['W'][1] == .8
assert res['weak19_adult0']['W'][1] == .5

res['opponent_raw_counts'] = stop('opponent', [(1,2,20)], [(2,3,100)], {3:.9,4:.1})
res['opponent_ensemble_counterfactual'] = .1*.9 + .9*.1
assert res['opponent_raw_counts']['W'][1] == .9

res['missing_child_renormalization'] = stop('missing', [(1,2,20)], [(2,3,50),(2,4,50)], {3:.8})
res['missing_child_value_bounds'] = [.5*.8, .5*.8+.5]
assert res['missing_child_renormalization']['W'][1] == .8

res['tail_cutoff'] = stop('tail', [(1,2,20)], [(2,3,991),(2,4,9)], {3:1.,4:0.})
res['tail_uncut_value'] = .991
assert res['tail_cutoff']['W'][1] == 1.

fallback = [.6,.3,.1]
res['one_target_observation_stage'] = [(n+30*p)/31 for n,p in zip([0,1,0],fallback)]
res['only_one_online_game_final'] = ensemble(['a','b','c'], [0,1,0], [0,1,0], [0,1,0],
                                            [0,0,0], [.6,.3,.1], maia_support_gate=300.)
assert math.isclose(sum(res['only_one_online_game_final']), 1.)

leaf = [(1.,123,None,True,None,chess.STARTING_FEN,30,0.,0,0)]
valued = cv.value_leaves(leaf, True, lambda hs: np.full(len(hs), MISSING), None,
                       ((0.,.00368208),(0.,.00368208)))
res['legacy_no_eval_leaf'] = dict(score=float(valued['uniform'][0]),
                                no_eval=bool(valued['nocp'][0]))
assert res['legacy_no_eval_leaf'] == dict(score=.5,no_eval=True)

class Counts:
    def get(self, hs, rep): return [{} for _ in hs]
class Model:
    def probs(self, epds, se, oe, hs): return [{'e4':1.} for _ in hs]
b=chess.Board(); h=zobrist_int64(b)
lookup=lambda hs: np.array([12.75 if int(x)==h else MISSING for x in hs], dtype=np.float64)
starts=[(h,b.epd(),1.,0,12.75,0.,0.)]
args=SimpleNamespace(rollouts=1,max_ply=1,our_elo=1400,opp_elo=2000,k=1.,min_p=0.)
leaves,_=cv.rollouts(starts,{},True,0,args,1,Counts(),Counts(),Model(),lookup)
res['native_fractional_ancestor_cp'] = dict(input=12.75,output=leaves[0][2])
assert leaves[0][2] == 12
valued=cv.value_leaves(leaves,True,lookup,None,((0.,.00368208),(0.,.00368208)))
res['native_fractional_fallback_score']=float(valued['uniform'][0])

cp_lookup=lambda hs: np.full(len(hs),2000.,dtype=np.float64)
res['score_conversion_2000cp'] = dict(ordinary=float(EvalSource(True,cp_lookup)(np.array([1]))[0]),
                                     horizon=float(EvalSource(True,cp_lookup,boundary=True)(np.array([1]))[0]))

from sweep.verify import combine
from consumer_keys import stored_key
child=b.copy();child.push_san('e4')
def source_plan(cp, name):
    return dict(policy=dict(limit=.095,floor=.408,band=.01),colour='white',book_sha256=name*64,
        requests=[dict(key=stored_key(b,True),epd=b.epd(),moves=['e4'],done=[],previous_nodes=0,
          reason='independent-final-prescription',menu=[dict(san='e4',child=stored_key(child,True),
                                                             total=12,stored_cp=cp)])])
try:
    combine([source_plan(40.,'a'),source_plan(60.,'b')])
except ValueError as exc:
    res['paired_source_policy_menu_rejection']=str(exc)
else:
    raise AssertionError('paired source-policy difference was unexpectedly accepted')

import budget_core as bc
g=bc.Graph(root=1,our={},opp={},epd={},ply={},reach={})
g.our[1]=bc.OurNode(l_node=.25,cands=[('a',2,False,.75)])
bc.apply_stop_values(g,{1:.6},{2:.7})
res['complete_stop_overlay_replaces_initial_values']=dict(initial_node=.25,initial_edge=.75,
                                                         final_node=g.our[1].l_node,final_edge=g.our[1].cands[0][3])

res['source_hashes'] = {p:hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in
                       ['python/weak_stop.py','python/sweep/policy.py','python/convertibility_eval.py',
                        'rust/book_builder/src/py_rollout.rs','python/sweep/verify.py',
                        'rust/book_builder/src/core.rs']}
res['reproducer_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
(OUT/'results.json').write_text(json.dumps(res,indent=2)+'\n',encoding='utf8')
print(json.dumps(res,indent=2))
