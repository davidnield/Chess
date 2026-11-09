"""Time a gate sensitivity screen on every unique board of a finished Black panel."""
import argparse
from collections import Counter
import gzip
import json
from pathlib import Path
import time

from root_pilot_data import Data, successors
from budget_core import gate_candidates
from shared_slice_stops import Stops
from stage3_backwards_induction import cp_to_expected_score
from run_practical_rebuild import publish


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--panel",type=Path,required=True)
    ap.add_argument("--inputs",type=Path,default=Path("outputs/rebuild-2026-11-06/black/depth-policy/round-00"))
    ap.add_argument("--out",type=Path,required=True)
    a = ap.parse_args()
    t = time.perf_counter()
    with gzip.open(a.panel,"rt",encoding="utf8") as f:
        panel = json.load(f)
    boards = dict(panel["boards"])
    hs = sorted(set().union(*panel["layers"]))
    data = Data()
    stops = Stops(a.inputs,data)
    settings = {"baseline":(.10,.10,30),"absolute-tight":(.06,.10,30),"absolute-loose":(.14,.10,30),
                "relative-tight":(.10,.06,30),"relative-loose":(.10,.14,30),
                "cp-15":(.10,.10,15),"cp-60":(.10,.10,60)}
    stats = {key:Counter() for key in settings}
    for i in range(0,len(hs),384):
        children = {h:successors(boards[h]) for h in hs[i:i+384]}
        ids = set(children).union(*(set(cs) for cs in children.values()))
        data.evals(ids)
        ev = {h:cp_to_expected_score(data.cp[h]) for h in ids if data.cp[h] is not None}
        for h,cs in children.items():
            rows = [dict(child_hash=ch,move_san=s) for ch,(s,_) in cs.items()]
            for key,(absolute,relative,cp) in settings.items():
                kept = gate_candidates(h,rows,None,ev,False,1-stops.prior,absolute,relative,.02,True,True,cp)
                choice = sorted((row["child_hash"] for row,_ in kept),key=lambda ch:(data.cp[ch],cs[ch][0]))
                if key=="baseline":
                    base = choice
                stats[key].update(dict(changed_any=int(choice!=base),changed_top_two=int(choice[:2]!=base[:2]),
                                       blocked_all=int(not choice),moves=len(choice)))
    publish(a.out,dict(boards=len(hs),panel=panel["key"],seconds=time.perf_counter()-t,
                       settings=settings,counts=stats))


if __name__=="__main__":
    main()
