"""Analyze the completed SF18 panel and OTB exposure; no model or gate changes.

python chessdb_validation.py --panel E:/chess/eval-panel-20261122 --out NEW_DIR
Reports describe this stratified sample and its top-three ChessDB candidate menu,
not a reach-weighted repertoire test or a proof of engine truth.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd
import duckdb
from month_archive import sha,write_json
from sweep.spec import BASELINES,GATES

def score(cp):return 1/(1+np.exp(-.00368208*np.asarray(cp,dtype=float)))
def summary(v):
    v=np.asarray(v,dtype=float); v=v[np.isfinite(v)]
    return dict(n=len(v),mean=float(v.mean()),median=float(np.median(v)),
                p95=float(np.quantile(v,.95)),p99=float(np.quantile(v,.99)),maximum=float(v.max())) if len(v) else dict(n=0)

def analyze(panel,out):
    panel=Path(panel);out=Path(out)
    if out.exists():raise ValueError("use a new report directory")
    out.mkdir(parents=True)
    d=pd.read_parquet(panel/"panel_scored_sf18_d24.parquet")
    m=pd.read_parquet(panel/"panel_moves.parquet")
    raw=pd.read_json(panel/"search_sf18_d24.jsonl",lines=True)
    if d.sf.isna().any():raise ValueError("missing SF18 reference")
    refs=dict(zip(raw.id,raw.cp_white))
    m["id"]=m.lo.astype(str)+":"+m.hi.astype(str)+":"+m["rank"].astype(str)
    m["sf"]=m.id.map(refs)
    if m.sf.isna().any():raise ValueError("missing child reference search")
    m=m.merge(d[["lo","hi","grp","white_to_move","sf","chessdb"]].rename(columns={"sf":"parent_sf","chessdb":"parent_cdb"}),on=["lo","hi"],validate="many_to_one")
    m["sign"]=np.where(m.white_to_move,1,-1)
    m["cdb"]=np.clip(np.where(m.score.abs()>=15000,np.sign(m.score)*2000,m.score),-2000,2000)*m.sign
    rows=[]
    for grp,x in d.groupby("grp"):
        for source in ["current","cloud","fishnet","chessdb","rule_13nov"]:
            y=x.dropna(subset=[source,"sf"])
            if not len(y):continue
            rows.append(dict(group=grp,source=source,metric="absolute_cp",**summary(abs(y[source]-y.sf))))
            rows.append(dict(group=grp,source=source,metric="absolute_score_points",**summary(100*abs(score(y[source])-score(y.sf)))))
    pd.DataFrame(rows).to_csv(out/"source-errors.csv",index=False)
    gate_rows=[]
    for gate,settings in GATES.items():
        for colour in ["white","black"]:
            sign=1 if colour=="white" else -1
            baseline=BASELINES[colour] if colour=="white" else 1-BASELINES[colour]
            floor=baseline-settings[colour]
            for source in ["current","cloud","fishnet","chessdb","rule_13nov"]:
                y=d.dropna(subset=[source,"sf"])
                a=score(sign*y[source]); ref=score(sign*y.sf)
                gate_rows.append(dict(gate=gate,colour=colour,source=source,n=len(y),floor=floor,
                  falsely_admitted=int(((a>=floor)&(ref<floor)).sum()),
                  falsely_excluded=int(((a<floor)&(ref>=floor)).sum()),
                  within_one_point=int((abs(ref-floor)<=.01).sum())))
    pd.DataFrame(gate_rows).to_csv(out/"absolute-gates.csv",index=False)
    candidates=[];regret=[];gaps=[];ties=dict(both_tied=0,cdb_tied=0,reference_tied=0,strict_agree=0,strict_disagree=0)
    for key,x in m.groupby(["lo","hi"]):
        x=x.sort_values("rank").dropna(subset=["sf"])
        if len(x)<1:continue
        sign=x.sign.iloc[0]; colour="white" if sign==1 else "black"
        cs=score(sign*x.cdb); rs=score(sign*x.sf)
        cp=sign*x.cdb.to_numpy(); rp=sign*x.sf.to_numpy()
        regret.append(dict(lo=int(key[0]),hi=int(key[1]),grp=x.grp.iloc[0],cp=max(0,float(rp.max()-rp[0])),score_points=100*max(0,float(rs.max()-rs[0])),moves=len(x)))
        if len(x)>=2:
            cd=cp[0]-cp[1];rd=rp[0]-rp[1]
            tag="both_tied" if cd==rd==0 else "cdb_tied" if cd==0 else "reference_tied" if rd==0 else "strict_agree" if cd*rd>0 else "strict_disagree"
            ties[tag]+=1
            a=np.sort(cs)[::-1];b=np.sort(rs)[::-1]
            gaps.append(dict(lo=int(key[0]),hi=int(key[1]),gap2_error_points=100*abs((a[0]-a[1])-(b[0]-b[1]))))
        for gate,settings in GATES.items():
            baseline=BASELINES[colour] if sign==1 else 1-BASELINES[colour]
            floor=baseline-settings[colour];rel=settings["relative"]
            cbest=max(float(cs.max()),float(score(sign*x.parent_cdb.iloc[0])))
            rbest=max(float(rs.max()),float(score(sign*x.parent_sf.iloc[0])))
            ca=(cs>=floor)&((cbest-cs)<=rel);ra=(rs>=floor)&((rbest-rs)<=rel)
            for i,(_,row) in enumerate(x.iterrows()):
                candidates.append(dict(lo=int(key[0]),hi=int(key[1]),uci=row.uci,gate=gate,colour=colour,
                  cdb_admitted=bool(ca[i]),sf_admitted=bool(ra[i]),sf_relative_loss=float(rbest-rs[i]),sf_absolute_score=float(rs[i])))
    pd.DataFrame(candidates).to_csv(out/"candidate-gates.csv",index=False)
    pd.DataFrame(regret).to_csv(out/"candidate-regret.csv",index=False)
    pd.DataFrame(gaps).to_csv(out/"candidate-gaps.csv",index=False)
    # Cloud rows for exactly the searched positions/moves; full-key join.
    c=duckdb.connect();c.execute("SET threads=2");c.execute("SET memory_limit='1GB'")
    wanted=m[["lo","hi","uci"]].drop_duplicates();c.register("wanted",wanted)
    files=[f"E:/chess/chessdb-menus-20260702/cloud_moves/bkt{b:03d}.parquet" for b in sorted(d.bucket.unique())]
    cloud=c.execute("SELECT c.* FROM read_parquet(?) c JOIN wanted w USING(lo,hi,uci)",[files]).df()
    cloud=cloud.merge(m[["lo","hi","uci","sf","sign"]],on=["lo","hi","uci"],validate="one_to_one")
    cloud["cp"]=np.clip(cloud.cp_white.fillna(np.sign(cloud.mate_white.astype(float))*2000),-2000,2000)
    cloud_gaps=[]
    for key,x in cloud.groupby(["lo","hi"]):
        if len(x)<2:continue
        a=np.sort(score(x.sign*x.cp))[::-1];b=np.sort(score(x.sign*x.sf))[::-1]
        cloud_gaps.append(dict(lo=int(key[0]),hi=int(key[1]),moves=len(x),different_depth_or_knodes=len(x[["depth","knodes"]].drop_duplicates())>1,
                               gap2_error_points=100*abs((a[0]-a[1])-(b[0]-b[1]))))
    pd.DataFrame(cloud_gaps).to_csv(out/"cloud-shared-move-gaps.csv",index=False)
    cloud.to_parquet(out/"cloud-searched-moves.parquet",index=False)
    origin=c.execute("""SELECT role,assignment_origin,count(*) AS games FROM read_parquet(
      'D:/chess/sweep-inputs-20261119/otb-v2-20261119/games.parquet') GROUP BY ALL""").df()
    origin.to_csv(out/"otb-role-origins.csv",index=False)
    detail=dict(ties=ties,regret_cp=summary([r["cp"] for r in regret]),
       regret_score_points=summary([r["score_points"] for r in regret]),
       cdb_gap2_error_points=summary([r["gap2_error_points"] for r in gaps]),
       cloud_gap2_error_points=summary([r["gap2_error_points"] for r in cloud_gaps]),
       cloud_pairs=len(cloud_gaps),cloud_mixed_depth_or_knodes=sum(r["different_depth_or_knodes"] for r in cloud_gaps),
       source_identities={p.name:sha(p) for p in [panel/"panel_scored_sf18_d24.parquet",panel/"panel_moves.parquet",panel/"search_sf18_d24.jsonl"]})
    write_json(out/"summary.json",detail)
    lines=["# SF18 panel and takeover validation","",
      "This is a stratified diagnostic on 1,500 ChessDB-covered positions. It is not a reach-weighted repertoire result.",
      "Gate comparisons use the approved fixed per-colour baselines and expected-score tolerances from sweep.spec.",
      "Candidate gates compare the same searched ChessDB top-three menu, using max(parent reference, best searched child); they do not reproduce the full construction graph.",
      "CP values are capped at +/-2000. Reference engine uncertainty remains, especially near cutoffs.","",
      "## Move ordering and regret","",json.dumps(detail,indent=2),"",
      "## Interpretation constraints","",
      "- Identical cloud depth/knodes do not prove the PV rows share an analysis; differing values prove they cannot all be one shared analysis budget.",
      "- Cloud gap results are restricted to moves with SF18 child references in this panel. Missing cloud candidates remain untested.",
      "- Historical OTB test ancestry is not a fresh confirmation set. Earlier analysis used historical split=0 both for tuning and confirmation.",
      "- New-family test games are only a candidate fresh cohort until an exposure audit confirms they were never used in model selection.",
      "- Freeze the gate-300 versus ungated comparison and metrics before examining that cohort's outcomes. Preserve families and report sparse-support strata.",
      "- Do not tune source blend weights or threshold choices on these diagnostics and then call the same observations an independent test.",
      "- No source-policy change, pool launch, or book adoption is performed by this report.",""]
    (out/"report.md").write_text("\n".join(lines),encoding="utf8")
    print(json.dumps(detail,indent=2))

if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("--panel",required=True,type=Path);p.add_argument("--out",required=True,type=Path);a=p.parse_args();analyze(a.panel,a.out)
