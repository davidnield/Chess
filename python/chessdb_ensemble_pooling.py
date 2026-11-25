"""Pool old/new selection evidence without touching workers or sealed confirmation.

No Stockfish is launched. Qualified deeper references replace old scores by full
position identity. The final confirmation panel contributes only exclusion keys.
"""
from __future__ import annotations
import argparse
import datetime as dt
import importlib.metadata
import json
import os
from pathlib import Path
import time

import chess
import numpy as np
import pandas as pd
import chessdb_ensemble_confirmation as confirmation
import chessdb_ensemble_study as study
import chessdb_policy_validation as ref
from month_archive import file_lock, read_json, sha, write_json
from zobrist128 import zobrist128

PROTOCOL='pooled-selection-v1'
MARKER='POOL_SELECTION_REQUIRED.json'


def code_identity():
    return dict(confirmation.code_identity(),**{Path(__file__).name:sha(Path(__file__))})


def emit(out,state,**kw):
    r=dict(state=state,utc=dt.datetime.now(dt.timezone.utc).isoformat(),pid=os.getpid(),**kw)
    write_json(out/'status.json',r);print(json.dumps(r),flush=True)


def initialize(base,confirmation_out,out):
    cfg=study.check_definition(base);cc=confirmation.check(confirmation_out)
    if Path(cc['base_study']).resolve()!=base.resolve():raise ValueError('confirmation belongs to another study')
    if out.exists():raise ValueError('new pooled-analysis output required')
    if any((confirmation_out/n).exists() for n in ('choice.json','OPENED.json',MARKER)):
        raise ValueError('confirmation already has a selection/opening protocol')
    sources={}
    for root,names in [(base,['definition.json','freeze.json','finalists.json','manifest.json','holdout-inputs.parquet']),
        (confirmation_out,['definition.json','freeze.json','manifest.json','confirmation-inputs.parquet']),
        (Path(cfg['targeted']),['manifest.json']),
        (Path(cfg['panel']),['panel_scored_sf18_d24.parquet','search_sf18_d24.jsonl'])]:
        for name in names:sources[str(root/name)]=sha(root/name)
    definition=dict(schema=1,protocol=PROTOCOL,code=code_identity(),sources=sources,
        study=str(base),confirmation=str(confirmation_out),targeted=cfg['targeted'],panel=cfg['panel'],
        catalog=cfg['catalog'],gates=cfg['gates'],baselines=cfg['baselines'],settings=cfg['settings'],runtime=cfg['runtime'],
        reference_rule='One canonical board; replace old reference only with qualified >=old requested depth+2 or terminal; never add child searches as rows',
        primary_weighting='Equal four NNUE triple-overlap strata; equal position weights within each stratum',
        sensitivity_weighting='Also equal old/new panel weights within each primary stratum',
        old_depth='Historical depth is REQUESTED, not measured actual depth',
        adopted=False)
    out.mkdir(parents=True);write_json(out/'definition.json',definition)
    write_json(out/'freeze.json',dict(definition_sha256=sha(out/'definition.json')))
    write_json(confirmation_out/MARKER,dict(protocol=PROTOCOL,pooling_output=str(out),
        definition_sha256=sha(out/'definition.json'),runner=str(Path(__file__).resolve()),
        instruction='Use pooled lock-choice and report-confirmation; retain existing engine workers and sealed data.'))
    emit(out,'initialized',extra_engine_searches=0,confirmation_scores_read=False)


def check(out):
    cfg=read_json(out/'definition.json')
    if (sha(out/'definition.json')!=read_json(out/'freeze.json')['definition_sha256'] or cfg['code']!=code_identity()
            or any(importlib.metadata.version(n)!=v for n,v in cfg.get('runtime',{}).items())):
        raise ValueError('pooling recipe/code identity changed')
    for p,h in cfg['sources'].items():
        if sha(Path(p))!=h:raise ValueError('pooling source identity changed')
    marker=read_json(Path(cfg['confirmation'])/MARKER)
    if marker['protocol']!=PROTOCOL or marker['definition_sha256']!=sha(out/'definition.json') or Path(marker['pooling_output']).resolve()!=out.resolve():
        raise ValueError('confirmation selection protocol changed')
    return cfg


def epd(fen):
    return chess.Board(fen).epd(en_passant='legal')


def annotate(old,new,old_refs,upgrades,new_refs,excluded):
    """Build position-level evidence only; returns accepted rows + complete audit."""
    records=[];key_boards={}
    for source,frame in [('old',old),('new',new)]:
        if frame.id.duplicated().any():raise ValueError('duplicate full key inside source panel')
        for row in frame.itertuples(index=False):
            r=row._asdict();ident=f'{int(row.lo)}:{int(row.hi)}'
            if row.id!=ident:raise ValueError('lossy or inconsistent full key')
            board=chess.Board(row.epd)
            if not board.is_valid() or zobrist128(board)!=(int(row.lo),int(row.hi)):
                raise ValueError('position key/FEN mismatch')
            canonical=study.canon(row.epd)
            if ident in key_boards and key_boards[ident]!=canonical:raise ValueError('full key collision')
            key_boards[ident]=canonical
            if canonical in excluded:raise ValueError('confirmation board leaked into selection inputs')
            info=dict(r,source_panel=source,canonical=canonical,reference_actual_depth=np.nan,
                reference_nodes=np.nan,reference_terminal=False,reference_qualified=True,reference_source='',
                upgrade_status='not-selected',reference_requested_depth=np.nan,old_reference_cp=np.nan)
            if source=='old':
                previous=old_refs[ident]
                if (not previous.get('terminal') and previous.get('engine')!='Stockfish 18') or epd(previous['fen'])!=epd(row.epd):
                    raise ValueError('old reference engine/FEN mismatch')
                if float(previous['cp_white'])!=float(row.sf):raise ValueError('old panel score differs from original reference')
                requested=int(previous['depth'])
                info.update(old_reference_cp=float(row.sf),reference_requested_depth=requested,
                    reference_nodes=previous.get('nodes'),reference_terminal=bool(previous.get('terminal')),
                    reference_source='old-requested-depth24')
                fresh=upgrades.get(ident)
                if fresh:
                    if fresh['task']['kind']!='parent' or epd(fresh['task']['fen'])!=epd(row.epd):
                        raise ValueError('upgrade is not the matching parent')
                    stronger=bool(fresh.get('terminal') or fresh.get('depth',0)>=requested+2)
                    if fresh['qualified'] and stronger:
                        info.update(sf=fresh['cp_white'],reference_actual_depth=fresh['depth'],
                            reference_nodes=fresh.get('nodes'),reference_terminal=bool(fresh.get('terminal')),
                            reference_requested_depth=fresh['task']['target_depth'],
                            reference_source='old-qualified-upgrade',upgrade_status='replaced-old')
                    else:info['upgrade_status']='unqualified-or-not-stronger; retained-old'
            else:
                fresh=new_refs[ident]
                if fresh['task']['kind']!='parent' or epd(fresh['task']['fen'])!=epd(row.epd):
                    raise ValueError('new reference parent/FEN mismatch')
                info.update(sf=fresh['cp_white'],reference_actual_depth=fresh['depth'],
                    reference_nodes=fresh.get('nodes'),reference_requested_depth=fresh['task']['target_depth'],
                    reference_terminal=bool(fresh.get('terminal')),reference_source='new-search',
                    reference_qualified=bool(fresh['qualified']),upgrade_status='not-applicable')
            records.append(info)
    audit=pd.DataFrame(records)
    audit['stratum']=study.strata(audit)
    # Missing Fishnet is distinct from old-generation Fishnet; gap rows are not
    # blended-source evidence and must not dilute fallback comparisons silently.
    no_fish=audit.fishnet.isna()
    audit.loc[no_fish & audit.cloud.isna(),'stratum']='chessdb_only'
    for depth in ('deep','shallow'):
        mask=no_fish & audit.cloud.notna() & audit.stratum.str.startswith(depth+'_')
        audit.loc[mask,'stratum']=depth+'_cloud_chessdb_only'
    audit['reference_tier']=np.where(audit.reference_actual_depth.notna(),
        np.where(audit.reference_actual_depth.ge(28),'actual-depth28plus','actual-depth26-27'),
        'requested-depth24-actual-unknown')
    audit.loc[audit.reference_terminal,'reference_tier']='terminal'
    audit.loc[~audit.reference_qualified,'reference_tier']='unqualified-new-search'
    eligible=audit[audit.reference_qualified].copy()
    # Prefer a qualified measured deeper reference; no reference is selected by
    # agreement with an ensemble. Mirrored spellings are also counted once.
    eligible['strength']=eligible.reference_actual_depth.fillna(eligible.reference_requested_depth)
    eligible=eligible.sort_values(['reference_terminal','strength','id'],ascending=[False,False,True])
    selected=eligible.drop_duplicates('canonical').drop(columns='strength').reset_index(drop=True)
    audit['retained']=False
    chosen=set(zip(selected.source_panel,selected.id))
    audit['retained']=[(r.source_panel,r.id) in chosen for r in audit.itertuples()]
    return selected,audit


def score_panels(d,cfg,out):
    old=d[d.source_panel=='old'].copy();new=d[d.source_panel=='new'].copy()
    old_original=old.copy();old_original['sf']=old_original.old_reference_cp
    original=d.copy();mask=original.source_panel=='old';original.loc[mask,'sf']=original.loc[mask,'old_reference_cp']
    panels={'pooled-best':d,'old-best':old,'new-only':new,'old-original':old_original,
            'pooled-old-original':original,'deep-only':d[d.reference_actual_depth.ge(26)|d.reference_terminal]}
    panels.update({f'reference-{name}':part for name,part in d.groupby('reference_tier')})
    tables={name:study.metrics(frame,cfg,name,out) for name,frame in panels.items()}
    comparisons=[]
    for name in ('pooled-best','old-best','new-only','old-original','pooled-old-original','deep-only'):
        table=tables[name]
        if len(table):
            primary=table[table.cohort=='all3_nnue'].copy()
            primary['rank']=primary.macro_mae_points.rank(method='min')
            comparisons.append(primary[['policy','macro_mae_points','p95_points','p99_points','n','rank']].set_index('policy').add_prefix(name+'__'))
    combined=pd.concat(comparisons,axis=1)
    q=d[d[study.SOURCES].notna().all(axis=1)&d.fishnet_tier.eq('nnue')]
    errors=100*abs(study.predict(q,cfg['catalog']).sub(ref.score(q.sf),axis=0))
    # Equal old/new panels *within each source stratum*, irrespective of their
    # sample sizes. This is a sensitivity, not the main ranking.
    means=errors.groupby([q.stratum,q.source_panel]).mean().groupby(level=0).mean().mean()
    combined['equal-panel-within-stratum__macro_mae_points']=means
    combined.to_csv(out/'ranking-comparison.csv')
    return combined


def completed_inputs(cfg):
    base=Path(cfg['study']);target=Path(cfg['targeted'])
    if any(read_json(root/'status.json')['state']!='complete' for root in (base,target)):
        raise ValueError('selection reports require both complete studies; no interim pooling')
    bm=read_json(base/'manifest.json');tm=read_json(target/'manifest.json')
    for m in (bm,tm):
        for p,h in m['sources'].items():
            if sha(Path(p))!=h:raise ValueError('reference input identity drift')
    br=ref.load_results(base,bm);tr=ref.load_results(target,tm)
    if len(br)!=len(bm['tasks']) or len(tr)!=len(tm['tasks']):raise ValueError('incomplete durable reference set')
    return br,tr


def build(out,cfg):
    base=Path(cfg['study']);target=Path(cfg['targeted']);panel=Path(cfg['panel']);conf=Path(cfg['confirmation'])
    br,tr=completed_inputs(cfg)
    old=pd.read_parquet(panel/'panel_scored_sf18_d24.parquet')
    new=pd.read_parquet(base/'holdout-inputs.parquet')
    # Only public selection keys/FENs are read from the confirmation panel.
    blocked={study.canon(f) for f in pd.read_parquet(conf/'confirmation-inputs.parquet',columns=['epd']).epd}
    raw=[json.loads(line) for line in (panel/'search_sf18_d24.jsonl').read_text().splitlines() if line.strip()]
    old_refs={r['id']:r for r in raw}
    if len(raw)!=len(old_refs):raise ValueError('duplicate old reference records')
    upgrades={k:r for k,r in tr.items() if r['task']['kind']=='parent'}
    if not set(upgrades).issubset(set(old.id)):raise ValueError('upgrade absent from old panel')
    d,audit=annotate(old,new,old_refs,upgrades,br,blocked)
    d.to_parquet(out/'pooled-positions.parquet',index=False)
    audit.to_parquet(out/'reference-audit.parquet',index=False)
    audit.groupby(['source_panel','reference_source','reference_tier','reference_qualified','retained']).size().rename('n').to_csv(out/'reference-coverage.csv')
    ranking=score_panels(d,cfg,out)
    primary=d[d[study.SOURCES].notna().all(axis=1)&d.fishnet_tier.eq('nnue')]
    if set(primary.stratum)!=set(confirmation.QUOTAS):raise ValueError('primary selection strata incomplete')
    primary.groupby(['source_panel','stratum','reference_tier']).size().rename('n').to_csv(out/'primary-cohort-counts.csv')
    singles=ranking.loc[confirmation.SINGLES].sort_values('pooled-best__macro_mae_points')
    summary=dict(input_old=len(old),input_new=len(new),retained=len(d),
        retained_by_panel=d.source_panel.value_counts().to_dict(),
        all_three=int(d[study.SOURCES].notna().all(axis=1).sum()),nnue_all_three=len(primary),
        qualified_old_upgrades=int((audit.upgrade_status=='replaced-old').sum()),
        old_rechecks_retained_old=int(audit.upgrade_status.str.startswith('unqualified-or-not-stronger').sum()),
        excluded_unqualified_new=int((~audit.reference_qualified).sum()),
        deduplicated_rows=int(audit.reference_qualified.sum()-len(d)),
        best_single_pooled=singles.index[0],confirmation_scores_read=False,adopted=False,
        notes=['Pooled/new-only/old-only rankings are selection evidence, not independent final confirmation.',
        'Old references report requested depth; actual old completed depths were not recorded.',
        'Old deeper rechecks were targeted; deep-only and cohort sensitivities are not unbiased population estimates.',
        'Primary ranking gives equal weight to four NNUE triple-overlap strata, equal rows within each stratum.',
        'Gap rows and missing-Fishnet cases are separated in fallback strata; no coverage mixing in complete-case comparisons.',
        'The final 300 positions remain excluded; gate and tail tradeoffs still require judgment.'])
    write_json(out/'summary.json',summary)
    inputs=dict(cfg['sources'])
    for root in (base,target):
        for p in (root/'results').glob('*.json'):inputs[str(p)]=sha(p)
    for name in ('summary.json','holdout-metrics.csv','holdout-gates.csv','holdout-paired.csv','reference-qualification.csv'):
        inputs[str(base/name)]=sha(base/name)
    outputs=[p for p in out.iterdir() if p.suffix in ('.csv','.parquet') or p.name=='summary.json']
    receipt=dict(protocol=PROTOCOL,definition_sha256=sha(out/'definition.json'),
        inputs=inputs,outputs={str(p):sha(p) for p in outputs},confirmation_scores_read=False)
    write_json(out/'COMPLETE.json',receipt)
    emit(out,'complete',retained=len(d),nnue_all_three=len(primary),adopted=False)


def verify_complete(out):
    cfg=check(out);receipt=read_json(out/'COMPLETE.json')
    if receipt['protocol']!=PROTOCOL or receipt['definition_sha256']!=sha(out/'definition.json'):
        raise ValueError('pooled receipt identity mismatch')
    for p,h in {**receipt['inputs'],**receipt['outputs']}.items():
        if sha(Path(p))!=h:raise ValueError('pooled evidence checksum mismatch')
    return cfg,receipt


def run(out):
    cfg=check(out)
    with file_lock(out/'run.lock'):
        try:
            if (out/'COMPLETE.json').exists():verify_complete(out);return
            while True:
                if (out/'STOP').exists():emit(out,'stopped');return
                state=read_json(Path(cfg['study'])/'status.json')
                if state['state']=='complete':break
                if state['state'] in ('error','stopped','stopped-or-bounded'):raise ValueError('preceding study stopped/failed')
                emit(out,'waiting-for-study',preceding_state=state['state'],confirmation_scores_read=False)
                time.sleep(30)
            build(out,cfg)
        except BaseException as exc:emit(out,'error',error=repr(exc));raise


def lock_choice(out,contender,rationale):
    cfg,receipt=verify_complete(out);conf=Path(cfg['confirmation']);confirmation.check(conf)
    with file_lock(conf/'choice.lock'):
        if (conf/'choice.json').exists() or (conf/'OPENED.json').exists():raise ValueError('confirmation choice already locked/opened')
        recipes={r['name']:r for r in cfg['catalog']}
        if contender not in recipes:raise ValueError('contender absent from frozen suite')
        text=rationale.read_text(encoding='utf-8').strip()
        if not text:raise ValueError('selection rationale required')
        table=pd.read_csv(out/'pooled-best-metrics.csv')
        solo=table[(table.cohort=='all3_nnue')&table.policy.isin(confirmation.SINGLES)]
        if len(solo)!=3 or set(solo.policy)!=set(confirmation.SINGLES):raise ValueError('incomplete pooled single-source scores')
        single=solo.sort_values(['macro_mae_points','policy']).iloc[0].policy
        evidence={**receipt['inputs'],**receipt['outputs'],str(out/'COMPLETE.json'):sha(out/'COMPLETE.json'),
                  str(conf/MARKER):sha(conf/MARKER)}
        selected=list(dict.fromkeys([contender,'proposal',single]))
        payload=dict(selection_protocol=PROTOCOL,pooling_output=str(out),
            confirmation_manifest_sha256=sha(conf/'manifest.json'),locked_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
            contender=contender,best_single=single,comparators=['proposal',single],recipes=[recipes[n] for n in selected],
            rationale=text,rationale_source_sha256=sha(rationale),preceding_evidence=evidence,
            selection_uses_confirmation_labels=False)
        with (conf/'choice.json').open('x',encoding='utf-8') as f:
            json.dump(dict(payload=payload,sha256=ref.digest(payload)),f,indent=2);f.flush();os.fsync(f.fileno())
        print(json.dumps(dict(choice_locked=True,selection_protocol=PROTOCOL,contender=contender,best_single=single,scores_opened=False)))


def report_confirmation(out):
    cfg,_=verify_complete(out);conf=Path(cfg['confirmation'])
    choice=confirmation.checked_choice(conf)
    if choice.get('selection_protocol')!=PROTOCOL or Path(choice.get('pooling_output','')).resolve()!=out.resolve():
        raise ValueError('confirmation choice is not bound to this pooled selection')
    confirmation.report(conf)


def main():
    ap=argparse.ArgumentParser(description=__doc__);sub=ap.add_subparsers(dest='command',required=True)
    p=sub.add_parser('initialize')
    for n in ('base','confirmation-out','out'):p.add_argument('--'+n,type=Path,required=True)
    for cmd in ('run','lock-choice','report-confirmation'):
        p=sub.add_parser(cmd);p.add_argument('--out',type=Path,required=True)
        if cmd=='lock-choice':p.add_argument('--contender',required=True);p.add_argument('--rationale',type=Path,required=True)
    args=vars(ap.parse_args());cmd=args.pop('command').replace('-','_');globals()[cmd](**args)


if __name__=='__main__':main()
