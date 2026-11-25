"""Separate blinded confirmation panel for a final evaluation-source contender.

The worker writes reference scores only under sealed/. No policy comparisons or
score previews are produced until lock-choice records an immutable selection
based on the preceding study. This is a workflow blind, not encrypted storage.
"""
from __future__ import annotations
import argparse
import concurrent.futures as cf
import copy
import datetime as dt
import importlib.metadata
import json
import os
from pathlib import Path
import time

import chess
import numpy as np
import pandas as pd
import chessdb_ensemble_study as study
import chessdb_policy_validation as ref
from month_archive import file_lock, read_json, sha, write_json
from zobrist128 import zobrist128

HERE=Path(__file__).resolve().parent
BUCKET_SEED=202611252
QUOTAS={f'{d}_{n}':75 for d in ('deep','shallow') for n in ('once','repeated')}
SINGLES=['cp_10_0_0','cp_0_10_0','cp_0_0_10']


def code_identity():
    return dict(study.code_identity(), **{Path(__file__).name:sha(Path(__file__))})


def emit(out,state,**kw):
    # Callers must pass operational counters only, never scores/PVs or rankings.
    obj=dict(state=state,utc=dt.datetime.now(dt.timezone.utc).isoformat(),pid=os.getpid(),**kw)
    write_json(out/'status.json',obj);print(json.dumps(obj),flush=True)


def initialize(base,out):
    cfg0=study.check_definition(base)
    bm=read_json(base/'manifest.json')
    for p,h in bm['sources'].items():
        if sha(Path(p))!=h:raise ValueError('base sample identity changed')
    if out.exists():raise ValueError('new confirmation directory required')
    old=pd.read_parquet(base/'holdout-inputs.parquet')
    blocked=set(cfg0['blocked_fens'])|{study.canon(f) for f in old.epd}
    old_buckets=set(cfg0['buckets'])|set(read_json(Path(cfg0['panel'])/'panel.meta.json')['buckets'])
    buckets=sorted(set(range(512))-old_buckets,key=lambda b:ref.digest([BUCKET_SEED,b]))[:8]
    cfg=copy.deepcopy(cfg0)
    cfg.update(kind='sealed-confirmation',code=code_identity(),base_study=str(base),
        bucket_selection_seed=BUCKET_SEED,within_bucket_seed=study.SEED,
        quotas=QUOTAS,buckets=buckets,per_bucket_keep=150,blocked_fens=sorted(blocked),
        interpretation='300 modern-Fishnet three-source overlaps, 75 per stratum; no opening/reach independence claim',
        blind='Reference scores remain in sealed/ until an immutable choice is recorded; no automatic report',
        created_utc=dt.datetime.now(dt.timezone.utc).isoformat())
    for name in ('definition.json','freeze.json','finalists.json','manifest.json','holdout-inputs.parquet'):
        cfg['sources'][str(base/name)]=sha(base/name)
    out.mkdir(parents=True)
    write_json(out/'definition.json',cfg)
    write_json(out/'freeze.json',dict(definition_sha256=sha(out/'definition.json')))
    emit(out,'initialized',planned_positions=sum(QUOTAS.values()),scores_opened=False)


def check(out):
    cfg=read_json(out/'definition.json')
    if sha(out/'definition.json')!=read_json(out/'freeze.json')['definition_sha256'] or cfg['code']!=code_identity():
        raise ValueError('confirmation definition/code drift')
    if any(importlib.metadata.version(n)!=v for n,v in cfg.get('runtime',{}).items()):
        raise ValueError('confirmation runtime drift')
    for p,h in cfg['sources'].items():
        if sha(Path(p))!=h:raise ValueError('confirmation source identity drift')
    return cfg


def select_panel(d,blocked,quotas):
    d=d[d.stratum.isin(quotas)].copy()
    if not d[study.SOURCES].notna().all().all() or not d.fishnet_tier.eq('nnue').all():
        raise ValueError('confirmation requires all three sources and NNUE Fishnet')
    d['canonical']=[study.canon(f) for f in d.epd]
    d=d[~d.canonical.isin(blocked)].sort_values(['sample_priority','id']).drop_duplicates('canonical')
    parts=[]
    for group,quota in quotas.items():
        part=d[d.stratum==group].head(quota)
        if len(part)!=quota:raise ValueError(f'confirmation quota shortfall: {group} {len(part)}/{quota}')
        parts.append(part)
    selected=pd.concat(parts,ignore_index=True).sort_values(['sample_priority','id']).reset_index(drop=True)
    if selected[['lo','hi']].duplicated().any():raise ValueError('confirmation duplicate key')
    for r in selected.itertuples():
        b=chess.Board(r.epd)
        if (not b.is_valid() or b.is_game_over() or b.turn!=bool(r.white_to_move)
                or zobrist128(b)!=(int(r.lo),int(r.hi))):
            raise ValueError('confirmation FEN/key/legality mismatch')
    return selected


def sample(out,cfg,max_buckets=None):
    new=0
    for b in cfg['buckets']:
        p=out/'sample-buckets'/f'b{b:03d}.parquet';receipt=p.with_suffix('.json')
        if receipt.exists():
            if sha(p)!=read_json(receipt)['output_sha256']:raise ValueError('sample checkpoint corrupt')
            continue
        if (out/'STOP').exists() or (max_buckets is not None and new>=max_buckets):
            emit(out,'stopped-or-bounded-sampling',new_buckets=new);return False
        emit(out,'sampling',bucket=b)
        r=study.sample_bucket(out,cfg,b);new+=1
        emit(out,'sampled-bucket',bucket=b,candidates=r['rows'])
    if (out/'manifest.json').exists():return True
    d=pd.concat([pd.read_parquet(out/'sample-buckets'/f'b{b:03d}.parquet') for b in cfg['buckets']],ignore_index=True)
    d=select_panel(d,set(cfg['blocked_fens']),cfg['quotas'])
    dest=out/'confirmation-inputs.parquet';d.to_parquet(dest,index=False)
    tasks=[dict(id=r.id,parent=r.id,kind='parent',fen=r.epd+' 0 1',
                target_depth=cfg['settings']['target_depth'],minimum_depth=cfg['settings']['minimum_depth'])
           for r in d.itertuples()]
    files=[out/'definition.json',out/'freeze.json',dest]
    files += [out/'sample-buckets'/f'b{b:03d}.json' for b in cfg['buckets']]
    manifest=dict(schema=1,code=cfg['code'],engine=cfg['engine'],engine_sha256=cfg['engine_sha256'],
                  settings=cfg['settings'],tasks=tasks,sources={str(p):sha(p) for p in files})
    write_json(out/'manifest.json',manifest)
    write_json(out/'sample-summary.json',dict(total=len(d),quotas=d.stratum.value_counts().to_dict(),
        duplicate_keys=0,duplicate_canonical_boards=0,known_board_overlap=0,
        ply=d.first_ply.describe().to_dict(),historical_games=d.games.describe().to_dict(),
        inputs_sha256=sha(dest),manifest_sha256=sha(out/'manifest.json')))
    emit(out,'sample-ready',total=len(d),counts=d.stratum.value_counts().to_dict(),scores_opened=False)
    return True


def validate_manifest(out,cfg):
    m=read_json(out/'manifest.json')
    if m['code']!=cfg['code'] or sha(Path(m['engine']))!=m['engine_sha256']:
        raise ValueError('confirmation engine/manifest drift')
    for p,h in m['sources'].items():
        if sha(Path(p))!=h:raise ValueError('confirmation manifest input drift')
    return m


def search_sealed(out,max_new=None,search=ref.search_one):
    cfg=check(out);m=validate_manifest(out,cfg)
    sealed=out/'sealed';(sealed/'results').mkdir(parents=True,exist_ok=True)
    with file_lock(out/'reference.lock'):
        results=ref.load_results(sealed,m)
        todo=[t for t in m['tasks'] if t['id'] not in results]
        if max_new is not None:todo=todo[:max_new]
        todo=iter(todo);exhausted=False
        with cf.ThreadPoolExecutor(m['settings']['workers']) as pool:
            live={}
            while live or not exhausted:
                while len(live)<m['settings']['workers'] and not exhausted and not (out/'STOP').exists():
                    task=next(todo,None)
                    if task is None:exhausted=True;break
                    live[pool.submit(search,task,m)]=task
                if not live:break
                done,_=cf.wait(live,timeout=10,return_when=cf.FIRST_COMPLETED)
                for f in done:
                    task=live.pop(f);r=dict(f.result(),task=task);r['qualified']=ref.qualified(task,r)
                    write_json(sealed/'results'/f"{ref.digest(task['id'])}.json",
                               dict(identity=ref.digest(m),payload=r,sha256=ref.digest(r)))
                    results[task['id']]=r
                emit(out,'searching-sealed',completed=len(results),total=len(m['tasks']),active=len(live),
                     qualified=sum(r['qualified'] for r in results.values()),scores_opened=False)
        complete=len(results)==len(m['tasks'])
        if complete:
            write_json(out/'SEALED.COMPLETE.json',dict(identity=ref.digest(m),total=len(results),
                qualified=sum(r['qualified'] for r in results.values()),
                files={p.name:sha(p) for p in sorted((sealed/'results').glob('*.json'))}))
        emit(out,'sealed-complete' if complete else 'stopped-or-bounded',completed=len(results),
             total=len(m['tasks']),scores_opened=False)
        return complete


def pipeline(out,max_buckets=None):
    cfg=check(out)
    with file_lock(out/'pipeline.lock'):
        try:
            if not sample(out,cfg,max_buckets):return
            base=Path(cfg['base_study'])
            while True:
                if (out/'STOP').exists():emit(out,'stopped');return
                state=read_json(base/'status.json')
                if state['state']=='complete':break
                if state['state'] in ('error','stopped','stopped-or-bounded'):
                    raise ValueError('preceding study stopped/failed; inspect before resume')
                emit(out,'waiting-for-study',preceding_state=state['state'],scores_opened=False)
                time.sleep(30)
            search_sealed(out)
            # Intentionally no analytical report or automatic contender selection.
        except BaseException as e:
            emit(out,'error',error=repr(e));raise


def lock_choice(out,contender,rationale):
    """Reads only preceding study evidence, never sealed confirmation scores."""
    cfg=check(out);base=Path(cfg['base_study'])
    with file_lock(out/'choice.lock'):
        if (out/'choice.json').exists():raise ValueError('choice already locked; cannot change after selection')
        if read_json(base/'status.json')['state']!='complete':raise ValueError('preceding study is not complete')
        recipes={r['name']:r for r in cfg['catalog']}
        if contender not in recipes:raise ValueError('contender not in frozen candidate suite')
        text=rationale.read_text(encoding='utf-8').strip()
        if not text:raise ValueError('written selection rationale required')
        table=pd.read_csv(base/'holdout-metrics.csv')
        solo=table[(table.cohort=='all3_nnue') & table.policy.isin(SINGLES)]
        if set(solo.policy)!=set(SINGLES) or len(solo)!=3:raise ValueError('missing single-source comparison')
        single=solo.sort_values(['macro_mae_points','policy']).iloc[0].policy
        chosen=list(dict.fromkeys([contender,'proposal',single]))
        evidence=[base/n for n in ('summary.json','holdout-metrics.csv','holdout-gates.csv',
                   'holdout-paired.csv','reference-qualification.csv')]
        payload=dict(confirmation_manifest_sha256=sha(out/'manifest.json'),
            locked_utc=dt.datetime.now(dt.timezone.utc).isoformat(),contender=contender,
            best_single=single,comparators=['proposal',single],recipes=[recipes[n] for n in chosen],
            rationale=text,rationale_source_sha256=sha(rationale),
            preceding_evidence={str(p):sha(p) for p in evidence},selection_uses_confirmation_labels=False)
        # file lock + exclusive creation prevents later automatic re-selection.
        with (out/'choice.json').open('x',encoding='utf-8') as f:
            json.dump(dict(payload=payload,sha256=ref.digest(payload)),f,indent=2);f.flush();os.fsync(f.fileno())
        print(json.dumps(dict(choice_locked=True,contender=contender,best_single=single,scores_opened=False)))
        return payload


def checked_choice(out):
    if not (out/'choice.json').exists():raise ValueError('lock-choice is required before opening confirmation results')
    receipt=read_json(out/'choice.json');c=receipt['payload']
    if receipt['sha256']!=ref.digest(c) or c['confirmation_manifest_sha256']!=sha(out/'manifest.json'):
        raise ValueError('locked choice identity drift')
    for p,h in c['preceding_evidence'].items():
        if sha(Path(p))!=h:raise ValueError('preceding selection evidence changed')
    return c


def report(out):
    cfg=check(out)
    with file_lock(out/'report.lock'):
        choice=checked_choice(out)  # Must precede ANY access to sealed results.
        m=validate_manifest(out,cfg)
        completion=read_json(out/'SEALED.COMPLETE.json')
        if completion['identity']!=ref.digest(m):raise ValueError('sealed completion identity mismatch')
        for name,h in completion['files'].items():
            if sha(out/'sealed'/'results'/name)!=h:raise ValueError('sealed result checksum mismatch')
        results=ref.load_results(out/'sealed',m)
        if len(results)!=len(m['tasks']):raise ValueError('all confirmation searches must finish before reporting')
        opened=dict(choice_sha256=sha(out/'choice.json'),manifest_sha256=sha(out/'manifest.json'),
                    sealed_completion_sha256=sha(out/'SEALED.COMPLETE.json'))
        if (out/'OPENED.json').exists() and read_json(out/'OPENED.json')!=opened:
            raise ValueError('confirmation already opened under different identities')
        write_json(out/'OPENED.json',opened)
        d=pd.read_parquet(out/'confirmation-inputs.parquet')
        d['sf']=d.id.map({k:r['cp_white'] for k,r in results.items()})
        d['depth']=d.id.map({k:r['depth'] for k,r in results.items()})
        d['qualified']=d.id.map({k:r['qualified'] for k,r in results.items()})
        dest=out/'report';dest.mkdir(exist_ok=True)
        d.to_parquet(dest/'confirmation-scored.parquet',index=False)
        q=d[d.qualified].copy();cfg=copy.deepcopy(cfg);cfg['catalog']=choice['recipes']
        study.metrics(q,cfg,'confirmation',dest)
        study.metrics(q[q.depth>=28],cfg,'confirmation-depth28',dest)
        quality=d.groupby('stratum').agg(n=('id','size'),qualified=('qualified','sum'),
                                        depth28=('depth',lambda x:int((x>=28).sum())))
        quality.to_csv(dest/'qualification.csv')
        sufficient=set(q.stratum)==set(cfg['quotas'])
        pairs=[]
        if sufficient:
            p=study.predict(q,cfg['catalog']);err=100*abs(p.sub(ref.score(q.sf),axis=0))
            bases=[b for b in dict.fromkeys(choice['comparators']) if b!=choice['contender']]
            tail=.025/max(1,len(bases));rng=np.random.default_rng(BUCKET_SEED)
            for b in bases:
                delta=err[choice['contender']]-err[b];boot=[]
                for _,g in q.groupby('stratum'):
                    z=delta.loc[g.index].to_numpy()
                    boot.append(z[rng.integers(0,len(z),size=(cfg['settings']['bootstrap'],len(z)))].mean(axis=1))
                means=np.mean(boot,axis=0)
                pairs.append(dict(contender=choice['contender'],baseline=b,n=len(q),
                    delta_macro_mae_points=float(study.macro(delta,q.stratum).iloc[0]),
                    ci_low=float(np.quantile(means,.025)),ci_high=float(np.quantile(means,.975)),
                    simultaneous_ci_low=float(np.quantile(means,tail)),
                    simultaneous_ci_high=float(np.quantile(means,1-tail))))
        pd.DataFrame(pairs).to_csv(dest/'paired.csv',index=False)
        summary=dict(total=len(d),qualified=len(q),all_strata_represented=sufficient,choice=choice,
            paired=pairs,adopted=False,limits=[
            'Only the pre-locked contender and controls are tested; this is not another tuning leaderboard.',
            '300 source-stratified unique positions are not opening-family independent or reach weighted.',
            'Rare errors and tiny differences may remain unresolved; engine references remain approximate.',
            'No pooling of selection and confirmation panels to claim independent confirmation.',
            'Full blended-child gates and book effects still require the Black pilot.'])
        write_json(dest/'summary.json',summary)
        emit(out,'confirmation-reported',total=len(d),qualified=len(q),adopted=False,scores_opened=True)


def main():
    ap=argparse.ArgumentParser(description=__doc__);sub=ap.add_subparsers(dest='command',required=True)
    p=sub.add_parser('initialize');p.add_argument('--base',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
    p=sub.add_parser('pipeline');p.add_argument('--out',type=Path,required=True);p.add_argument('--max-buckets',type=int)
    p=sub.add_parser('lock-choice');p.add_argument('--out',type=Path,required=True);p.add_argument('--contender',required=True);p.add_argument('--rationale',type=Path,required=True)
    p=sub.add_parser('report');p.add_argument('--out',type=Path,required=True)
    a=vars(ap.parse_args());cmd=a.pop('command').replace('-','_');globals()[cmd](**a)


if __name__=='__main__':main()
