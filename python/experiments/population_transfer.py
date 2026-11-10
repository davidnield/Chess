"""Checkpoint model comparison and score frozen 2,000-decision books as they become available."""
import argparse
import json
import msvcrt
import os
import shutil
import subprocess
import time
from pathlib import Path

import psutil
import polars as pl
from population_data import OUT, RAW, DIV, EVENTS, TARGETS, HERE, rr, sys

SWEEP=rr.OUT/'cp-screen-030-060-100'
COLORS=("black","white")

def run(stage,cmd):
    rr.publish(OUT/'progress.json',dict(stage=stage,pid=os.getpid(),utc=rr.now()))
    with (OUT/(stage+'.log')).open('a',encoding='utf8') as f:
        subprocess.run(cmd,cwd=rr.ROOT,stdout=f,stderr=subprocess.STDOUT,check=True)
def wait_external(pid,stage,complete):
    while not complete():
        try:
            cmd=' '.join(psutil.Process(pid).cmdline()) if pid else ''
        except psutil.NoSuchProcess:
            cmd=''
        if 'population_data.py' not in cmd or stage not in cmd:
            run('resume-'+stage,[sys.executable,'-u',str(HERE/'population_data.py'),stage])
            if not complete():raise RuntimeError(f'Incomplete {stage} extraction')
            return
        time.sleep(15)

def freeze(c):
    manifest=OUT/'frozen'/f'{c}.json'
    files={}
    status=SWEEP/'status.json'
    sweep=json.loads(status.read_text()) if status.exists() else {}
    caps=sweep['completed_caps'][c] if sweep.get('state')=='closed' else (30,60,100)
    for cap in caps:
        src=SWEEP/c/f'cp{cap}/book_b2000.parquet'
        receipt=SWEEP/'stages'/f'{c}-cp{cap}.json'
        if not receipt.exists():return
        digest=json.loads(receipt.read_text())['outputs'][str(src)]
        if rr.digest(src)!=digest:raise ValueError('Gate sweep book changed')
        files[f'cp{cap}']=(src,digest)
    canon=rr.incumbent(c)
    files['CANON']=(canon,rr.digest(canon))
    frozen={}
    for label,(src,digest) in files.items():
        dest=OUT/'frozen'/c/(label+'.parquet')
        dest.parent.mkdir(exist_ok=True,parents=True)
        if not dest.exists():
            temp=dest.with_suffix('.copying')
            shutil.copy2(src,temp);temp.replace(dest)
        if rr.digest(dest)!=digest:raise ValueError(f'Frozen book changed: {dest}')
        f=pl.read_parquet(dest,columns=['position_hash','best_move','side_to_move']).filter(
            pl.col('best_move').is_not_null()&(pl.col('side_to_move')==c))
        if f.height!=2000 or f['position_hash'].n_unique()!=2000:raise ValueError('Book footprint mismatch')
        frozen[label]=dict(path=str(dest),sha256=digest)
    rr.publish(manifest,frozen)
    return frozen

def score(c,books,target,model,own):
    dest=OUT/'scores'/c/target/model/own
    dest.mkdir(parents=True,exist_ok=True)
    result=dest/'scores.parquet'
    if not result.exists():
        ours=1450 if own=='current' else 1900
        models=OUT/'models'/target
        args=[sys.executable,'-X','utf8','-u',str(HERE.parent/'convertibility_eval.py'),
              '--our-moves',str(models/own),'--opp-moves',str(models/model),'--our-elo',str(ours),
              '--opp-elo',str(TARGETS[target][2]),'--k','30','--seed','1,2','--max-ply','30',
              '--tau','.001','--rollouts','2','--sf-procs','1','--sf-cache',str(OUT/'sf.parquet'),
              '--compare-to','CANON','--out',str(result),'--exits-out',str(dest/'exits')]
        for label,b in books.items():args+=['--book',f"{label}={c}={b['path']}"]
        run(f'score-{c}-{target}-{model}-{own}',args)
    f=pl.read_parquet(result)
    if set(f.select('seed','label').iter_rows())!={(i,k) for i in [1,2] for k in books}:raise ValueError(result)

def rankings():
    frames=[]
    for file in (OUT/'scores').glob('*/*/*/*/scores.parquet'):
        c,t,m,o=file.relative_to(OUT/'scores').parts[:4]
        frames.append(pl.read_parquet(file).select('label','seed','uniform','book_moves').with_columns(
            pl.lit(c).alias('colour'),pl.lit(t).alias('target'),pl.lit(m).alias('model'),pl.lit(o).alias('own')))
    if not frames:return
    data=pl.concat(frames)
    data.group_by('colour','target','model','own','label').agg(pl.col('uniform').mean(),pl.col('book_moves').mean()).sort(
        'colour','model','own','target','uniform',descending=[False,False,False,False,True]).write_csv(OUT/'scenario-scores.csv')
    # Two environments per speed. The 2200+ slow scenario is a separately reported stress test.
    roll=data.filter(pl.col('target')!='slow-stress').group_by('colour','model','own','label','seed').agg(
        pl.col('target').n_unique().alias('targets'),pl.col('uniform').mean()).filter(pl.col('targets')==6)
    roll.sort('colour','model','own','seed','uniform',descending=[False,False,False,False,True]).write_csv(OUT/'equal-speed-scores.csv')

def main(online_pid,otb_pid):
    expected=[OUT/'edges'/('train' if m<=7 else 'tune' if m==8 else 'test')/e/f'{m}-{p.stem}.parquet'
              for m in range(4,10) for e in EVENTS for p in (RAW/f'year=2026/month={m}/event={e}').glob('*.parquet')]
    design=dict(train='Apr-Jul 2026; mover hash folds 0-5',tune='Aug 2026; mover folds 6-7',
                test='Sep 2026; mover folds 8-9',sampling_divisors=DIV,targets=TARGETS,
                game_sampling='DuckDB hash(game_id) modulo divisor; tune/test divisor quarter of train',
                analysis_panel='published ECO parents and canonical positions, fixed before fitting; training and rollout tables cover all sampled positions',
                pooled='time-sampling adjusted matched April-July mean-1800+ BRC reconstruction',
                strong_rapid='sampled Rapid with mover rated at least 2000',
                priors='30 Maia pseudogames once per decision; neighbor strength from August Brier',
                own=['1000-1799','1800-1999'],stress='online classical 2200+ separate from equal-speed average',
                otb='external transfer test; source-specific published rating bands 1800-1999 and 2000-2199; known slow controls only',
                code={p.name:rr.digest(p) for p in [Path(__file__),HERE/'population_models.py',HERE/'population_data.py',
                    HERE.parent/'convertibility_eval.py',HERE.parent/'maia_server.py',HERE.parent/'maia_batch.py']})
    saved=OUT/'design.json'
    if saved.exists() and json.loads(saved.read_text())!=json.loads(json.dumps(design)):raise RuntimeError('Design changed')
    rr.publish(saved,design)
    rr.publish(OUT/'progress.json',dict(stage='extract',pid=os.getpid(),utc=rr.now()))
    wait_external(online_pid,'online',lambda:all(p.exists() for p in expected))
    wait_external(otb_pid,'otb',lambda:(OUT/'otb-coverage.json').exists())
    if not (OUT/'validation.json').exists():
        run('fit-and-test',[sys.executable,'-X','utf8','-u',str(HERE/'population_models.py')])
    done=set()
    for cache in ('sf.parquet','sf.parquet.meta.json'):
        src=SWEEP/cache
        dest=OUT/cache
        if not dest.exists():
            # Copy from the immutable historical cache, independent of the running screen.
            src=Path('E:/chess/calibration')/cache.replace('sf.parquet','convertibility_sf_cache_sampled.parquet')
            temp=dest.with_suffix('.copying');shutil.copy2(src,temp);temp.replace(dest)
    while len(done)<2:
        for c in COLORS:
            if c in done:continue
            books=freeze(c)
            if books is None:continue
            for own in ('current','future'):
                for model in ('borrowed','pooled','strong_rapid'):
                    for t in TARGETS:
                        score(c,books,t,model,own)
                        rankings()
            done.add(c)
        if len(done)<2:
            upstream=json.loads((SWEEP/'status.json').read_text())
            if upstream.get('state')=='failed':raise RuntimeError(f'Upstream sweep failed: {upstream}')
            rr.publish(OUT/'progress.json',dict(stage='waiting for frozen books',completed=sorted(done),utc=rr.now()))
            time.sleep(30)
    rr.publish(OUT/'progress.json',dict(stage='complete',utc=rr.now()))
if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--online-pid',type=int);ap.add_argument('--otb-pid',type=int)
    a=ap.parse_args()
    with (OUT/'coordinator.lock').open('a+b') as f:
        f.seek(0);msvcrt.locking(f.fileno(),msvcrt.LK_NBLCK,1)
        try:main(a.online_pid,a.otb_pid)
        except Exception as error:
            rr.publish(OUT/'progress.json',dict(stage='failed',error=str(error),utc=rr.now()))
            raise
