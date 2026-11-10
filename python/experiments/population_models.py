"""Fit transfer strengths in August, test unseen movers in September and US club/league PGNs."""
import math
import numpy as np
import polars as pl
from population_data import OUT, DIV, TARGETS, EVENTS, panel, write, rr
from convertibility_eval import Maia

CAPS=(0,30,100,300)
def aggregate():
    parts=[]
    for e in EVENTS:
        data=pl.scan_parquet(OUT/'edges/train'/e/'*.parquet')
        parts.append(data.group_by('h','m','band','pooled').len('n').with_columns(pl.lit(e).alias('event')).collect())
    return pl.concat(parts)

def inputs(raw,event,band):
    ev,br,n=pl.col('event'),pl.col('band'),pl.col('n').cast(pl.Float64)
    def counts(cond): return pl.when(cond).then(n).otherwise(0.)
    pi=ev.replace_strict(DIV,return_dtype=pl.Float64)
    pooled=counts(pl.col('pooled'))
    proxy='Rapid' if event!='Rapid' else 'Blitz'
    frame=raw.group_by('h','m').agg(
        counts((ev==event)&(br==band)).sum().alias('target'),
        counts((ev==event)&((br-band).abs()==200)).sum().alias('adjacent'),
        counts((ev==proxy)&(br==band)).sum().alias('other_speed'),
        counts((ev=='Rapid')&(br>=2000)).sum().alias('strong_rapid'),
        (pi*pooled).sum().alias('weighted'),(pi*pi*pooled).sum().alias('sq'),
        counts((ev==event)&(br<1800)).sum().alias('current'),
        counts((ev==event)&(br==1800)).sum().alias('future'))
    return frame.with_columns((pl.col('weighted')*pl.col('weighted').sum().over('h')/
            pl.col('sq').sum().over('h').clip(lower_bound=1)).alias('pooled'),
            *[pl.col(k).sum().over('h').alias('N_'+k) for k in ('adjacent','other_speed')])

def mass(model,cap=0,frac=0.5):
    if model!='borrowed':return pl.col(model)
    x=pl.col('target')
    for k,w in [('adjacent',1-frac),('other_speed',frac)]:
        den=pl.col('N_'+k).clip(lower_bound=1)
        x=x+pl.col(k)*(pl.lit(cap*w)/den).clip(upper_bound=1.)
    return x

def observed(paths,band):
    # One equal-player vote per position for the sensitivity metric.
    cnt=(pl.scan_parquet(paths).filter(pl.col('band')==band).group_by('h','player','m').len('obs')
           .with_columns((pl.col('obs')/pl.col('obs').sum().over('h','player')).alias('equal')))
    return cnt.group_by('h','m').agg(pl.col('obs').sum(),pl.col('equal').sum()).collect()

def assess(obs,features,prior,models):
    if not obs.height:return []
    # Include all legal alternatives, including unplayed replies, in the Brier denominator.
    frame=prior.join(features,on=['h','m'],how='left').fill_null(0).join(obs,on=['h','m'],how='left').fill_null(0)
    if frame['obs'].sum()!=obs['obs'].sum():raise RuntimeError('Observed move absent from Maia legal set')
    out=[]
    for mod,cap,frac in models:
        cell=frame.with_columns((mass(mod,cap,frac)+30*pl.col('q')).alias('a')).with_columns(
            (pl.col('a')/pl.col('a').sum().over('h')).alias('p'))
        brier=1+(pl.col('p')**2).sum().over('h')-2*pl.col('p')
        logloss=-pl.col('p').clip(lower_bound=1e-15).log()
        scores=cell.select(*[(metric*pl.col(n)).sum().truediv(pl.col(n).sum()).alias(n+'_'+key)
                            for metric,key in ((brier,'brier'),(logloss,'logloss')) for n in ('obs','equal')]).row(0,named=True)
        out.append(dict(model=mod,cap=cap,fraction_other_speed=frac,moves=int(obs['obs'].sum()),
                        positions=obs['h'].n_unique(),**scores))
    return out

def export(frame,root,expr):
    file=root/'month=training/bkt=0/part.parquet'
    result=frame.select(pl.col('h').alias('parent_hash'),pl.col('m').alias('move_san'),expr.alias('total')).filter(pl.col('total')>0)
    if result.is_empty():raise ValueError(f'Empty {root}')
    write(result,file)

def compare_positions(d,features,prior,models,boards):
    # Probabilities and observed moves make differences in held-out loss traceable.
    frame=prior.join(features,on=['h','m'],how='left').fill_null(0).join(d,on=['h','m'],how='left').fill_null(0)
    for mod,cap,frac in models:
        a=mass(mod,cap,frac)+30*pl.col('q')
        frame=frame.with_columns((a/a.sum().over('h')).alias(mod))
    frame=frame.with_columns(pl.col('obs').sum().over('h').alias('replies'),
                            pl.col('target').sum().over('h').alias('local_train'))
    return frame.filter(pl.col('replies')>0).select('h','m','obs','equal','replies','local_train',
            'borrowed','pooled','strong_rapid').with_columns(
            pl.col('h').replace_strict(boards,return_dtype=pl.String).alias('epd'))

def main():
    raw=aggregate()
    boards=panel()
    # Deterministic stratification by target and by data availability is implicit in
    # coverage. A board must be prelisted by the ECO catalog or the canonical books.
    detail=[]; fits={}
    for name,(e,b,rating) in TARGETS.items():
        local=OUT/'models'/name
        if (local/'fit.json').exists():
            import json
            saved=json.loads((local/'fit.json').read_text())
            fits[name]=saved['chosen'];detail.extend(saved['detail']);continue
        subsets={split:observed(OUT/'edges'/split/e/'*.parquet',b) for split in ['tune','test']}
        if name.startswith('slow') and b<=2000:
            for f in sorted((OUT/'otb').glob('*.parquet')):subsets[f.stem]=observed(f,b)
        hs=sorted(set().union(*(set(d['h']) for d in subsets.values())))
        if not hs:raise ValueError(f'No evaluation data for {name}')
        maia=Maia()
        try:
            rows=[]
            for i in range(0,len(hs),512):
                keys=hs[i:i+512]
                probs=maia.probs([boards[h] for h in keys],rating,1450,keys)
                for h,dist in zip(keys,probs):
                    if not dist or abs(sum(dist.values())-1)>1e-4:raise ValueError('Invalid prior')
                    rows.extend((h,m,q) for m,q in dist.items())
        finally:maia.close()
        prior=pl.DataFrame(rows,schema=['h','m','q'],orient='row')
        all_features=inputs(raw,e,b)
        f=all_features.filter(pl.col('h').is_in(hs))
        grid=[('borrowed',k,w) for k in CAPS for w in ((.5,) if not k else (0.,.5,1.))]
        base=[('pooled',0,0.),('strong_rapid',0,0.)]
        tuning=assess(subsets['tune'],f,prior,grid+base)
        best=min((r for r in tuning if r['model']=='borrowed'),key=lambda r:(r['obs_brier'],r['cap']))
        best=('borrowed',best['cap'],best['fraction_other_speed'])
        records=[dict(target=name,split='tune',**r) for r in tuning]
        for tag,d in subsets.items():
            if tag=='tune':continue
            records.extend(dict(target=name,split=tag,**r) for r in assess(d,f,prior,[best,*base]))
            if d.height:
                diagnostics=compare_positions(d,f,prior,[best,*base],boards)
                write(diagnostics,local/'diagnostics'/f'{tag}.parquet')
                for tier,lo,hi in [('0-19',0,20),('20-99',20,100),('100+',100,float('inf'))]:
                    keys=diagnostics.filter((pl.col('local_train')>=lo)&(pl.col('local_train')<hi)).select('h').unique()
                    slice_obs=d.join(keys,on='h',how='semi')
                    records.extend(dict(target=name,split=tag,training_visits=tier,**r)
                                   for r in assess(slice_obs,f,prior,[best,*base]))
        for mod,k,w in [best,*base,('current',0,0.),('future',0,0.)]:
            export(all_features,local/mod,mass(mod,k,w))
        rr.publish(local/'fit.json',dict(chosen=best,detail=records))
        detail.extend(records);fits[name]=best
        print(name,best,[r for r in records if r['split']=='test'],flush=True)
    rr.publish(OUT/'validation.json',dict(fits=fits,results=detail))
if __name__=='__main__':main()
