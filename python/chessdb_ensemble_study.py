"""Frozen ensemble tournament, new held-out positions, and checkpointed SF18 references.

Small-panel statistics/orchestration only; DuckDB and Stockfish do the native work.
Never writes production eval arrays or changes repertoire gates.
"""
from __future__ import annotations
import argparse
import concurrent.futures as cf
import datetime as dt
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import time

import numpy as np
import pandas as pd
import chess
import chessdb_policy_validation as ref
from month_archive import file_lock, read_json, sha, write_json
from zobrist128 import zobrist128

HERE = Path(__file__).resolve().parent
SEED = 202611251
SOURCES = ['cloud', 'fishnet', 'chessdb']
QUOTAS = {f'{depth}_{tier}': n for depth in ('deep', 'shallow')
          for tier, n in [('once', 150), ('repeated', 150), ('legacy', 50)]}
QUOTAS.update(no_cloud_once=100, no_cloud_repeated=100, no_cloud_legacy=100)


def code_identity():
    return {n: sha(HERE/n) for n in ('chessdb_ensemble_study.py',
        'chessdb_policy_validation.py', 'month_archive.py', 'zobrist128.py', 'zobrist.py')}


def canon(fen):
    # EP spelling differences with no legal capture are not independent positions.
    b = chess.Board(fen)
    return min(b.epd(en_passant='legal'), b.mirror().epd(en_passant='legal'))


def catalog():
    out = []
    for c in range(11):
        for f in range(11-c):
            w = [c, f, 10-c-f]
            for space in ('cp', 'score'):
                if space == 'score' and max(w) == 10:
                    continue  # Single sources do not need two identical variants.
                out.append(dict(name=f'{space}_{w[0]}_{w[1]}_{w[2]}', kind='linear',
                                weights=[x/10 for x in w], space=space))
    for space in ('cp', 'score'):
        out.append(dict(name=f'{space}_equal3', kind='linear', weights=[1/3]*3, space=space))
    out += [dict(name='median3', kind='median')]
    out += [dict(name=n, kind=n) for n in ('step1', 'rule_13nov', 'proposal')]
    return out


def strata(d):
    depth = np.where(d.cloud.notna(), np.where(d.cloud_depth.ge(22).fillna(False), 'deep', 'shallow'), 'no_cloud')
    tier = np.where(d.fishnet_tier.eq('nnue'), np.where(d.fishnet_n_tier.eq(1).fillna(False), 'once', 'repeated'), 'legacy')
    return pd.Series([f'{a}_{b}' for a,b in zip(depth,tier)], index=d.index)


def policies(d):
    c, f, cb = (d[s].to_numpy(float) for s in SOURCES)
    n = np.where(d.fishnet_tier.eq('nnue'), f, np.nan)
    hasc, hasn = np.isfinite(c), np.isfinite(n)
    deep = hasc & d.cloud_depth.ge(22).fillna(False).to_numpy(dtype=bool)
    mid = np.median(np.stack([c, n, cb]), axis=0)
    shallow = np.where(hasn, mid, (c+cb)/2)
    rule = np.where(deep, np.where(hasn, (c+n)/2, c),
                    np.where(hasc, shallow, np.where(hasn, (2*cb+n)/3, cb)))
    proposal = np.where(hasc, rule, cb)
    # Replicate the step-1 gap / old-Fishnet override, otherwise keep the stored score.
    current = d.current.to_numpy(float)
    step1 = np.where(~hasc & ~hasn, cb, current)
    return dict(step1=step1, rule_13nov=rule, proposal=proposal)


def predict(d, recipes, production=False):
    """White expected scores. Missing selected sources stay missing in source-only tests.

    Deployment sensitivity excludes legacy Fishnet, renormalizes available weights,
    and explicitly falls back to ChessDB if all chosen sources are absent.
    """
    x = d[SOURCES].to_numpy(float).copy()
    if production:
        x[~d.fishnet_tier.eq('nnue'), 1] = np.nan
    available = np.isfinite(x)
    fixed = policies(d)
    out = {}
    for r in recipes:
        if r['kind'] == 'linear':
            w = np.array(r['weights'])[None, :]*available
            den = w.sum(axis=1)
            val = ref.score(x) if r['space'] == 'score' else x
            z = np.divide(np.nansum(val*w, axis=1), den,
                          out=np.full(len(d), np.nan), where=den > 0)
            if r['space'] == 'cp':
                z = ref.score(z)
        elif r['kind'] == 'median':
            z = ref.score(np.nanmedian(x, axis=1))
        else:
            z = ref.score(fixed[r['kind']])
        if production:
            z = np.where(np.isfinite(z), z, ref.score(d.chessdb))
        out[r['name']] = z
    return pd.DataFrame(out, index=d.index)


def macro(a, groups):
    return pd.DataFrame(a).groupby(np.asarray(groups)).mean().mean()


def gate_counts(pred, truth, gates, baselines):
    rows = []
    for arm, gate in gates.items():
        for color, sign in [('white', 1), ('black', -1)]:
            floor = (baselines[color] if sign == 1 else 1-baselines[color])-gate[color]
            r = truth if sign == 1 else 1-truth
            p = pred if sign == 1 else 1-pred
            valid = np.isfinite(p) & np.isfinite(r)
            near = abs(r-floor) <= .002
            rows.append(dict(gate=arm, color=color, n=int(valid.sum()),
                false_admissions=int((valid & (p>=floor) & (r<floor)).sum()),
                clear_false_admissions=int((valid & (p>=floor) & (r<floor) & ~near).sum()),
                false_exclusions=int((valid & (p<floor) & (r>=floor)).sum()),
                near_floor=int((valid & near).sum()),
                unsafe_shortfall_points=float(100*np.where(valid & (p>=floor) & (r<floor),floor-r,0).sum())))
    return rows


def development(d, recipes, gates, baselines, out):
    common = d[SOURCES].notna().all(axis=1)
    q = d[common & d.fishnet_tier.eq('nnue')].copy()
    q['stratum'] = strata(q)
    p = predict(q, recipes)
    e = 100*abs(p.sub(ref.score(q.sf), axis=0))
    means = macro(e, q.stratum).sort_values(kind='stable')
    champion, runner = list(means.index[:2])
    singles = ['cp_10_0_0', 'cp_0_10_0', 'cp_0_0_10']
    best_single = means.loc[singles].idxmin()
    safety = {}
    for name in means.index:
        safety[name] = max(x['clear_false_admissions'] for x in
                          gate_counts(p[name], ref.score(q.sf), gates, baselines))
    near_best = means[means <= means.iloc[0]*1.10].index
    gate_finalist = min(near_best, key=lambda n: (safety[n], means[n], n))
    # Bucket-blocked cross-validation of the selection procedure; not a fake
    # out-of-sample score for a winner selected on all development rows.
    cv = []
    for b in sorted(q.bucket.unique()):
        train = q.bucket != b
        winner = macro(e.loc[train], q.loc[train, 'stratum']).idxmin()
        cv.append(dict(bucket=int(b), selected=winner, n=int((~train).sum()),
                       heldout_mae_points=float(e.loc[~train, winner].mean())))
    table = pd.DataFrame(dict(policy=means.index, macro_mae_points=means.values,
                             worst_gate_clear_false_admissions=[safety[n] for n in means.index]))
    table.to_csv(out/'development-ranking.csv', index=False)
    selected = dict(champion=champion, runner_up=runner, safety_finalist=gate_finalist,
        best_single=best_single, confirmatory=list(dict.fromkeys([champion, runner, gate_finalist,
            best_single, 'proposal', 'step1'])), n_primary=len(q), n_all3=int(common.sum()),
        primary='macro expected-score MAE across cloud depth x NNUE repetition strata',
        cv=cv, caveat='Development labels were already exposed; independent new holdout is required.')
    write_json(out/'finalists.json', selected)
    return selected


def normalize(d):
    d = d.copy()
    def convert(cp, mate):
        cp = cp.to_numpy(dtype=float, na_value=np.nan)
        mate = mate.to_numpy(dtype=float, na_value=np.nan)
        return np.clip(np.where(np.isfinite(cp), cp, np.sign(mate)*2000), -2000, 2000)
    d['cloud'] = convert(d.cloud_cp, d.cloud_mate)
    d['fishnet'] = convert(d.fishnet_cp, d.fishnet_mate)
    d['current'] = convert(d.cp, d.mate)
    d['chessdb'] = np.clip(d.best_score.to_numpy(np.int32)*np.where(d.white_to_move, 1, -1), -2000, 2000)
    d['id'] = [f'{int(r.lo)}:{int(r.hi)}' for r in d.itertuples()]
    d['stratum'] = strata(d)
    return d


def initialize(out, panel, targeted, prior, eval_db, menus, engine, exclusions):
    if out.exists():
        raise ValueError('initialize requires a new directory')
    d = pd.read_parquet(panel/'panel_scored_sf18_d24.parquet')
    meta = read_json(panel/'panel.meta.json')
    proposal = read_json(prior/'definition.json')
    if sha(panel/'panel_scored_sf18_d24.parquet') != proposal['panel_sha256']:
        raise ValueError('development identity changed')
    prior_refs = [json.loads(s) for s in (panel/'search_sf18_d24.jsonl').read_text().splitlines() if s]
    blocked = {canon(r['fen']) for r in prior_refs}
    source_files = [panel/'panel_scored_sf18_d24.parquet', panel/'panel.meta.json',
                    panel/'search_sf18_d24.jsonl', prior/'definition.json',
                    targeted/'manifest.json', menus/'_MENUS.COMPLETE.json', eval_db/'_build.meta.json']
    for path in exclusions:
        t = pd.read_csv(path)
        col = next((c for c in ('fen', 'fen4', 'epd', 'parent_epd') if c in t), None)
        if col is None:
            raise ValueError(f'no FEN column in historical exclusion {path}')
        blocked.update(canon(x) for x in t[col].dropna())
        source_files.append(path)
    # Different Zobrist buckets; canonical mirrored-FEN exclusion also removes
    # all known older parent/child examples across bucket boundaries.
    buckets = sorted(set(range(512))-set(meta['buckets']), key=lambda b: ref.digest([SEED,b]))[:8]
    recipes = catalog()
    definition = dict(schema=1, seed=SEED, code=code_identity(), catalog=recipes,
        runtime={n:importlib.metadata.version(n) for n in ('duckdb','chess','pandas','numpy','pyarrow')},
        quotas=QUOTAS, buckets=buckets, thinning=8, per_bucket_keep=450,
        gates=proposal['gates'], baselines=proposal['baselines'],
        panel=str(panel), targeted=str(targeted), eval_db=str(eval_db), menus=str(menus),
        sources={str(p):sha(p) for p in source_files}, blocked_fens=sorted(blocked),
        engine=str(engine), engine_sha256=sha(engine),
        settings=dict(workers=4, threads=1, hash_mb=128, time_seconds=60., target_depth=28,
                      minimum_depth=26, bootstrap=4000, disk_reserve_gib=250),
        sampling='new hash buckets; hash thinning; uniform unique positions within fixed source strata; games >=2; first ply <=50',
        primary='all-three NNUE overlap, equal stratum weights; no reach weighting',
        missing_policy='Production sensitivity excludes legacy Fishnet; renormalize; ChessDB fallback if selected sources absent',
        adoption=False)
    if not definition['engine_sha256'].startswith('c86215fa'):
        raise ValueError('wrong SF18 engine')
    out.mkdir(parents=True)
    write_json(out/'definition.json', definition)
    development(d, recipes, definition['gates'], definition['baselines'], out)
    write_json(out/'freeze.json', dict(definition=sha(out/'definition.json'), finalists=sha(out/'finalists.json')))
    return definition


def check_definition(out):
    cfg = read_json(out/'definition.json')
    freeze = read_json(out/'freeze.json')
    if (sha(out/'definition.json') != freeze['definition'] or
            sha(out/'finalists.json') != freeze['finalists'] or cfg['code'] != code_identity() or
            any(importlib.metadata.version(n)!=v for n,v in cfg.get('runtime',{}).items())):
        raise ValueError('frozen study recipe/code changed')
    for path,h in cfg.get('sources',{}).items():
        if sha(Path(path)) != h:
            raise ValueError('frozen study source changed')
    return cfg


def path_sql(path):
    return "'"+Path(path).as_posix().replace("'", "''")+"'"


def sample_bucket(out, cfg, b):
    import duckdb
    e = Path(cfg['eval_db'])/f'bkt{b:03d}.parquet'
    p = Path(cfg['menus'])/'positions'/f'bkt{b:03d}.parquet'
    pub = read_json(Path(cfg['menus'])/'_MENUS.COMPLETE.json')
    stamp = {str(x): (x.stat().st_size, x.stat().st_mtime_ns) for x in (e,p)}
    hashes = {str(x):sha(x) for x in (e,p)}
    if hashes[str(p)] != pub['buckets'][str(b)]['positions']['sha256']:
        raise ValueError('menu publication mismatch')
    if shutil.disk_usage(out).free < cfg['settings']['disk_reserve_gib']*2**30:
        raise ValueError('sampling scratch reserve')
    con = duckdb.connect()
    scratch = out/'duckdb-scratch'; scratch.mkdir(exist_ok=True)
    try:
        con.execute("SET threads=2; SET memory_limit='4GB'")
        con.execute(f"SET temp_directory={path_sql(scratch)}; SET max_temp_directory_size='12GB'")
        con.execute(f"""CREATE TEMP TABLE ev AS
          SELECT position_hash lo, position_hash_hi hi, any_value(epd) epd,
            any_value(cp) cp, any_value(mate) mate, any_value(cloud_depth) cloud_depth,
            any_value(cloud_cp) cloud_cp, any_value(cloud_mate) cloud_mate,
            any_value(fishnet_cp) fishnet_cp, any_value(fishnet_mate) fishnet_mate,
            any_value(fishnet_tier) fishnet_tier, any_value(fishnet_n_tier) fishnet_n_tier
          FROM read_parquet({path_sql(e)}) WHERE NOT hash_ambiguous AND source <> 'terminal'
            AND (fishnet_cp IS NOT NULL OR fishnet_mate IS NOT NULL)
            AND hash(position_hash, position_hash_hi, {SEED}) % {cfg['thinning']} = 0
          GROUP BY 1,2 HAVING count(*) = 1""")
        d = con.execute(f"""WITH eligible AS (
          SELECT {b} bucket, e.*, p.games, p.first_ply, p.white_to_move, p.best_score,
            concat(CASE WHEN e.cloud_cp IS NULL AND e.cloud_mate IS NULL THEN 'no_cloud'
                        WHEN e.cloud_depth >= 22 THEN 'deep' ELSE 'shallow' END, '_',
                   CASE WHEN e.fishnet_tier <> 'nnue' THEN 'legacy'
                        WHEN e.fishnet_n_tier=1 THEN 'once' ELSE 'repeated' END) sample_stratum
          FROM ev e JOIN read_parquet({path_sql(p)}) p USING(lo,hi)
          WHERE p.found AND p.best_score IS NOT NULL AND p.games >= 2 AND p.first_ply <= 50
        ) SELECT *, hash(lo,hi,{SEED+1}) sample_priority,
            count(*) OVER(PARTITION BY sample_stratum) eligible_in_bucket
          FROM eligible QUALIFY row_number() OVER(PARTITION BY sample_stratum
                    ORDER BY hash(lo,hi,{SEED+1}), lo,hi) <= {cfg['per_bucket_keep']}""").df()
    finally:
        con.close()
    if any((x.stat().st_size,x.stat().st_mtime_ns) != stamp[str(x)] for x in (e,p)):
        raise ValueError('source changed while sampling')
    d = normalize(d)
    if d[['lo','hi']].duplicated().any() or not (d.stratum == d.sample_stratum).all():
        raise ValueError('duplicate keys or source stratum disagreement')
    dest = out/'sample-buckets'/f'b{b:03d}.parquet'; dest.parent.mkdir(exist_ok=True)
    tmp = dest.with_suffix('.tmp.parquet'); d.to_parquet(tmp,index=False); tmp.replace(dest)
    receipt = dict(bucket=b, output_sha256=sha(dest), sources=hashes,
                   rows=len(d), eligible=d.groupby('stratum').eligible_in_bucket.first().to_dict())
    write_json(dest.with_suffix('.json'), receipt)
    return receipt


def sample(out, max_buckets=None):
    cfg = check_definition(out)
    with file_lock(out/'sample.lock'):
        for path, h in cfg['sources'].items():
            if sha(Path(path)) != h:
                raise ValueError('frozen input changed')
        new = 0
        for b in cfg['buckets']:
            p = out/'sample-buckets'/f'b{b:03d}.parquet'
            receipt = p.with_suffix('.json')
            if receipt.exists():
                r = read_json(receipt)
                if sha(p) != r['output_sha256']:
                    raise ValueError('sample checkpoint corrupt')
                continue
            if (out/'STOP').exists() or (max_buckets is not None and new >= max_buckets):
                return False
            emit(out, 'sampling', bucket=b)
            r = sample_bucket(out,cfg,b); new += 1
            emit(out, 'sampled-bucket', bucket=b, candidates=r['rows'], eligible=r['eligible'])
        if (out/'manifest.json').exists():
            return True
        parts = [pd.read_parquet(out/'sample-buckets'/f'b{b:03d}.parquet') for b in cfg['buckets']]
        d = pd.concat(parts,ignore_index=True)
        blocked = set(cfg['blocked_fens'])
        d['canonical'] = [canon(f) for f in d.epd]
        d = d[~d.canonical.isin(blocked)].copy()
        # Same ranking as the per-bucket top-K: the union retains the global
        # bottom quota rather than re-randomizing a bucket-balanced subsample.
        d = d.sort_values(['sample_priority','id']).drop_duplicates('canonical')
        keep = []
        for group, quota in cfg['quotas'].items():
            part = d[d.stratum == group].head(quota)
            if len(part) != quota:
                raise ValueError(f'insufficient independent {group}: {len(part)}/{quota}; do not silently change quotas')
            keep.append(part)
        d = pd.concat(keep,ignore_index=True).sort_values(['sample_priority','id']).reset_index(drop=True)
        for r in d.itertuples():
            b = chess.Board(r.epd)
            if not b.is_valid() or b.is_game_over() or b.turn != bool(r.white_to_move):
                raise ValueError('invalid sampled board')
            if zobrist128(b) != (int(r.lo),int(r.hi)):
                raise ValueError('sample key/FEN mismatch; investigate EP spelling before proceeding')
        path = out/'holdout-inputs.parquet'; d.to_parquet(path,index=False)
        # All evaluation inputs are frozen before looking at any fresh SF label.
        tasks = [dict(id=r.id,parent=r.id,kind='parent',fen=r.epd+' 0 1',
                      target_depth=cfg['settings']['target_depth'],minimum_depth=cfg['settings']['minimum_depth'])
                 for r in d.itertuples()]
        bound = [out/'definition.json',out/'freeze.json',out/'finalists.json',path]
        bound += [out/'sample-buckets'/f'b{b:03d}.json' for b in cfg['buckets']]
        manifest = dict(schema=1,code=cfg['code'],engine=cfg['engine'],engine_sha256=cfg['engine_sha256'],
                        settings=cfg['settings'],tasks=tasks,sources={str(p):sha(p) for p in bound})
        write_json(out/'manifest.json',manifest)
        emit(out,'sample-ready',total=len(d),counts=d.stratum.value_counts().to_dict())
        return True


def emit(out, state, **kwargs):
    obj = dict(state=state,utc=dt.datetime.now(dt.timezone.utc).isoformat(),pid=os.getpid(),**kwargs)
    write_json(out/'status.json',obj); print(json.dumps(obj),flush=True)


def reference_run(out, max_new=None, search=ref.search_one):
    cfg = check_definition(out)
    m = read_json(out/'manifest.json')
    if m['code'] != cfg['code'] or sha(Path(m['engine'])) != m['engine_sha256']:
        raise ValueError('reference code/engine drift')
    for p,h in m['sources'].items():
        if sha(Path(p)) != h:
            raise ValueError('reference input drift')
    with file_lock(out/'reference.lock'):
        (out/'results').mkdir(exist_ok=True)
        results = ref.load_results(out,m)
        tasks = [t for t in m['tasks'] if t['id'] not in results]
        if max_new is not None:
            tasks = tasks[:max_new]
        todo = iter(tasks)
        with cf.ThreadPoolExecutor(m['settings']['workers']) as pool:
            live = {}
            exhausted = False
            while live or not exhausted:
                while len(live) < m['settings']['workers'] and not exhausted and not (out/'STOP').exists():
                    task = next(todo,None)
                    if task is None:
                        exhausted = True; break
                    live[pool.submit(search,task,m)] = task
                if not live:
                    break
                done,_ = cf.wait(live,timeout=10,return_when=cf.FIRST_COMPLETED)
                for f in done:
                    task = live.pop(f)
                    r = dict(f.result(),task=task); r['qualified'] = ref.qualified(task,r)
                    write_json(out/'results'/f"{ref.digest(task['id'])}.json",
                               dict(identity=ref.digest(m),payload=r,sha256=ref.digest(r)))
                    results[task['id']] = r
                emit(out,'searching',completed=len(results),total=len(m['tasks']),active=len(live),
                     qualified=sum(r['qualified'] for r in results.values()))
        complete = len(results) == len(m['tasks'])
        emit(out,'references-complete' if complete else 'stopped-or-bounded',
             completed=len(results),total=len(m['tasks']))
        return complete


def metrics(d, cfg, label, out):
    rows, gates = [],[]
    common = d[SOURCES].notna().all(axis=1)
    cohorts = [('all3_nnue',common & d.fishnet_tier.eq('nnue'),False),
               ('all3_including_legacy',common,False),
               ('production_fallback',pd.Series(True,index=d.index),True)]
    cohorts += [(g,d.stratum.eq(g),True) for g in sorted(d.stratum.unique())]
    truth = ref.score(d.sf)
    for cohort,mask,production in cohorts:
        x = d.loc[mask]; y = truth[mask]
        if not len(x):
            continue
        preds = predict(x,cfg['catalog'],production=production)
        for name in preds:
            p = preds[name].to_numpy(); valid=np.isfinite(p)
            err = 100*abs(p[valid]-y[valid])
            if not len(err):
                continue
            mae = float(macro(pd.DataFrame({'e':err}),x.loc[valid,'stratum']).iloc[0])
            rows.append(dict(panel=label,cohort=cohort,policy=name,n=len(err),macro_mae_points=mae,
                mae_points=float(err.mean()),p95_points=float(np.quantile(err,.95)),
                p99_points=float(np.quantile(err,.99)),maximum_points=float(err.max()),
                mae_cp=float(abs(np.log(p[valid]/(1-p[valid]))/.00368208-x.sf.to_numpy()[valid]).mean()),
                bias_points=float(100*(p[valid]-y[valid]).mean()),
                rmse_points=float(np.sqrt(np.mean(err**2)))))
            for g in gate_counts(p,y,cfg['gates'],cfg['baselines']):
                gates.append(dict(panel=label,cohort=cohort,policy=name,**g))
    pd.DataFrame(rows).to_csv(out/f'{label}-metrics.csv',index=False)
    pd.DataFrame(gates).to_csv(out/f'{label}-gates.csv',index=False)
    return pd.DataFrame(rows)


def paired_intervals(d,cfg,finalists):
    q=d[d[SOURCES].notna().all(axis=1) & d.fishnet_tier.eq('nnue')].copy()
    preds=predict(q,cfg['catalog'])
    errors=100*abs(preds.sub(ref.score(q.sf),axis=0))
    rng=np.random.default_rng(SEED)
    pairs=[]
    comparisons=[(policy,base) for policy in finalists['confirmatory']
                 for base in dict.fromkeys(['proposal','step1',finalists['best_single']]) if policy!=base]
    family_tail=.025/max(1,len(comparisons))
    for policy in finalists['confirmatory']:
        for base in dict.fromkeys(['proposal','step1',finalists['best_single']]):
            if policy==base: continue
            delta=errors[policy]-errors[base]
            means=[]
            for _,g in q.groupby('stratum'):
                z=delta.loc[g.index].to_numpy()
                means.append(z[rng.integers(0,len(z),size=(cfg['settings']['bootstrap'],len(z)))].mean(axis=1))
            boot=np.mean(means,axis=0)
            pairs.append(dict(policy=policy,baseline=base,n=len(q),
                 delta_macro_mae_points=float(macro(delta,q.stratum).iloc[0]),
                 ci_low=float(np.quantile(boot,.025)),ci_high=float(np.quantile(boot,.975)),
                 simultaneous_ci_low=float(np.quantile(boot,family_tail)),
                 simultaneous_ci_high=float(np.quantile(boot,1-family_tail)),
                 family_comparisons=len(comparisons)))
    return pairs


def report(out):
    cfg=check_definition(out); m=read_json(out/'manifest.json')
    results=ref.load_results(out,m)
    if len(results)!=len(m['tasks']):
        raise ValueError('held-out report requires all searches attempted; no interim winner peeking')
    d=pd.read_parquet(out/'holdout-inputs.parquet')
    d['sf']=d.id.map({k:r['cp_white'] for k,r in results.items()})
    d['qualified']=d.id.map({k:r['qualified'] for k,r in results.items()})
    d['depth']=d.id.map({k:r['depth'] for k,r in results.items()})
    d.to_parquet(out/'holdout-scored.parquet',index=False)
    qualified=d[d.qualified].copy()
    quality=d.groupby('stratum').agg(attempted=('id','size'),qualified=('qualified','sum'),
                                      depth28=('depth',lambda x:int((x>=28).sum())))
    quality.to_csv(out/'reference-qualification.csv')
    metrics(qualified,cfg,'holdout',out)
    metrics(qualified[qualified.depth>=28],cfg,'holdout-depth28',out)
    finalists=read_json(out/'finalists.json')
    pairs=paired_intervals(qualified,cfg,finalists)
    pd.DataFrame(pairs).to_csv(out/'holdout-paired.csv',index=False)
    # Reuse the already-running deeper panel as a stress test, separate from holdout.
    panel=pd.read_parquet(Path(cfg['panel'])/'panel_scored_sf18_d24.parquet')
    target=Path(cfg['targeted']); tm=read_json(target/'manifest.json')
    tr=ref.load_results(target,tm)
    parent_refs={k:v for k,v in tr.items() if v['task']['kind']=='parent' and v['qualified']}
    stress=panel[panel.id.isin(parent_refs)].copy()
    stress['sf']=stress.id.map({k:r['cp_white'] for k,r in parent_refs.items()})
    stress['stratum']=strata(stress)
    metrics(stress,cfg,'targeted-stress',out)
    summary=dict(total=len(d),qualified=len(qualified),frozen_finalists=finalists,
        paired=pairs,adopted=False,limitations=[
        'Fresh positions, excluding known older boards and color mirrors; not reach weighted.',
        'Bootstrap intervals are conditional on sampled positions/strata; opening-family dependence remains.',
        '135-candidate holdout table is exploratory; only pre-frozen finalists are confirmatory comparisons.',
        'SF18 reference is not ground truth; qualification exclusions and depth28 sensitivity are reported.',
        'Source position errors do not establish blended child move-regret/relative-gate safety; Black pilot remains required.'])
    write_json(out/'summary.json',summary)
    emit(out,'complete',total=len(d),qualified=len(qualified),adopted=False)


def pipeline(out,max_buckets=None):
    cfg=check_definition(out)
    with file_lock(out/'pipeline.lock'):
        try:
            if not sample(out,max_buckets): return
            target=Path(cfg['targeted'])
            while True:
                if (out/'STOP').exists():
                    emit(out,'stopped'); return
                state=read_json(target/'status.json')
                if state['state']=='complete': break
                if state['state'] in ('error','stopped-or-bounded'):
                    raise ValueError('preceding diagnostic stopped/failed; inspect before resuming')
                emit(out,'waiting-for-targeted',completed=state.get('completed'),total=state.get('total'))
                time.sleep(30)
            if reference_run(out): report(out)
        except BaseException as e:
            emit(out,'error',error=repr(e)); raise


def main():
    ap=argparse.ArgumentParser(description=__doc__); sub=ap.add_subparsers(dest='cmd',required=True)
    p=sub.add_parser('initialize')
    for n in ('out','panel','targeted','prior','eval-db','menus','engine'):
        p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--exclusions',type=Path,nargs='*',default=[])
    for cmd in ('sample','pipeline','report'):
        p=sub.add_parser(cmd);p.add_argument('--out',type=Path,required=True)
        if cmd!='report': p.add_argument('--max-buckets',type=int)
    args=vars(ap.parse_args()); cmd=args.pop('cmd')
    globals()[cmd](**args)


if __name__=='__main__': main()
