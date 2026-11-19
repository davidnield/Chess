"""Independent census, flow, full aggregation and Python replay checks for OTB counts."""
from __future__ import annotations
import argparse,json,logging,shutil,time
from pathlib import Path
import duckdb
from .stages import artifacts,atomic_json,file_sha
from .spec import digest
from .freeze_otb import quote
from .otb_counts import ROLES,connection,ledger_views,entry,reference,status,reserve

ROOT_KEY=(5060803636482931868,4427599749834255219)
def n(c,sql):return c.execute(sql).fetchone()[0]
def reject(c,sql,message):
    count=n(c,sql)
    if count:raise ValueError(f'{message}: {count}')

def verify_ledger(c,cap=51):
    reject(c,'SELECT count(*) FROM (SELECT game_id FROM game_status GROUP BY 1 HAVING count(*)<>1)','duplicate game dispositions')
    reject(c,'''SELECT count(*) FROM frozen f FULL OUTER JOIN game_status s USING(game_id)
        WHERE f.game_id IS NULL OR s.game_id IS NULL OR f.role IS DISTINCT FROM s.role''','frozen game/role census mismatch')
    reject(c,f'''SELECT count(*) FROM game_status WHERE status IS NULL OR replayed_plies IS NULL
        OR (role='quarantine' AND (status<>'quarantine' OR replayed_plies<>-10))
        OR (role<>'quarantine' AND status='quarantine')
        OR (status='valid' AND replayed_plies NOT BETWEEN 1 AND {cap})
        OR (status<>'valid' AND replayed_plies>=0)
        OR (status,replayed_plies) NOT IN (('syntax-error',-1),('illegal-move',-2),('null-move',-3),('empty-score',-4),('quarantine',-10)) AND status<>'valid' ''','invalid game disposition')
    reject(c,'''WITH k AS (SELECT game_id,count(*) moves,count(DISTINCT ply) plies,min(ply) AS first_ply,max(ply) AS last_ply FROM ledger GROUP BY 1)
        SELECT count(*) FROM game_status s FULL OUTER JOIN k USING(game_id) WHERE s.game_id IS NULL
        OR (s.status='valid' AND (k.moves IS NULL OR k.moves<>s.replayed_plies OR k.plies<>s.replayed_plies OR k.first_ply<>1 OR k.last_ply<>s.replayed_plies))
        OR (s.status<>'valid' AND k.moves IS NOT NULL)''','missing/duplicate/truncated game decisions')
    reject(c,'''SELECT count(*) FROM ledger l LEFT JOIN frozen f USING(game_id) WHERE f.game_id IS NULL
        OR l.role IS DISTINCT FROM f.role OR l.role='quarantine'
        OR l.mover_elo IS DISTINCT FROM CASE WHEN l.ply%2=1 THEN f.white_elo ELSE f.black_elo END
        OR l.mover_band IS DISTINCT FROM CASE WHEN l.mover_elo<1500 THEN 0 WHEN l.mover_elo<1800 THEN 1500 WHEN l.mover_elo<2000 THEN 1800 ELSE 2000 END
        OR l.parent_lo IS NULL OR l.parent_hi IS NULL OR l.child_lo IS NULL OR l.child_hi IS NULL
        OR l.parent_epd IS NULL OR l.move_san IS NULL OR l.move_uci IS NULL''','decision identity/role/mover mismatch')
    reject(c,f'''WITH chain AS (SELECT *,lag(child_lo) OVER w prior_lo,lag(child_hi) OVER w prior_hi
        FROM ledger WINDOW w AS (PARTITION BY game_id ORDER BY ply)) SELECT count(*) FROM chain
        WHERE parent_lo IS DISTINCT FROM CASE WHEN ply=1 THEN {ROOT_KEY[0]} ELSE prior_lo END
        OR parent_hi IS DISTINCT FROM CASE WHEN ply=1 THEN {ROOT_KEY[1]} ELSE prior_hi END''','broken full-key move chain')
    return dict(games=n(c,'SELECT count(*) FROM game_status'),moves=n(c,'SELECT count(*) FROM ledger'),
        qualifying_moves=n(c,'SELECT count(*) FROM ledger WHERE mover_elo BETWEEN 1 AND 1999'),
        dispositions=[dict(zip(('role','status','games'),r)) for r in c.execute('SELECT role,status,count(*) FROM game_status GROUP BY 1,2 ORDER BY 1,2').fetchall()])

def verify_master(c,path,role):
    # Rebuild expected counts directly from game decisions, with flat key halves.
    c.execute(f"CREATE OR REPLACE TEMP VIEW observed AS SELECT * FROM read_parquet({quote(path)})")
    keys='parent_lo,parent_hi,child_lo,child_hi,move_san,move_uci,parent_epd,ply,mover_band'
    reject(c,f'''WITH expected AS (SELECT {keys},count(*) amount FROM ledger
        WHERE role={quote(role)} AND mover_elo BETWEEN 1 AND 1999 GROUP BY {keys}),
        actual AS (SELECT parent_hash.lo parent_lo,parent_hash.hi parent_hi,child_hash.lo child_lo,child_hash.hi child_hi,
        move_san,move_uci,parent_epd,ply,mover_band,sum(total) amount,count(*) copies FROM observed GROUP BY ALL)
        SELECT count(*) FROM expected e FULL OUTER JOIN actual a USING({keys})
        WHERE e.amount IS NULL OR a.amount IS NULL OR e.amount<>a.amount OR a.copies<>1''','full master count mismatch')
    return dict(rows=n(c,'SELECT count(*) FROM observed'),moves=n(c,'SELECT coalesce(sum(total),0) FROM observed'))

def verify_pool(c,path,master,cap):
    c.execute(f"CREATE OR REPLACE TEMP VIEW observed AS SELECT * FROM read_parquet({quote(path)})")
    keys='parent_hash,child_hash,move_san,move_uci,parent_epd'
    reject(c,f'''WITH expected AS (SELECT {keys},sum(total) amount,min(ply) AS first_ply FROM read_parquet({quote(master)})
        WHERE ply<={cap} GROUP BY {keys}),actual AS (SELECT {keys},sum(total) amount,min(ply) AS first_ply,max(ply) AS last_ply,count(*) copies FROM observed GROUP BY {keys})
        SELECT count(*) FROM expected e FULL OUTER JOIN actual a USING({keys}) WHERE e.amount IS NULL OR a.amount IS NULL
        OR e.amount<>a.amount OR e.first_ply<>a.first_ply OR a.first_ply<>a.last_ply OR a.copies<>1''','full pooled count mismatch')
    reject(c,'''SELECT count(*) FROM (SELECT parent_hash,move_san FROM observed GROUP BY 1,2 HAVING count(*)<>1)''','duplicate pooled SAN choice')
    reject(c,'''SELECT count(*) FROM (SELECT parent_hash FROM observed GROUP BY 1 HAVING min(parent_epd)<>max(parent_epd))''','full key maps to inconsistent positions')
    return dict(rows=n(c,'SELECT count(*) FROM observed'),moves=n(c,'SELECT coalesce(sum(total),0) FROM observed'))

def verify_sample(c,per_role=256):
    c.execute(f'''CREATE TEMP TABLE sample_ids AS SELECT game_id FROM game_status WHERE status='valid'
        QUALIFY row_number() OVER (PARTITION BY role ORDER BY sha256('otb-independent-counts-v1:'||game_id::VARCHAR))<={per_role}''')
    games=c.execute('SELECT f.game_id,f.movetext FROM frozen f JOIN sample_ids USING(game_id) ORDER BY game_id').fetchall()
    rows=c.execute('''SELECT game_id,parent_lo,parent_hi,child_lo,child_hi,move_san,move_uci,parent_epd FROM ledger
        SEMI JOIN sample_ids USING(game_id) ORDER BY game_id,ply''').fetchall()
    by={}
    for gid,*r in rows:by.setdefault(gid,[]).append(tuple(r))
    for gid,text in games:
        ref=reference(text,51)
        if ref is None or ref!=by.get(gid):raise ValueError(f'independent stored-ledger replay mismatch: {gid}')
    # Rejections are explicit. A stricter native syntax rejection may merit investigation,
    # but can never become an unlabelled contribution or be called fully accepted.
    rejections=c.execute('''SELECT f.game_id,s.status,f.movetext FROM frozen f JOIN game_status s USING(game_id)
        WHERE s.status NOT IN ('valid','quarantine') ORDER BY f.game_id LIMIT 256''').fetchall()
    rejected=[dict(game_id=gid,status=state,python_valid=reference(text,51) is not None) for gid,state,text in rejections]
    return dict(games=len(games),positions=len(rows),mismatches=0,rejection_sample=rejected)

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,required=True);a=p.parse_args();root=a.root.resolve();reserve(root)
    recipe_path=root/'recipe.json';meta=json.loads(recipe_path.read_text(encoding='utf8'));recipe=meta['recipe'];identity=meta['identity']
    if digest(recipe)!=identity or recipe['schema']!='otb-counts-v1' or recipe['max_ply']!=51:raise ValueError('unaccepted count recipe')
    artifacts(recipe['inputs']);replayed=json.loads((root/'replayed.json').read_text())
    if replayed['identity']!=identity:raise ValueError('replay identity mismatch')
    artifacts(replayed['chunks']);expected=set();chunk_checks=[]
    for item in replayed['chunks']:
        receipt=json.loads(Path(item['path']).read_text());artifacts(receipt['outputs'])
        if receipt['identity']!=identity:raise ValueError('chunk identity mismatch')
        expected.update(str(Path(x['path']).resolve()) for x in receipt['outputs']);chunk_checks.append(receipt['checks'])
    actual={str(x.resolve()) for x in (root/'chunks').glob('*.parquet')}
    if expected!=actual:raise ValueError('extra or missing decision chunks')
    source=next(x['path'] for x in recipe['inputs'] if Path(x['path']).name=='games.parquet')
    outputs=[];logging.getLogger('chess.pgn').setLevel(logging.CRITICAL)
    status(root,'independently-verifying-counts');start=time.monotonic()
    try:
        with connection(root) as c:
            ledger_views(c,root,source);print('verify complete game census and flow',flush=True);ledger=verify_ledger(c)
            print(json.dumps(ledger),flush=True)
            for role in ROLES:
                master=root/'counts'/f'{role}-by-ply-band.parquet'
                for path,cap in [(master,51)]+[(root/'counts'/f'{role}-p{k}.parquet',k) for k in (30,50)]:
                    saved=json.loads(path.with_suffix('.json').read_text())
                    if saved['identity']!=identity:raise ValueError('aggregate identity mismatch')
                    artifacts([saved['output']]);status(root,'verifying-exact-aggregation',output=path.name);print('verify',path.name,flush=True)
                    checks=verify_master(c,path,role) if path==master else verify_pool(c,path,master,cap)
                    if checks['rows']!=saved['output']['rows']:raise ValueError('aggregate receipt row count mismatch')
                    outputs.append(dict(**saved['output'],role=role,cap=cap,kind='by-ply-band' if path==master else 'pooled',checks=checks))
            print('independent Python game replay',flush=True);sample=verify_sample(c)
        artifacts(outputs);artifacts(recipe['inputs']);archive=root/'frozen-inputs';archive.mkdir(exist_ok=True);archived=[]
        for i,item in enumerate(recipe['inputs']):
            source_path=Path(item['path'])
            if source_path.suffix=='.parquet':archived.append(item);continue
            dest=archive/f'{i:02d}-{source_path.name}';shutil.copyfile(source_path,dest);artifacts([dict(path=str(dest),sha256=item['sha256'])]);archived.append(entry(dest,original_path=item['path']))
        verifier=archive/f'verify_otb_counts-{file_sha(Path(__file__))[:16]}.py';shutil.copyfile(__file__,verifier)
        report=dict(schema='otb-counts-verification-v1',identity=identity,recipe=entry(recipe_path),replayed=entry(root/'replayed.json'),
            source_inputs=archived,outputs=outputs,ledger=ledger,independent_replay=sample,
            native_crosschecks={key:sum(x[key] for x in chunk_checks) for key in ('sample_games','sample_positions')},
            verification_code=entry(verifier),seconds=round(time.monotonic()-start,1),training_counts_ready=True,
            limitations=['Legal-key parity is cross-checked on sampled games; game census, flow and every aggregate count are checked exhaustively.',
                'Malformed requested prefixes are excluded in their entirety and reported, not counted as endings.',
                'Unknown clocks and mixed published rating systems retain the frozen split eligibility.'])
        atomic_json(root/'verified.json',report);status(root,'verified',training_counts_ready=True)
        print(json.dumps(report,indent=2),flush=True)
    except BaseException as exc:status(root,'verification-failed',error=str(exc));raise
if __name__=='__main__':main()
