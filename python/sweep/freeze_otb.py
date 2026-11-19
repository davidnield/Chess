"""Freeze a refreshed OTB split using complete archived source-record ancestry.

The source database is read-only. Existing folds are never recomputed from new
IDs. A family spanning historical train/validation/test roles is quarantined.
All descendants of an old game stay in one component, even after splits/merges.
Frozen game text and ratings, not mutable source offsets, are the replay input.
"""
from __future__ import annotations
import argparse
from datetime import date
import hashlib
import json
from pathlib import Path
import duckdb
import pool_from_book
from pool_from_book import write_sorted_export
from .spec import digest
from .stages import atomic_json, artifacts, file_sha

START='rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1'
HISTORICAL_ROWS=35456972
HISTORICAL_SNAPSHOT='31ed3eb35a272fd83bfdc0722d8326628d871ebff176eea89552bb2fd177d5a2'
SCHEMA='otb-split-v2'


def quote(value):return "'"+str(value).replace('\\','/').replace("'","''")+"'"
def scalar(c,sql):return c.execute(sql).fetchall()[0][0]
def check(c,sql,message):
    n=scalar(c,sql)
    if n:raise ValueError(f'{message}: {n}')


def source_prefix(shards, rows, expected):
    total=0; selected=[]
    for h in shards:
        if h['base']!=total:raise ValueError('archived shard ranges are not contiguous')
        total+=h['rows']
        if total<=rows:selected.append(h)
    if sum(h['rows'] for h in selected)!=rows:raise ValueError('historical snapshot is not a complete source prefix')
    value=hashlib.sha256(json.dumps(sorted((Path(h['path']).parent.name,Path(h['path']).name,
        h['base'],h['rows'],h['sha256']) for h in selected)).encode()).hexdigest()
    if value!=expected:raise ValueError('historical source snapshot mismatch')
    return selected


def connect_components(c):
    # Only actual one-to-many descendants need a union. Ordinary corpus growth
    # and many-to-one merges already share the same current game ID.
    parent={}
    def root(n):
        parent.setdefault(n,n)
        while parent[n]!=n:
            parent[n]=parent[parent[n]];n=parent[n]
        return n
    splits=c.execute('SELECT list(current_game_id) FROM lineage GROUP BY old_game_id HAVING count(*)>1').fetchall()
    for (ids,) in splits:
        first=root(ids[0])
        for item in ids[1:]:
            other=root(item); first=root(first)
            if other!=first:
                lo,hi=sorted((first,other));parent[hi]=lo;first=lo
    c.execute('CREATE TEMP TABLE component_map(current_game_id BIGINT,component_id BIGINT)')
    if parent:c.executemany('INSERT INTO component_map VALUES (?,?)',[(n,root(n)) for n in parent])
    return len(splits)


def reconcile(c, historical_rows):
    """Inputs are explicit views: historical, membership, archived_features and game_records."""
    check(c,'SELECT count(*)-count(DISTINCT game_id) FROM historical','duplicate historical game IDs')
    check(c,'SELECT count(*)-count(DISTINCT rid) FROM historical','duplicate historical representatives')
    check(c,'SELECT count(*) FROM historical WHERE split IS NULL OR split NOT BETWEEN 0 AND 4 OR game_id IS NULL OR rid IS NULL','invalid historical assignments')
    check(c,f'SELECT count(*) FROM historical WHERE rid<1 OR rid>{historical_rows}','historical representative outside frozen snapshot')
    # membership.rid is ZERO based; published record_id and historical.rid are ONE based.
    check(c,f'SELECT count(*)-count(DISTINCT rid) FROM membership WHERE rid<{historical_rows}','duplicate archived membership')
    if scalar(c,f'SELECT count(*) FROM membership WHERE rid BETWEEN 0 AND {historical_rows-1}')!=historical_rows:
        raise ValueError('incomplete archived membership')
    check(c,'SELECT count(*) FROM historical o LEFT JOIN membership m ON m.rid=o.rid-1 WHERE m.rid IS NULL OR m.game_id<>o.game_id','historical anchor differs from archived membership')
    c.execute(f'''CREATE TEMP TABLE old_members AS SELECT m.rid+1 record_id,m.game_id old_game_id
        FROM membership m SEMI JOIN historical h ON h.game_id=m.game_id WHERE m.rid<{historical_rows}''')
    c.execute('''CREATE TEMP TABLE ancestry AS SELECT m.*,r.game_id current_game_id
        FROM old_members m LEFT JOIN game_records r USING(record_id)''')
    if scalar(c,'SELECT count(*) FROM ancestry')!=scalar(c,'SELECT count(*) FROM old_members'):
        raise ValueError('current source-record identity is not unique')
    check(c,'SELECT count(*) FROM ancestry WHERE current_game_id IS NULL','historical source record lost')
    check(c,'''SELECT count(*) FROM old_members m LEFT JOIN archived_features f ON f.rid=m.record_id-1
        LEFT JOIN game_records r USING(record_id) WHERE f.rid IS NULL OR r.record_id IS NULL
        OR f.source IS DISTINCT FROM r.source OR f.source_file IS DISTINCT FROM r.source_file
        OR f.source_record IS DISTINCT FROM r.source_record''','source-record locator changed')
    c.execute('''CREATE TEMP TABLE lineage AS SELECT old_game_id,current_game_id,count(*)::BIGINT shared_records
        FROM ancestry GROUP BY 1,2''')
    split_count=connect_components(c)
    c.execute('''CREATE TEMP TABLE family_labels AS
        SELECT coalesce(m.component_id,l.current_game_id) component_id,
          bit_or(CASE WHEN h.split=0 THEN 1 WHEN h.split=1 THEN 2 ELSE 4 END)::BIGINT role_mask,
          min(h.split)::BIGINT fold, count(DISTINCT h.game_id)::BIGINT historical_games
        FROM lineage l JOIN historical h ON h.game_id=l.old_game_id
        LEFT JOIN component_map m USING(current_game_id) GROUP BY 1''')
    c.execute('''CREATE TEMP TABLE existing_assignments AS SELECT ids.current_game_id game_id,
        coalesce(m.component_id,ids.current_game_id) component_id, f.fold,f.role_mask,f.historical_games,
        CASE f.role_mask WHEN 1 THEN 'test' WHEN 2 THEN 'validation' WHEN 4 THEN 'training'
             ELSE 'quarantine' END AS role
        FROM (SELECT DISTINCT current_game_id FROM lineage) ids LEFT JOIN component_map m USING(current_game_id)
        JOIN family_labels f ON f.component_id=coalesce(m.component_id,ids.current_game_id)''')
    # Independent overlap check against every ancestor, rather than just assigned representatives.
    check(c,'''SELECT count(*) FROM lineage l JOIN historical h ON h.game_id=l.old_game_id
        JOIN existing_assignments a ON a.game_id=l.current_game_id WHERE
        (a.role='training' AND h.split<2) OR (a.role='validation' AND h.split<>1)
        OR (a.role='test' AND h.split<>0)''','historical role crossed into another partition')
    check(c,'''SELECT count(*) FROM (SELECT old_game_id FROM lineage l JOIN existing_assignments a
        ON a.game_id=l.current_game_id GROUP BY 1 HAVING count(DISTINCT component_id)>1
        OR count(DISTINCT role)>1)''','split descendants separated')
    return dict(historical_games=scalar(c,'SELECT count(*) FROM historical'),
        ancestral_records=scalar(c,'SELECT count(*) FROM ancestry'),
        current_descendants=scalar(c,'SELECT count(*) FROM existing_assignments'),
        historical_games_split=split_count,
        current_roles=dict(c.execute('SELECT role,count(*) FROM existing_assignments GROUP BY 1').fetchall()),
        historical_role_crossings=0,missing_records=0,changed_locators=0)


def eligible(c, as_of):
    c.execute(f'''CREATE TEMP TABLE eligible AS SELECT game_id::BIGINT game_id,
      representative_record_id::BIGINT representative_record_id, data_source AS source,
      WhiteElo::INTEGER white_elo,BlackElo::INTEGER black_elo,
      coalesce(WhiteRatingSystem,'unspecified') white_rating_system,
      coalesce(BlackRatingSystem,'unspecified') black_rating_system,GameDate::DATE game_date,
      clock_screen,movetext,Result AS result,{quote(START)} AS start_fen
      FROM opening_analysis WHERE WhiteElo>0 AND BlackElo>0 AND least(WhiteElo,BlackElo)<2000
      AND mainline_plies>0 AND GameDate BETWEEN DATE '2013-01-01' AND DATE {quote(as_of)}
      AND lower(coalesce(Variant,'standard')) IN ('standard','normal','chess')
      AND coalesce(nullif(FEN,''),{quote(START)})={quote(START)}''')
    check(c,'SELECT count(*)-count(DISTINCT game_id) FROM eligible','duplicate eligible game ID')
    check(c,"SELECT count(*) FROM eligible WHERE movetext IS NULL OR trim(movetext)=''",'eligible game has no score text')
    c.execute('''CREATE TEMP TABLE assignments AS WITH joined AS (
        SELECT e.*,coalesce(a.component_id,e.game_id) component_id,a.role old_role,
          coalesce(a.fold, ('0x'||substr(sha256('otb-split-v2:'||e.game_id::VARCHAR),1,15))::UBIGINT%5)::BIGINT fold,
          CASE WHEN a.game_id IS NULL THEN 'new-family' ELSE 'preserved-ancestry' END assignment_origin
        FROM eligible e LEFT JOIN existing_assignments a USING(game_id))
        SELECT * EXCLUDE(old_role), coalesce(old_role,CASE fold WHEN 0 THEN 'test' WHEN 1 THEN 'validation'
          ELSE 'training' END) AS role FROM joined''')
    check(c,'''SELECT count(*) FROM (SELECT component_id FROM assignments GROUP BY 1
        HAVING count(DISTINCT role)>1)''','eligible component spans roles')
    return dict(c.execute('SELECT role,count(*) FROM assignments GROUP BY 1').fetchall())


GAME_SCHEMA=[('game_id','int64'),('component_id','int64'),('fold','int64'),('role','large_string'),
 ('assignment_origin','large_string'),('representative_record_id','int64'),('source','large_string'),
 ('white_elo','int32'),('black_elo','int32'),('white_rating_system','large_string'),
 ('black_rating_system','large_string'),('game_date','date32'),('clock_screen','large_string'),
 ('movetext','large_string'),('result','large_string'),('start_fen','large_string')]


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ['database','source-manifest','historical-games','membership','index','shards','identity-code','identity-stamp','out']:
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--as-of',type=date.fromisoformat,required=True)
    p.add_argument('--historical-rows',type=int,default=HISTORICAL_ROWS)
    p.add_argument('--historical-snapshot',default=HISTORICAL_SNAPSHOT)
    a=p.parse_args();a.out.mkdir(parents=True,exist_ok=True)
    inputs=[dict(path=str(path.resolve()),sha256=file_sha(path)) for path in
        (a.source_manifest,a.historical_games,a.membership,a.shards,a.identity_code,a.identity_stamp,Path(__file__),Path(pool_from_book.__file__))]
    stat={str(path.resolve()):dict(size=path.stat().st_size,mtime_ns=path.stat().st_mtime_ns) for path in (a.database,a.index)}
    recipe=dict(schema=SCHEMA,inputs=inputs,source_files=stat,as_of=str(a.as_of),historical_rows=a.historical_rows,
        historical_snapshot=a.historical_snapshot,new_fold='sha256(otb-split-v2:game_id), first 15 hex digits mod 5',
        roles={'training':[2,3,4],'validation':[1],'test':[0]},mixed_roles='quarantine-entire-connected-family')
    identity=digest(recipe);receipt=a.out/'split.json'
    if receipt.exists():
        saved=json.loads(receipt.read_text(encoding='utf8'))
        if saved['identity']!=identity:raise ValueError('split inputs changed; use another output directory')
        artifacts(saved['outputs']);print('Frozen split verified and reused');return
    source_prefix(json.loads(a.shards.read_text(encoding='utf8')),a.historical_rows,a.historical_snapshot)
    if hashlib.sha256(a.identity_code.read_bytes()+a.shards.read_bytes()).hexdigest()!=a.identity_stamp.read_text(encoding='utf8').strip():
        raise ValueError('archived membership code/source stamp mismatch')
    atomic_json(a.out/'status.json',dict(state='reconciling',identity=identity))
    c=duckdb.connect(str(a.database),read_only=True,config={'threads':2,'memory_limit':'4GB','temp_directory':str(a.out/'spill'),'preserve_insertion_order':False})
    try:
        c.execute(f'ATTACH {quote(a.index)} AS archived (READ_ONLY)')
        c.execute('CREATE TEMP VIEW archived_features AS SELECT rid,source,source_file,source_record FROM archived.f')
        c.execute(f'CREATE TEMP VIEW historical AS FROM read_parquet({quote(a.historical_games)})')
        c.execute(f'CREATE TEMP VIEW membership AS FROM read_parquet({quote(a.membership)})')
        checks=reconcile(c,a.historical_rows);print(json.dumps(checks),flush=True)
        checks['eligible_roles']=eligible(c,str(a.as_of));print(json.dumps(checks['eligible_roles']),flush=True)
        checks['new_eligible_games']=scalar(c,"SELECT count(*) FROM assignments WHERE assignment_origin='new-family'")
        checks['assigned_games']=scalar(c,'SELECT count(*) FROM assignments')
        outputs=[]
        exports=[('ancestry.parquet','SELECT * FROM ancestry ORDER BY old_game_id,record_id',
                   [('record_id','int64'),('old_game_id','int64'),('current_game_id','int64')]),
                 ('lineage.parquet','SELECT * FROM lineage ORDER BY old_game_id,current_game_id',
                   [('old_game_id','int64'),('current_game_id','int64'),('shared_records','int64')]),
                 ('historical-assignments.parquet','SELECT * FROM existing_assignments ORDER BY game_id',
                   [('game_id','int64'),('component_id','int64'),('fold','int64'),('role_mask','int64'),('historical_games','int64'),('role','large_string')]),
                 ('games.parquet','SELECT '+','.join(n for n,_ in GAME_SCHEMA)+' FROM assignments ORDER BY game_id',GAME_SCHEMA)]
        for name,query,schema in exports:
            out=a.out/name;rows=write_sorted_export(c,query,out,schema)
            outputs.append(dict(path=str(out.resolve()),sha256=file_sha(out),rows=rows));print(name,rows,flush=True)
        audit=c.execute('SELECT identity_build,snapshot_sha256 FROM audit_build').fetchall()
    except BaseException as exc:
        atomic_json(a.out/'status.json',dict(state='failed',error=str(exc)));raise
    finally:c.close()
    artifacts(inputs)
    for path,old in stat.items():
        st=Path(path).stat()
        if (st.st_size,st.st_mtime_ns)!=(old['size'],old['mtime_ns']):raise ValueError('source database changed during freeze')
    atomic_json(receipt,dict(schema=SCHEMA,identity=identity,recipe=recipe,checks=checks,outputs=outputs,
        source_audit=audit,split_verified=True,training_counts_ready=False,
        limitations=['Published ratings retain their source rating systems.',
          'Unknown clocks follow the existing opening_analysis eligibility screen.',
          'This is a preserved development/test split, not a claim of a never-inspected new test set.']))
    atomic_json(a.out/'status.json',dict(state='split-verified-awaiting-legal-replay',identity=identity))
    print('Split frozen and verified; replay counts not yet published',flush=True)

if __name__=='__main__':main()
