"""Audit a historical OTB split against refreshed deduplication; never publish training counts.

Representative-record matches diagnose changed/merged identities. They are NOT a
complete ancestry proof: a formerly merged game may have split since the old
snapshot. Candidate assignments stay unusable for training until that proof and
frozen legal replay are supplied. Mixed test/training families are quarantined.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import duckdb
from .spec import digest
from .stages import atomic_json, file_sha
from pool_from_book import write_sorted_export


def audit(con, historical):
    historical=str(Path(historical).resolve()).replace('\\','/').replace("'","''")
    con.execute(f"CREATE TEMP VIEW old_split AS SELECT game_id,rid,split,source FROM read_parquet('{historical}')")
    old=con.execute('''SELECT count(*),count(DISTINCT game_id),count(DISTINCT rid),
        count_if(split IS NULL OR split NOT BETWEEN 0 AND 4 OR game_id IS NULL OR rid IS NULL)
        FROM old_split''').fetchall()[0]
    if old[0]!=old[1] or old[0]!=old[2] or old[3]:
        raise ValueError('historical split has duplicate/missing identities or invalid fold labels')
    con.execute('''CREATE TEMP TABLE split_matches AS SELECT o.*,r.game_id current_game_id,
        r.source current_source FROM old_split o LEFT JOIN game_records r ON r.record_id=o.rid''')
    if con.execute('SELECT count(*) FROM split_matches').fetchall()[0][0]!=old[0]:
        raise ValueError('current record identity is not unique')
    con.execute('''CREATE TEMP TABLE split_candidates AS
        SELECT current_game_id, count(*)::BIGINT old_games,
          count_if(split=0)::BIGINT old_test,
          count_if(split=1)::BIGINT old_validation,
          count_if(split>=2)::BIGINT old_training,
          count_if(game_id<>current_game_id)::BIGINT changed_ids,
          min(split)::BIGINT proposed_fold,
          CASE WHEN count_if(split=0)>0 AND count_if(split<>0)>0 THEN 'quarantine'
               WHEN count_if(split=0)>0 THEN 'test'
               WHEN count_if(split=1)>0 THEN 'validation'
               ELSE 'training' END candidate_role
        FROM split_matches WHERE current_game_id IS NOT NULL GROUP BY current_game_id''')
    def one(sql):return con.execute(sql).fetchall()[0][0]
    result=dict(historical_games=old[0],historical_test_games=one('SELECT count(*) FROM old_split WHERE split=0'),
        matched_current_games=one('SELECT count(*) FROM split_candidates'),
        missing_records=one('SELECT count(*) FROM split_matches WHERE current_game_id IS NULL'),
        source_mismatches=one('SELECT count(*) FROM split_matches WHERE current_game_id IS NOT NULL AND source IS DISTINCT FROM current_source'),
        changed_ids=one('SELECT count(*) FROM split_matches WHERE game_id<>current_game_id'),
        merged_families=one('SELECT count(*) FROM split_candidates WHERE old_games>1'),
        merged_old_games=one('SELECT coalesce(sum(old_games),0) FROM split_candidates WHERE old_games>1'),
        test_training_families=one("SELECT count(*) FROM split_candidates WHERE candidate_role='quarantine'"),
        test_training_old_games=one("SELECT coalesce(sum(old_games),0) FROM split_candidates WHERE candidate_role='quarantine'"))
    result['candidate_roles']={r[0]:r[1] for r in con.execute('SELECT candidate_role,count(*) FROM split_candidates GROUP BY 1').fetchall()}
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',type=Path,required=True)
    p.add_argument('--source-manifest',type=Path,required=True)
    p.add_argument('--historical-games',type=Path,required=True)
    p.add_argument('--out',type=Path,required=True)
    a=p.parse_args()
    st=a.database.stat()
    inputs=dict(database=str(a.database.resolve()),database_bytes=st.st_size,database_mtime_ns=st.st_mtime_ns,
        source_manifest=dict(path=str(a.source_manifest.resolve()),sha256=file_sha(a.source_manifest)),
        historical_games=dict(path=str(a.historical_games.resolve()),sha256=file_sha(a.historical_games)),
        code_sha256=file_sha(__file__))
    identity=digest(inputs)
    target=a.out/'audit.json'
    if target.exists():
        saved=json.loads(target.read_text(encoding='utf8'))
        if saved['identity']!=identity:raise ValueError('audit generation changed; use another output directory')
        for f in saved['outputs']:
            if file_sha(f['path'])!=f['sha256']:raise ValueError('audit output changed')
        print(json.dumps(saved,indent=2));return
    a.out.mkdir(parents=True,exist_ok=True)
    c=duckdb.connect(str(a.database),read_only=True,config={'threads':2,'memory_limit':'4GB'})
    try:
        audit_build=c.execute('SELECT identity_build,snapshot_sha256 FROM audit_build').fetchall()
        if len(audit_build)!=1:raise ValueError('ambiguous source audit identity')
        counts=audit(c,a.historical_games)
        columns=[('current_game_id','int64'),('old_games','int64'),('old_test','int64'),
                 ('old_validation','int64'),('old_training','int64'),('changed_ids','int64'),
                 ('proposed_fold','int64'),('candidate_role','large_string')]
        outputs=[]
        for name,where in [('identity-candidates',''),('quarantine',"WHERE candidate_role='quarantine'")]:
            output=a.out/(name+'.parquet')
            write_sorted_export(c,f'SELECT * FROM split_candidates {where} ORDER BY current_game_id',output,columns)
            outputs.append(dict(path=str(output.resolve()),sha256=file_sha(output)))
    finally:c.close()
    now=a.database.stat()
    if (now.st_size,now.st_mtime_ns)!=(st.st_size,st.st_mtime_ns) or file_sha(a.source_manifest)!=inputs['source_manifest']['sha256']:
        raise ValueError('source changed during audit; do not use partial outputs')
    if file_sha(a.historical_games)!=inputs['historical_games']['sha256']:
        raise ValueError('historical assignment changed during audit')
    report=dict(schema=1,identity=identity,inputs=inputs,source_audit_build=audit_build[0],counts=counts,
        state='requires-complete-ancestry-and-frozen-replay',production_usable=False,outputs=outputs,
        limitations=['Representative-record matches are a diagnostic, not proof that every old game descendant is mapped.',
          'Do not reuse old numeric record offsets against refreshed shard files.',
          'Keep mixed test/training families out of both training and claimed untouched test results.',
          'Old model-selection results must be rechecked on the refreshed deduplicated split before close model comparisons.'])
    atomic_json(target,report)
    print(json.dumps(report,indent=2))


if __name__=='__main__':main()
