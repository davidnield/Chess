"""Independently verify a frozen OTB split and archive its small provenance inputs."""
from __future__ import annotations
import argparse
import io
import json
import logging
from pathlib import Path
import shutil
import duckdb
import chess
import chess.pgn
from .stages import artifacts,atomic_json,file_sha
from .spec import digest
from .freeze_otb import START,quote


def n(c,sql):return c.execute(sql).fetchall()[0][0]
def reject(c,sql,message):
    value=n(c,sql)
    if value:raise ValueError(f'{message}: {value}')


def equal_rows(c,left,right,message):
    reject(c,f'SELECT count(*) FROM (({left} EXCEPT {right}) UNION ALL ({right} EXCEPT {left}))',message)


def validate_tables(c, historical_rows, as_of):
    for table,columns in (
        ('ancestry',('record_id','old_game_id','current_game_id')),
        ('lineage',('old_game_id','current_game_id','shared_records')),
        ('labels',('game_id','component_id','fold','role_mask','historical_games','role')),
        ('frozen_games',('game_id','component_id','fold','assignment_origin','representative_record_id'))):
        reject(c,'SELECT count(*) FROM '+table+' WHERE '+' OR '.join(x+' IS NULL' for x in columns),
            'null identity/assignment in '+table)
    reject(c,'SELECT count(*)-count(DISTINCT record_id) FROM ancestry','ancestry duplicates')
    equal_rows(c,'SELECT record_id,old_game_id FROM ancestry',
        f'''SELECT m.rid+1,m.game_id FROM membership m JOIN historical h ON h.game_id=m.game_id
            WHERE m.rid<{historical_rows}''','incomplete ancestral-record census')
    equal_rows(c,'SELECT old_game_id,current_game_id,shared_records FROM lineage',
        'SELECT old_game_id,current_game_id,count(*) FROM ancestry GROUP BY 1,2','lineage does not match ancestral records')
    reject(c,'SELECT count(*)-count(DISTINCT game_id) FROM labels','duplicate historical assignments')
    equal_rows(c,'SELECT game_id FROM labels','SELECT DISTINCT current_game_id FROM ancestry','unassigned descendant')
    reject(c,'''SELECT count(*) FROM (SELECT old_game_id FROM lineage l JOIN labels a ON a.game_id=l.current_game_id
       GROUP BY 1 HAVING count(DISTINCT component_id)>1 OR count(DISTINCT role)>1)''','split family crosses partitions')
    c.execute('''CREATE OR REPLACE TEMP VIEW expected_labels AS SELECT a.component_id,
       bit_or(CASE WHEN h.split=0 THEN 1 WHEN h.split=1 THEN 2 ELSE 4 END)::BIGINT mask,
       min(h.split) fold,count(DISTINCT h.game_id) historical_games
       FROM lineage l JOIN historical h ON h.game_id=l.old_game_id JOIN labels a ON a.game_id=l.current_game_id GROUP BY 1''')
    reject(c,'''SELECT count(*) FROM labels a JOIN expected_labels e USING(component_id) WHERE a.role_mask<>e.mask
       OR a.fold<>e.fold OR a.historical_games<>e.historical_games OR a.role IS DISTINCT FROM
       CASE e.mask WHEN 1 THEN 'test' WHEN 2 THEN 'validation' WHEN 4 THEN 'training' ELSE 'quarantine' END''',
       'training/validation/test overlap or wrong quarantine')
    reject(c,'SELECT count(*)-count(DISTINCT game_id) FROM frozen_games','duplicate frozen game')
    reject(c,f'''SELECT count(*) FROM frozen_games WHERE white_elo<=0 OR black_elo<=0 OR
      white_elo IS NULL OR black_elo IS NULL OR least(white_elo,black_elo)>=2000
      OR game_date NOT BETWEEN DATE '2013-01-01' AND DATE {quote(as_of)} OR game_date IS NULL
      OR start_fen IS DISTINCT FROM {quote(START)} OR movetext IS NULL OR trim(movetext)=''
      OR role NOT IN ('training','validation','test','quarantine') OR role IS NULL''','invalid eligible frozen row')
    reject(c,'''SELECT count(*) FROM frozen_games g LEFT JOIN labels a USING(game_id) WHERE
      (a.game_id IS NOT NULL AND (g.role<>a.role OR g.fold<>a.fold OR g.component_id<>a.component_id
        OR g.assignment_origin<>'preserved-ancestry')) OR
      (a.game_id IS NULL AND (g.assignment_origin<>'new-family' OR g.component_id<>g.game_id
        OR g.fold<>(('0x'||substr(sha256('otb-split-v2:'||g.game_id::VARCHAR),1,15))::UBIGINT%5)
        OR g.role<>CASE g.fold WHEN 0 THEN 'test' WHEN 1 THEN 'validation' ELSE 'training' END))''',
       'frozen assignment differs from historical lineage')
    reject(c,'''SELECT count(*) FROM (SELECT component_id FROM frozen_games GROUP BY 1
       HAVING count(DISTINCT role)>1)''','frozen component spans roles')
    return dict(ancestral_records=n(c,'SELECT count(*) FROM ancestry'),
        historical_games=n(c,'SELECT count(DISTINCT old_game_id) FROM lineage'),
        current_descendants=n(c,'SELECT count(*) FROM labels'),assigned_games=n(c,'SELECT count(*) FROM frozen_games'),
        eligible_roles=dict(c.execute('SELECT role,count(*) FROM frozen_games GROUP BY 1').fetchall()),
        historical_role_crossings=0)


def read_score_text(text):
    # PGN readers can return an empty game for leading comments followed by a
    # blank line. Such source annotations are not a missing/illegal score.
    stream=io.StringIO(text)
    while True:
        game=chess.pgn.read_game(stream)
        if game is None or game.errors or any(game.mainline_moves()):return game


def replay_sample(c, per_role=64, max_ply=50):
    # All rows are reproducibly selected. Sampling checks source text usability;
    # it never turns a sampled check into a claim that all games parsed legally.
    logger=logging.getLogger('chess.pgn');prior=logger.level;logger.setLevel(logging.CRITICAL)
    result=dict(games=0,positions=0,malformed_prefixes=[],per_role={})
    try:
        for role in ('training','validation','test','quarantine'):
            rows=c.execute('''SELECT game_id,movetext FROM frozen_games WHERE role=?
                ORDER BY sha256('otb-replay-smoke-v2:'||game_id::VARCHAR) LIMIT ?''',[role,per_role]).fetchall()
            count=0
            for gid,text in rows:
                game=read_score_text(text)
                moves=[] if game is None else list(game.mainline_moves())
                valid=bool(moves) and game is not None and game.board().fen()==START and (len(moves)>=max_ply or not game.errors)
                if not valid:result['malformed_prefixes'].append(dict(game_id=gid,role=role))
                else:
                    b=game.board()
                    for m in moves[:max_ply]:
                        if m not in b.legal_moves:raise ValueError('independent replay returned illegal move')
                        b.push(m);result['positions']+=1
                    count+=1
                result['games']+=1
            result['per_role'][role]=dict(sampled=len(rows),valid=count)
    finally:logger.setLevel(prior)
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--split',type=Path,required=True)
    p.add_argument('--sample-per-role',type=int,default=64);a=p.parse_args()
    split_sha256=file_sha(a.split)
    split=json.loads(a.split.read_text(encoding='utf8'))
    if split.get('schema')!='otb-split-v2' or not split.get('split_verified'):raise ValueError('unaccepted split')
    if digest(split['recipe'])!=split['identity']:raise ValueError('split recipe identity mismatch')
    root=a.split.parent;frozen=root/'frozen-inputs';frozen.mkdir(exist_ok=True)
    archived=[]
    for i,item in enumerate(split['recipe']['inputs']):
        dest=frozen/f'{i:02d}-{Path(item["path"]).name}'
        if not dest.exists():
            artifacts([item]);tmp=dest.with_suffix(dest.suffix+'.tmp');shutil.copyfile(item['path'],tmp)
            if file_sha(tmp)!=item['sha256']:raise ValueError('source changed while archiving')
            tmp.replace(dest)
        entry=dict(path=str(dest.resolve()),sha256=item['sha256'],original_path=item['path']);artifacts([entry]);archived.append(entry)
    artifacts(split['outputs'])
    by_name={Path(r['path']).name:r for r in split['outputs']}
    old=next(r for r in archived if Path(r['original_path']).name=='games.parquet')
    member=next(r for r in archived if Path(r['original_path']).name=='membership.parquet')
    c=duckdb.connect(config={'threads':2,'memory_limit':'4GB','temp_directory':str(root/'verify-spill'),'preserve_insertion_order':False})
    try:
        views={'historical':old['path'],'membership':member['path'],'ancestry':by_name['ancestry.parquet']['path'],
            'lineage':by_name['lineage.parquet']['path'],'labels':by_name['historical-assignments.parquet']['path'],
            'frozen_games':by_name['games.parquet']['path']}
        for name,path in views.items():c.execute(f'CREATE VIEW {name} AS SELECT * FROM read_parquet({quote(path)})')
        checks=validate_tables(c,split['recipe']['historical_rows'],split['recipe']['as_of'])
        for key,value in checks.items():
            if value!=split['checks'][key]:raise ValueError(f'independent count mismatch: {key}')
        sample=replay_sample(c,a.sample_per_role)
    finally:c.close()
    artifacts(split['outputs']);artifacts(archived)
    if file_sha(a.split)!=split_sha256:raise ValueError('split receipt changed during verification')
    code=frozen/f'verify_otb_split-{file_sha(Path(__file__))[:16]}.py';shutil.copyfile(__file__,code)
    report=dict(schema='otb-split-verification-v2',split=dict(path=str(a.split.resolve()),sha256=split_sha256),
        archived_inputs=archived,outputs=split['outputs'],checks=checks,legal_replay_sample=sample,
        verification_code=dict(path=str(code),sha256=file_sha(code)),split_verified=True,training_counts_ready=False)
    atomic_json(root/'verified.json',report)
    atomic_json(root/'status.json',dict(state='split-independently-verified-ready-for-count-replay',
        split_sha256=report['split']['sha256'],training_counts_ready=False))
    print(json.dumps(dict(checks=checks,legal_replay_sample=sample),indent=2))

if __name__=='__main__':main()
