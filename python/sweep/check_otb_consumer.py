"""Verify accepted OTB exports against the actual selective count-reader contract."""
from pathlib import Path
import argparse,json,time,shutil
import chess,duckdb,pyarrow as pa,pyarrow.parquet as pq
from consumer_keys import stored_key,checked_board
from .keyed_parquet import KeyedParquet
from .frontier import Counts,vectors
from .stages import artifacts,atomic_json,file_sha

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--root',type=Path,required=True);args=parser.parse_args()
    root=args.root.resolve();receipt=root/'verified.json';verified=json.loads(receipt.read_text(encoding='utf8'))
    if verified.get('schema')!='otb-counts-verification-v1' or not verified.get('training_counts_ready'):raise ValueError('accepted full count verification required')
    artifacts(verified['outputs']);pair=pa.struct([('lo',pa.int64()),('hi',pa.int64())]);schemas=[];checks=[]
    for item in verified['outputs']:
        path=Path(item['path']);schema=pq.read_schema(path)
        if any(schema.field(name).type!=pair for name in ('parent_hash','child_hash')):raise ValueError('not exact signed full-key structs')
        if schema.field('total').type!=pa.int64():raise ValueError('not exact int64 counts')
        if pq.ParquetFile(path).metadata.num_rows!=item['rows']:raise ValueError('row count differs from accepted receipt')
        schemas.append(dict(path=str(path),key_bits=128,total_type='int64',rows=item['rows']))
        if item['kind']!='pooled':continue
        reader=KeyedParquet(path,'parent_hash');boards=[chess.Board()];required=set()
        for line in ['e4','d4','Nf3','e4 e5 Nf3','e4 c5 Nf3','d4 d5 c4']:
            b=chess.Board()
            for san in line.split():b.push_san(san)
            boards.append(b)
        for group in (sorted({0,len(reader.groups)//2,len(reader.groups)-1}) if reader.groups else []):
            if group<0:continue
            rows=reader.reader.read_row_group(group,columns=['parent_hash','parent_epd']).slice(0,1).to_pylist()
            if rows:
                boards.append(checked_board(rows[0]['parent_epd'],rows[0]['parent_hash']))
                required.add(tuple(rows[0]['parent_hash'][k] for k in ('lo','hi')))
        keys=[stored_key(b,True) for b in boards];fake=dict(keys[0]);fake['hi']^=1
        start=time.monotonic()
        with duckdb.connect(config={'threads':1,'memory_limit':'512MB'}) as db:
            rows=reader.fetch(db,keys).to_dicts()
            if reader.fetch(db,[fake]).height:raise ValueError('wrong high-half key produced a false match')
        found={tuple(r['parent_hash'][k] for k in ('lo','hi')) for r in rows}
        if not required.issubset(found):raise ValueError('selective reader lost known stored positions')
        if reader.groups and not rows:raise ValueError('nonempty source returned no sampled decisions')
        for row in rows:
            if row['parent_hash']==fake:raise ValueError('low-half-only lookup returned a false position')
            b=checked_board(row['parent_epd'],row['parent_hash']);move=b.parse_san(row['move_san'])
            if b.san(move)!=row['move_san'] or move.uci()!=row['move_uci']:raise ValueError('noncanonical move text')
            b.push(move)
            if stored_key(b,True)!=row['child_hash']:raise ValueError('incorrect full child key')
            if not isinstance(row['total'],int) or row['total']<1:raise ValueError('not an uncut observed count')
        grouped={}
        for row in rows:grouped.setdefault(tuple(row['parent_hash'][k] for k in ('lo','hi')),[]).append(row)
        for group_rows in grouped.values():
            board=checked_board(group_rows[0]['parent_epd'],group_rows[0]['parent_hash'])
            record={name:[] for name in Counts.names};record.update(otb=group_rows,aux={})
            legal,broad,adult,targeted,otb,outcomes,nonmove=vectors(board,record)
            if sum(otb)!=sum(r['total'] for r in group_rows):raise ValueError('policy vector lost OTB count mass')
            if outcomes or nonmove:raise ValueError('OTB counts fabricated outcome or nonmove evidence')
        checks.append(dict(path=str(path),role=item['role'],cap=item['cap'],moves_checked=len(rows),policy_positions_checked=len(grouped),false_high_half_hits=0,
            seconds=round(time.monotonic()-start,4),read_metrics=reader.stats));reader.close()
    snapshot=root/'frozen-inputs'/('check_otb_consumer-'+file_sha(Path(__file__))[:16]+'.py');shutil.copyfile(__file__,snapshot)
    dependencies=[];base=Path(__file__).resolve().parents[1]
    for relative in ('consumer_keys.py','zobrist128.py','zobrist.py','eval_lookup128.py','sweep/keyed_parquet.py','sweep/frontier.py','sweep/policy.py','sweep/spec.py'):
        source=base/relative;target=snapshot.parent/(source.stem+'-'+file_sha(source)[:16]+'.py')
        shutil.copyfile(source,target);dependencies.append(dict(path=str(target),sha256=file_sha(target),original_path=str(source)))
    result=dict(schema='otb-consumer-check-v1',counts_receipt=dict(path=str(receipt),sha256=file_sha(receipt)),
        verification_code=dict(path=str(snapshot),sha256=file_sha(snapshot)),consumer_code=dependencies,
        runtime=dict(duckdb=duckdb.__version__,pyarrow=pa.__version__,python_chess=chess.__version__,polars=__import__('polars').__version__),schemas=schemas,checks=checks,passed=True)
    atomic_json(root/'consumer-check.json',result)
    print(json.dumps(dict(files_schema_checked=len(schemas),pooled_files_checked=len(checks),moves_checked=sum(x['moves_checked'] for x in checks),passed=True),indent=2))
if __name__=='__main__':main()
