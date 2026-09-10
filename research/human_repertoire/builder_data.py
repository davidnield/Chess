"""Generic bounded source projections; no historical experiment state needed."""
import json
import time
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from .extract_empirical_positions import footer_hash
from .io import fingerprint,sha256,output_path,write_json
from .hashed_board import HashedBoard
from python.zobrist import zobrist_int64

STATS=['parent_hash','child_hash','move_san','total','white_wins','draws','black_wins','event','elo_band']
AUX=['position_hash']+[p+'_'+s for p in ['other','horizon','term_normal','term_flag','term_other'] for s in ['total','white_wins','draws','black_wins']]
REFERENCE=['position_hash','best_move','value','event','elo_band','side_to_move']


def source_identity(path): return {**fingerprint(path),'footer_sha256':footer_hash(path)}


def project(path,key_column,keys,columns):
    started=time.monotonic(); pf=pq.ParquetFile(path); rows=[]; decoded=0; payloads=0
    if len(keys)>64 or pf.metadata.num_rows>40000000 or pf.num_row_groups>400: raise ValueError('source/query cap')
    values=pa.array([int(k) for k in keys],type=pa.int64())
    for index in range(pf.num_row_groups):
        if time.monotonic()-started>240: raise TimeoutError('projection time cap')
        if pf.metadata.row_group(index).total_byte_size>32*1024**2: raise ValueError('row group cap')
        hashes=pf.read_row_group(index,columns=[key_column],use_threads=False); decoded+=hashes.nbytes
        mask=pc.is_in(hashes[key_column],value_set=values)
        if pc.any(mask).as_py():
            table=pf.read_row_group(index,columns=columns,use_threads=False); decoded+=table.nbytes; payloads+=1
            rows.extend(table.filter(mask).to_pylist()); del table
        if decoded>4*1024**3 or len(rows)>5000: raise ValueError('projection/output cap')
    return rows,{'decoded_bytes':decoded,'rows':len(rows),'groups':pf.num_row_groups,'payload_groups':payloads,'seconds':time.monotonic()-started}


def snapshot(prefix,label,boards,config,identities):
    stamp={'boards':boards,'sources':identities,'code_sha256':sha256(__file__)}
    target=output_path(prefix+'queries/'+label+'.json')
    if target.exists():
        cached=json.loads(target.read_text())
        if cached['identity']!=stamp: raise ValueError('query cache drift')
        return cached
    out={'identity':stamp,'work':{}}
    for kind,column,columns in [('source_stats','parent_hash',STATS),('source_aux','position_hash',AUX),('reference','position_hash',REFERENCE)]:
        rows,work=project(config[kind],column,boards,columns); out['work'][kind]=work
        if kind=='source_stats':
            grouped={k:[] for k in boards}
            for r in rows:
                k=str(r['parent_hash']); board=HashedBoard(boards[k])
                if str(zobrist_int64(board))!=k or r['event']!='Pooled' or r['elo_band']!=0: raise ValueError('source key/cohort')
                if r['total']<=0 or sum(r[x] for x in ['white_wins','draws','black_wins'])!=r['total']: raise ValueError('invalid source counts')
                board.push_san(r['move_san'])
                if str(zobrist_int64(board))!=str(r['child_hash']): raise ValueError('source legal child drift')
                grouped[k].append(r)
            if any(len(rs)!=len({r['move_san'] for r in rs}) for rs in grouped.values()): raise ValueError('duplicate moves')
            out['stats']=grouped
        else:
            grouped={str(r['position_hash']):r for r in rows}
            if len(grouped)!=len(rows): raise ValueError('duplicate position')
            if kind=='source_aux':
                for r in rows:
                    for p in ['other','horizon','term_normal','term_flag','term_other']:
                        if any(r[p+'_'+s]<0 for s in ['total','white_wins','draws','black_wins']) or sum(r[p+'_'+s] for s in ['white_wins','draws','black_wins'])!=r[p+'_total']:
                            raise ValueError('aux counts')
                out['aux']=grouped
            else:
                for r in rows:
                    if r['event']!='Pooled' or r['elo_band']!=0: raise ValueError('reference cohort')
                    if r['best_move'] is not None:
                        b=HashedBoard(boards[str(r['position_hash'])]); b.push_san(r['best_move'])
                out['reference']=grouped
        if source_identity(config[kind])!=identities[kind]: raise ValueError('source changed')
    write_json(prefix+'queries/'+label+'.json',out); return out


def accounting(rows,aux):
    stored=sum(r['total'] for r in rows); other=aux['other_total']+aux['horizon_total']
    ends=sum(aux[p+'_total'] for p in ['term_normal','term_flag','term_other']); continuation=stored+other
    if continuation<=0: raise ValueError('no supported continuation')
    wdl=tuple(sum(aux[p+'_'+s] for p in ['term_normal','term_flag','term_other'])/ends for s in ['white_wins','draws','black_wins']) if ends else (0,1,0)
    return {'probabilities':{r['move_san']:r['total']/continuation for r in rows},'tail':other/continuation,
            'end_probability':ends/(continuation+ends),'end_wdl':wdl}
