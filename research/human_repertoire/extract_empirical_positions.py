"""Stream hash columns and copy only requested empirical rows; no canonical writes."""
from pathlib import Path
import hashlib
import json
import struct
import time
import chess
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from python.zobrist import zobrist_int64
from .hashed_board import HashedBoard
from .io import fingerprint, output_path, sha256, write_json

PREFIX='exposure/'
SOURCE=Path('E:/chess/position-stats')
TAG='ge1800_2013_2026_brc'


def footer_hash(path):
    with Path(path).open('rb') as f:
        f.seek(-8,2); length,magic=struct.unpack('<I4s',f.read(8))
        if magic!=b'PAR1' or length>16*1024*1024: raise ValueError('invalid or oversized parquet footer')
        f.seek(-8-length,2); return hashlib.sha256(f.read(length)).hexdigest()


def requested():
    root=HashedBoard()
    for m in ['e4','e5','Nf3','Nc6']: root.push_san(m)
    boards={str(zobrist_int64(root)):root.fen()}
    for move in root.legal_moves:
        b=root.copy(); b.push(move); boards[str(zobrist_int64(b))]=b.fen()
    for reply in ['exd4','Nf6','d6']:
        b=root.copy(); b.push_san('d4'); b.push_san(reply); boards[str(zobrist_int64(b))]=b.fen()
    return boards


def scan(path,key_column,keys,start):
    pf=pq.ParquetFile(path); selected=[]; decoded=0; groups=0
    if pf.metadata.num_rows>40000000 or pf.num_row_groups>400: raise ValueError('source row/group cap')
    values=pa.array([int(k) for k in keys],type=pa.int64())
    for i in range(pf.num_row_groups):
        if time.monotonic()-start>240: raise TimeoutError('snapshot lookup time cap')
        rg=pf.metadata.row_group(i)
        if rg.total_byte_size>32*1024*1024: raise ValueError('row group memory bound')
        col=pf.read_row_group(i,columns=[key_column],use_threads=False)
        mask=pc.is_in(col[key_column],value_set=values); decoded+=col.nbytes
        if pc.any(mask).as_py():
            rows=pf.read_row_group(i,use_threads=False); decoded+=rows.nbytes; groups+=1
            selected.extend(rows.filter(mask).to_pylist()); del rows
        # Measured unsorted groups required >2 GiB cumulative decoding for
        # 249 selected rows. This is total I/O, not resident memory; retain
        # the 32 MiB row-group bound and 5,000-row output bound.
        if decoded>4*1024**3 or len(selected)>5000:
            raise ValueError(f'bounded projection/output cap: decoded={decoded}, rows={len(selected)}, row_group={i}, payload_groups={groups}')
        del col,mask
    return selected,{'row_groups_read_hash':pf.num_row_groups,'row_groups_read_payload':groups,'decoded_bytes':decoded,'source_rows':pf.metadata.num_rows,'schema':str(pf.schema_arrow)}


def main():
    boards=requested(); start=time.monotonic()
    paths={k:SOURCE/(prefix+TAG+'.parquet') for k,prefix in [('stats','position_stats_pooled_'),('aux','position_stats_aux_pooled_')]}
    identity={k:{**fingerprint(p),'footer_sha256':footer_hash(p)} for k,p in paths.items()}
    target=output_path(PREFIX+'empirical.json')
    if target.exists():
        saved=json.loads(target.read_text())
        if saved['sources']!=identity or saved['requested']!=boards: raise ValueError('frozen snapshot source drift')
        print('Existing empirical snapshot verified'); return
    rows={}; work={}
    for kind,path in paths.items():
        rows[kind],work[kind]=scan(path,'parent_hash' if kind=='stats' else 'position_hash',boards,start)
    for k,p in paths.items():
        if identity[k]!={**fingerprint(p),'footer_sha256':footer_hash(p)}: raise ValueError('source changed during extraction')
    grouped={k:[] for k in boards}; aux={str(r['position_hash']):r for r in rows['aux']}
    for r in rows['stats']:
        key=str(r['parent_hash']); b=HashedBoard(r['parent_epd'])
        if str(zobrist_int64(b))!=key or b.fen().split()[:4]!=boards[key].split()[:4]: raise ValueError('empirical parent/FEN mismatch')
        if r['event']!='Pooled' or r['elo_band']!=0: raise ValueError('unexpected population slice')
        if r['total']<=0 or sum(r[n] for n in ['white_wins','draws','black_wins'])!=r['total']: raise ValueError('invalid empirical counts')
        b.push_san(r['move_san'])
        if str(zobrist_int64(b))!=str(r['child_hash']): raise ValueError('empirical child hash mismatch')
        grouped[key].append(r)
    if any(len({r['move_san'] for r in rs})!=len(rs) for rs in grouped.values()): raise ValueError('duplicate edges')
    out={'sources':identity,'requested':boards,'stats':grouped,'aux':aux,'work':work,'seconds':time.monotonic()-start,
        'extractor_sha256':sha256(__file__),'cohort':'Artifact tag 2013-2026 ge1800 brc; canonical pooler documents mean_elo>=1800 and pooled BRC. No generation sidecar found; not a claim of both-player/gap filtering.'}
    write_json(PREFIX+'empirical.json',out)
    print(json.dumps({'positions':len(boards),'found':sum(bool(rs) for rs in grouped.values()),'copied_edges':len(rows['stats']),'aux_rows':len(aux),'seconds':out['seconds'],'work':work},indent=2))


if __name__=='__main__': main()
