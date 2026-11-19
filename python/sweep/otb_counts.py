"""Frozen OTB scores -> audited 128-bit, uncut, mover-filtered counts."""
from __future__ import annotations
import argparse,ctypes as ct,json,logging,os,shutil,time
from pathlib import Path
import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import duckdb
import chess
from zobrist128 import zobrist128
from pool_from_book import write_sorted_export
from .verify_otb_split import read_score_text
from .freeze_otb import quote
from .stages import artifacts,atomic_json,file_sha
from .spec import digest

ROLES=('training','validation','test')
KEY='struct<lo: int64, hi: int64>'
DTYPE=np.dtype([(x,'<i8') for x in ('parent_lo','parent_hi','child_lo','child_hi')]+ [('san','S12'),('uci','S8'),('epd','S96')],align=True)
LEDGER_SCHEMA=pa.schema([('game_id',pa.int64()),('role',pa.large_string()),('ply',pa.int16()),('mover_elo',pa.int32()),('mover_band',pa.int16())]+[(x,pa.int64()) for x in ('parent_lo','parent_hi','child_lo','child_hi')]+[(x,pa.large_string()) for x in ('move_san','move_uci','parent_epd')])
STATUS_SCHEMA=pa.schema([('game_id',pa.int64()),('role',pa.large_string()),('replayed_plies',pa.int16()),('status',pa.large_string())])
MASTER_SCHEMA=[('parent_hash',KEY),('child_hash',KEY),('move_san','large_string'),('move_uci','large_string'),('parent_epd','large_string'),('ply','int16'),('mover_band','int16'),('total','int64')]
POOL_SCHEMA=[(x,t) for x,t in MASTER_SCHEMA if x!='mover_band']
ERRORS={-1:'syntax-error',-2:'illegal-move',-3:'null-move',-4:'empty-score',-10:'quarantine'}

class Native:
    def __init__(self,path):
        self.dll=ct.CDLL(str(Path(path).resolve()));self.dll.edge_size.restype=ct.c_size_t
        if self.dll.edge_size()!=DTYPE.itemsize:raise ValueError('native record ABI mismatch')
        self.dll.replay_batch.argtypes=[ct.c_void_p,ct.c_size_t,ct.c_void_p,ct.c_size_t,ct.c_size_t,ct.c_void_p,ct.c_void_p]
        self.dll.replay_batch.restype=ct.c_int32
    def replay(self,texts,cap):
        arr=pa.array(texts,type=pa.large_string());_,offsets,raw=arr.buffers()
        rows=np.zeros((len(arr),cap),dtype=DTYPE);lengths=np.zeros(len(arr),dtype=np.int32)
        if self.dll.replay_batch(raw.address,raw.size,offsets.address,len(arr),cap,rows.ctypes.data,lengths.ctypes.data):raise ValueError('native input bounds rejected')
        return rows,lengths

def reference(text,cap):
    # A frozen row is one movetext: blank lines are layout, not new games.
    game=read_score_text('\n'.join(line for line in text.splitlines() if line.strip()))
    if game is None:return None
    moves=list(game.mainline_moves())
    if not moves or (game.errors and len(moves)<cap):return None
    b=game.board()
    if b.fen()!=chess.STARTING_FEN:return None
    rows=[]
    for m in moves[:cap]:
        if not m or m not in b.legal_moves:return None
        lo,hi=zobrist128(b);epd=b.epd(en_passant='xfen');san=b.san(m);uci=m.uci()
        b.push(m);clo,chi=zobrist128(b);rows.append((lo,hi,clo,chi,san,uci,epd))
    return rows

def compare_reference(native_row,length,text,cap):
    ref=reference(text,cap)
    if ref is None:raise ValueError('native accepted a prefix rejected by independent Python replay')
    actual=[tuple(int(r[x]) for x in ('parent_lo','parent_hi','child_lo','child_hi'))+tuple(r[x].decode('ascii') for x in ('san','uci','epd')) for r in native_row[:length]]
    if actual!=ref:raise ValueError(f'native/Python move or full-key mismatch: native {len(actual)}, python {len(ref)}; '+repr(text[:300]))
    return len(ref)

def replay_table(table,native,cap):
    cols=table.to_pydict();active=[i for i,r in enumerate(cols['role']) if r!='quarantine']
    lens=np.full(len(table),-10,dtype=np.int32);sample_games=sample_positions=0
    if active:
        raw,counts=native.replay([cols['movetext'][i] for i in active],cap);lens[active]=counts
        good=np.flatnonzero(counts>0)
        selected=set(good[[0,len(good)//2,-1]]) if len(good) else set()
        for ix in selected:
            sample_positions+=compare_reference(raw[ix],int(counts[ix]),cols['movetext'][active[ix]],cap);sample_games+=1
        gi,pj=np.nonzero(np.arange(cap)[None,:]<counts[:,None]);rows=raw[gi,pj];original=np.asarray(active,dtype=np.int64)[gi]
        white=np.asarray(cols['white_elo'],dtype=np.int32)[original];black=np.asarray(cols['black_elo'],dtype=np.int32)[original]
        elo=np.where(pj%2==0,white,black)
        data={'game_id':np.asarray(cols['game_id'],dtype=np.int64)[original],'role':np.asarray(cols['role'])[original],
            'ply':pj+1,'mover_elo':elo,'mover_band':np.where(elo<1500,0,np.where(elo<1800,1500,np.where(elo<2000,1800,2000)))}
        data.update({x:rows[x] for x in ('parent_lo','parent_hi','child_lo','child_hi')})
        data.update({n:pc.utf8_rtrim(pa.array(rows[x],type=pa.large_string()),characters='\x00') for n,x in [('move_san','san'),('move_uci','uci'),('parent_epd','epd')]})
        edges=pa.Table.from_pydict(data,schema=LEDGER_SCHEMA)
    else:edges=pa.Table.from_batches([],schema=LEDGER_SCHEMA)
    states=pa.Table.from_pydict(dict(game_id=cols['game_id'],role=cols['role'],replayed_plies=lens,
        status=['valid' if n>0 else ERRORS[int(n)] for n in lens]),schema=STATUS_SCHEMA)
    return edges,states,dict(sample_games=sample_games,sample_positions=sample_positions)

def entry(path,**extra):return dict(path=str(Path(path).resolve()),sha256=file_sha(path),**extra)
def atomic_parquet(table,path):
    tmp=path.with_name(path.name+'.tmp');pq.write_table(table,tmp,compression='zstd',row_group_size=65536);tmp.replace(path)
def reserve(root):
    if shutil.disk_usage(root).free<150_000_000_000:raise RuntimeError('150 GB disk reserve reached')
def connection(root):return duckdb.connect(config={'threads':2,'memory_limit':'8GB','temp_directory':str(root/'spill'),'preserve_insertion_order':False})
def status(root,stage,**kw):atomic_json(root/'status.json',dict(stage=stage,time=time.strftime('%Y-%m-%dT%H:%M:%S'),**kw))
def reject(c,sql,message):
    if c.execute(sql).fetchone()[0]:raise ValueError(message)

def replay_stage(root,source,dll,cap,identity):
    native=Native(dll);chunks=root/'chunks';chunks.mkdir(exist_ok=True);receipts=[];start=time.monotonic()
    with pq.ParquetFile(source) as f:
        for i,batch in enumerate(f.iter_batches(batch_size=4096,use_threads=False)):
            receipt=chunks/f'{i:04d}.json'
            if receipt.exists():
                saved=json.loads(receipt.read_text(encoding='utf8'))
                if saved['identity']!=identity:raise ValueError('stale replay chunk')
                artifacts(saved['outputs'])
            else:
                reserve(root);tab=pa.Table.from_batches([batch]);edges,states,checks=replay_table(tab,native,cap)
                out=chunks/f'{i:04d}.edges.parquet';sp=chunks/f'{i:04d}.games.parquet'
                atomic_parquet(edges,out);atomic_parquet(states,sp)
                saved=dict(identity=identity,chunk=i,games=len(tab),moves=len(edges),checks=checks,outputs=[entry(out,rows=len(edges)),entry(sp,rows=len(states))]);atomic_json(receipt,saved)
            receipts.append(entry(receipt));status(root,'replaying',chunks=i+1,games=min((i+1)*4096,f.metadata.num_rows),total_games=f.metadata.num_rows,seconds=round(time.monotonic()-start,1))
            if i%10==0:print(json.dumps(json.loads((root/'status.json').read_text())),flush=True)
    atomic_json(root/'replayed.json',dict(identity=identity,chunks=receipts,training_counts_ready=False))

def ledger_views(c,root,source):
    c.execute(f"CREATE VIEW ledger AS SELECT * FROM read_parquet({quote(root/'chunks/*.edges.parquet')})")
    c.execute(f"CREATE VIEW game_status AS SELECT * FROM read_parquet({quote(root/'chunks/*.games.parquet')})")
    c.execute(f"CREATE VIEW frozen AS SELECT * FROM read_parquet({quote(source)})")

def master_query(role):
    return f'''SELECT struct_pack(lo:=parent_lo,hi:=parent_hi) parent_hash,
        struct_pack(lo:=child_lo,hi:=child_hi) child_hash,move_san,move_uci,parent_epd,
        ply,mover_band,count(*)::BIGINT total FROM ledger WHERE role={quote(role)}
        AND mover_elo>0 AND mover_elo<2000 GROUP BY ALL'''

def pool_query(path,cap):
    return f'''SELECT parent_hash,child_hash,move_san,move_uci,parent_epd,min(ply)::SMALLINT ply,
        sum(total)::BIGINT total FROM read_parquet({quote(path)}) WHERE ply<={cap}
        GROUP BY parent_hash,child_hash,move_san,move_uci,parent_epd'''

def aggregate_stage(root,source,identity):
    outdir=root/'counts';outdir.mkdir(exist_ok=True)
    with connection(root) as c:
        ledger_views(c,root,source)
        for role in ROLES:
            master=outdir/f'{role}-by-ply-band.parquet'
            exports=[(master,master_query(role),MASTER_SCHEMA)]+[(outdir/f'{role}-p{cap}.parquet',pool_query(master,cap),POOL_SCHEMA) for cap in (30,50)]
            for path,query,schema in exports:
                receipt=path.with_suffix('.json')
                if receipt.exists():
                    old=json.loads(receipt.read_text())
                    if old['identity']!=identity:raise ValueError('stale aggregate')
                    artifacts([old['output']]);continue
                reserve(root);status(root,'aggregating',output=path.name);print('aggregate',path.name,flush=True)
                rows=write_sorted_export(c,query+' ORDER BY parent_hash,move_san,ply'+(',mover_band' if path==master else ''),path,schema)
                atomic_json(receipt,dict(identity=identity,output=entry(path,rows=rows),training_counts_ready=False));print(path.name,rows,flush=True)

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--split-verified',type=Path,required=True);p.add_argument('--dll',type=Path,required=True)
    p.add_argument('--out',type=Path,required=True);p.add_argument('--phase',choices=['replay','aggregate','all'],default='all');p.add_argument('--max-ply',type=int,default=51);a=p.parse_args()
    if a.max_ply!=51:raise ValueError('production replay retains exactly 51 plies')
    root=a.out.resolve();root.mkdir(parents=True,exist_ok=True);reserve(root)
    verified=json.loads(a.split_verified.read_text(encoding='utf8'))
    if verified.get('schema')!='otb-split-verification-v2' or not verified.get('split_verified'):raise ValueError('verified split required')
    artifacts([verified['split']]);split=json.loads(Path(verified['split']['path']).read_text(encoding='utf8'))
    if verified['outputs']!=split['outputs'] or verified['checks']['historical_role_crossings']!=0:raise ValueError('split acceptance mismatch')
    source=next(x['path'] for x in verified['outputs'] if Path(x['path']).name=='games.parquet');artifacts(verified['outputs'])
    code=Path(__file__).resolve();base=code.parents[2]
    paths=[a.split_verified,Path(verified['split']['path']),Path(source),a.dll,code,base/'python/zobrist128.py',base/'python/sweep/verify_otb_split.py',base/'python/pool_from_book.py']
    for rel in ['otb_counts_replay/Cargo.toml','otb_counts_replay/Cargo.lock','otb_counts_replay/src/lib.rs','explorer_extract/src/san.rs','explorer_extract/src/pyre.rs','explorer_extract/src/pydigits.rs']:paths.append(base/'rust'/rel)
    recipe=dict(schema='otb-counts-v1',inputs=[entry(x) for x in paths],key_bits=128,max_ply=a.max_ply,mover_filter='positive rating below 2000',min_games=1,roles=list(ROLES),batch_games=4096,malformed_prefix='exclude entire game prefix; retain status',quarantine='exclude',pooled_caps=[30,50],rating_bands=[0,1500,1800],python_chess=chess.__version__)
    identity=digest(recipe);rp=root/'recipe.json'
    if rp.exists() and json.loads(rp.read_text())['identity']!=identity:raise ValueError('changed input/code: choose another output directory')
    if not rp.exists():atomic_json(rp,dict(identity=identity,recipe=recipe))
    logging.getLogger('chess.pgn').setLevel(logging.CRITICAL)
    if os.name=='nt':
        kernel=ct.WinDLL('kernel32',use_last_error=True);kernel.GetCurrentProcess.restype=ct.c_void_p
        kernel.SetPriorityClass.argtypes=[ct.c_void_p,ct.c_uint32];kernel.SetPriorityClass(kernel.GetCurrentProcess(),0x4000)
    try:
        if a.phase in ('replay','all'):replay_stage(root,source,a.dll,a.max_ply,identity)
        if a.phase in ('aggregate','all'):
            replayed=json.loads((root/'replayed.json').read_text())
            if replayed['identity']!=identity:raise ValueError('replay identity mismatch')
            artifacts(replayed['chunks'])
            for item in replayed['chunks']:artifacts(json.loads(Path(item['path']).read_text())['outputs'])
            aggregate_stage(root,source,identity)
        artifacts(recipe['inputs']);status(root,'awaiting-independent-count-verification',training_counts_ready=False)
    except BaseException as exc:status(root,'failed',error=str(exc));raise

if __name__=='__main__':main()
