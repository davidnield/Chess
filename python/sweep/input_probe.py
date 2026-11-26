"""Bounded real-input checks; no complete-pool acceptance or production activation."""
import argparse
from contextlib import contextmanager
import json
from pathlib import Path
import threading
import time
import duckdb
import numpy as np
import polars as pl
import psutil
import pyarrow.parquet as pq
from consumer_keys import checked_board, stored_key, PAIR
from .stages import atomic_json, file_sha
from .partitioned_pool import prepare_bucket
from .keyed_parquet import KeyedParquet


@contextmanager
def memory():
    stop=threading.Event();peak=dict(sampled_peak_tree_rss_bytes=0,sampled_peak_tree_private_bytes=0)
    def sample():
        while not stop.is_set():
            processes=[psutil.Process()]
            try:processes+=processes[0].children(recursive=True)
            except psutil.Error:pass
            rss=private=0
            for p in processes:
                try:
                    m=p.memory_info();rss+=m.rss;private+=getattr(m,'private',m.rss)
                except psutil.Error:pass
            peak['sampled_peak_tree_rss_bytes']=max(peak['sampled_peak_tree_rss_bytes'],rss)
            peak['sampled_peak_tree_private_bytes']=max(peak['sampled_peak_tree_private_bytes'],private)
            stop.wait(.25)
    thread=threading.Thread(target=sample,daemon=True);thread.start()
    try:yield peak
    finally:stop.set();thread.join()


def bucket_probe(cfg,bucket):
    t=time.perf_counter();receipt=prepare_bucket(cfg,bucket);prepare_seconds=time.perf_counter()-t
    root=Path(cfg['output']);source=cfg['sources'][str(bucket)]
    sorted_path=root/receipt['files']['edges']['file']
    # Sixteen spread groups, including low-frequency positions, not just the opening root.
    with pq.ParquetFile(sorted_path) as f:
        groups=sorted(set(np.linspace(0,f.num_row_groups-1,16,dtype=int).tolist()))
        rows=[f.read_row_group(g,use_threads=False).slice(0,1).to_pylist()[0] for g in groups]
    positions=[r['parent_hash'] for r in rows]
    boards=[checked_board(r['parent_epd'],r['parent_hash']) for r in rows]
    for r,b in zip(rows,boards):
        b.push_san(r['move_san'])
        if stored_key(b,True)!=r['child_hash']:raise ValueError('sample child differs from legal replay')
    queries={}
    with duckdb.connect(config={'threads':2,'memory_limit':'2GB'}) as db:
        for role,column in [('edges','parent_hash'),('aux','position_hash')]:
            readers=[KeyedParquet(source[role]['path'],column),KeyedParquet(root/receipt['files'][role]['file'],column)]
            result=[];measurements=[]
            try:
                for reader in readers:
                    t=time.perf_counter();frame=reader.fetch(db,positions);elapsed=time.perf_counter()-t
                    order=[column]+(['move_san'] if role=='edges' else [])
                    result.append(frame.sort(order))
                    measurements.append(dict(seconds=elapsed,rows=len(frame),**reader.stats))
                # The final edge file adds only derived score and pooled labels.
                if not result[0].equals(result[1].select(result[0].columns)):
                    raise ValueError(f'{role} raw versus sorted lookup differs')
                queries[role]=dict(raw=measurements[0],sorted=measurements[1],exact_equal=True)
            finally:
                for reader in readers:reader.close()
    t=time.perf_counter();again=prepare_bucket(cfg,bucket);resume=time.perf_counter()-t
    if again!=receipt:raise ValueError('bucket resume changed receipt')
    return dict(scope='one-bucket diagnostic; not a published pool',bucket=bucket,prepare_seconds=prepare_seconds,
        resume_seconds=resume,receipt=receipt,queries=queries,sample_legal_children=len(rows),
        raw_bytes=sum(Path(source[k]['path']).stat().st_size for k in ('edges','aux')),
        sorted_bytes=sum(f['bytes'] for f in receipt['files'].values()))


def model_probe(cfg):
    import chess
    from .maia_policy import MaiaPolicy,runtime_identity
    runtime=runtime_identity(cfg['python']);boards=[];b=chess.Board();rng=np.random.default_rng(251125)
    for _ in range(64):
        if b.is_game_over() or b.ply()>=32:b=chess.Board()
        boards.append(b.copy(stack=False));moves=list(b.legal_moves);b.push(moves[int(rng.integers(len(moves)))])
    measured=[]
    for ratings in ([1450,2200],[2600,2600]):
        cold=MaiaPolicy(cfg,dict(ratings=ratings,temperature=1.),runtime=runtime)
        try:
            t=time.perf_counter();first=cold(boards);seconds=time.perf_counter()-t;stats=cold.cache_metrics()
            if not cold.process:raise ValueError('cold benchmark unexpectedly used an existing cache')
        finally:cold.close()
        warm=MaiaPolicy(cfg,dict(ratings=ratings,temperature=1.35),runtime=runtime)
        try:
            t=time.perf_counter();second=warm(boards);elapsed=time.perf_counter()-t
            if warm.process is not None or warm.cache_metrics()['unique_inferred']!=0:
                raise ValueError('warm transform invoked inference')
            for a,z in zip(first,second):
                # Independent formula on the same raw normalized values.
                expected={u:p**(1/1.35) for u,p in a.items()};mass=sum(expected.values())
                if max(abs(z[u]-v/mass) for u,v in expected.items())>1e-12:raise ValueError('temperature differs')
            measured.append(dict(ratings=ratings,cold_seconds=seconds,warm_seconds=elapsed,cold_stats=stats,
                warm_stats=warm.cache_metrics(),warm_model_started=False,temperature_formula_equal=True))
        finally:warm.close()
    return dict(scope='engineering cache probe, not calibration',boards=len(boards),runtime=runtime,
        checkpoint_sha256=file_sha(cfg['checkpoint']),cache_bytes=Path(cfg['raw_cache']).stat().st_size,measurements=measured)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,required=True)
    p.add_argument('--bucket',type=int);p.add_argument('--result',type=Path,required=True);a=p.parse_args()
    cfg=json.loads(a.config.read_text());started=time.perf_counter()
    with memory() as peak:
        result=bucket_probe(cfg,a.bucket) if a.bucket is not None else model_probe(cfg)
    result.update(peak);result.update(seconds=time.perf_counter()-started,config_sha256=file_sha(a.config))
    atomic_json(a.result,result);print(json.dumps({k:v for k,v in result.items() if k not in ('runtime','receipt')},indent=2))


if __name__=='__main__':main()
