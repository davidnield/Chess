"""Publication and raw-prediction failure/restart tests, including native consumers."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import book_builder_rs as rs
import pool_from_book as pool
from _test_sweep_execution import board, edge, keys
from sweep.stages import atomic_json, file_sha
from sweep.spec import digest
from sweep import partitioned_pool as pub
from sweep.keyed_parquet import KeyedParquet
from sweep.prediction_cache import PredictionCache


def pinned(path):return dict(path=str(path),sha256=file_sha(path))


def outcome_rows(rows):
    return [dict(r,ply=1,event='Pooled',elo_band=0) for r in rows]


def write_rows(path,role,rows,metadata=None):
    schema=pool._arrow_schema(pool.output_schema(pool.POOL_SCHEMA if role=='edges' else pool.AUX_SCHEMA,True),metadata)
    pq.write_table(pa.Table.from_pylist(rows,schema=schema),path,compression='zstd',row_group_size=2)


def aux_rows(rows):
    zero={c:0 for c,t in pool.AUX_SCHEMA if c!='position_hash'}
    for c in ('other_eval_mean','other_eval_min','other_eval_max'):zero[c]=None
    return [dict(zero,position_hash=dict(lo=lo,hi=hi)) for lo,hi in sorted({tuple(r['parent_hash'][c] for c in ('lo','hi')) for r in rows})]


def params(root,extra=None):
    p=root/'params.json'
    atomic_json(p,dict(key_bits=128,min_games=1,max_ply=2,book_meta_sha256='fixture-book',
        eval_lookup_sha256='fixture-evals',eval_lookup128='fixture-lookup',
        events=['Blitz','Rapid','Classical'],elo_bands=[1800,2000,2200,2500],**(extra or {})))
    return pinned(p)


def published_fixture(root,rows,metadata=None):
    """All 512 pairs, including empty partitions; only synthetic test gate evidence."""
    root.mkdir(parents=True,exist_ok=True);(root/'buckets').mkdir()
    recipe=params(root)
    if metadata:
        v=json.loads(Path(recipe['path']).read_text());v.update(metadata)
        atomic_json(recipe['path'],v);recipe=pinned(recipe['path'])
    rows=outcome_rows(rows)
    for b in range(512):
        subset=sorted([r for r in rows if r['parent_hash']['lo']%512==b],key=lambda r:(r['parent_hash']['lo'],r['parent_hash']['hi'],r['move_san']))
        files={}
        for role,data in [('edges',subset),('aux',aux_rows(subset))]:
            p=root/'buckets'/f'b{b:03d}.{role}.parquet';write_rows(p,role,data)
            files[role]=dict(file=p.relative_to(root).as_posix(),bytes=p.stat().st_size,sha256=file_sha(p),audit=pub.audit(p,b,2,role))
        atomic_json(root/'buckets'/f'b{b:03d}.json',dict(schema=pub.SCHEMA,bucket=b,params_sha256=recipe['sha256'],files=files))
    evidence=root/'fixture-gates.json';atomic_json(evidence,dict(synthetic_fixture=True))
    proof=root/'acceptance.json'
    atomic_json(proof,dict(schema='partitioned-pool-acceptance-v1',params_sha256=recipe['sha256'],
        inventory_sha256=digest(pub.inventory(root,recipe['sha256'])),checks={g:True for g in pub.GATES},
        evidence={g:[pinned(evidence)] for g in pub.GATES}))
    cfg=dict(params=recipe,output=str(root),term_reasons='split')
    pub.publish(cfg,proof)
    return root/'edges.json',root/'aux.json',cfg,proof


class Publication(unittest.TestCase):
    def test_sort_restart_and_change_refusal(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);rows=outcome_rows([edge('','e4',3),edge('','d4',5)])
            b=rows[0]['parent_hash']['lo']%512
            for role,data in [('edges',rows),('aux',aux_rows(rows))]:write_rows(root/f'{role}.parquet',role,data)
            stats=root/'stats.json';atomic_json(stats,dict(bucket=b,survivors=2,aux_rows=1,survivor_mass=8,aux_term_mass=0,aux_horizon_mass=0))
            proof=root/'proof';proof.write_text('fixture source parity')
            cfg=dict(params=params(root),sources={str(b):dict(edges=pinned(root/'edges.parquet'),aux=pinned(root/'aux.parquet'),stats=pinned(stats),acceptance=[pinned(proof)])},
                output=str(root/'out'),tmp=str(root/'spill'),reserve_bytes=0,threads=1,memory='128MB')
            audit=pub.audit
            def interrupt(path,bucket,cap,role):
                if role=='aux':raise RuntimeError('interrupted')
                return audit(path,bucket,cap,role)
            with patch.object(pub,'audit',side_effect=interrupt),self.assertRaisesRegex(RuntimeError,'interrupted'):
                pub.prepare_bucket(cfg,b)
            final=root/'out'/'buckets'/f'b{b:03d}.edges.parquet';stamp=final.stat().st_mtime_ns
            r=pub.prepare_bucket(cfg,b)
            self.assertEqual(final.stat().st_mtime_ns,stamp)
            self.assertEqual(pub.prepare_bucket(cfg,b),r)
            self.assertEqual(pl.read_parquet(final)['move_san'].to_list(),['d4','e4'])
            with self.assertRaises(FileNotFoundError):pub.inventory(root/'out',cfg['params']['sha256'])
            proof.write_text('changed acceptance')
            with self.assertRaisesRegex(ValueError,'artifact changed'):pub.prepare_bucket(cfg,b)

    def test_native_audit_rejects_cross_batch_duplicate_wrong_bucket_and_mass(self):
        lo=np.array([5],np.int64);hi=np.array([1],np.int64);ply=np.array([1],np.int64)
        def send(a,counts):a.batch(lo,hi,['e4'],['epd'],ply,np.array([counts],np.int64))
        a=rs.RsPoolAudit(5,2,False);send(a,[3,1,2,6])
        with self.assertRaisesRegex(ValueError,'duplicate'):send(a,[3,1,2,6])
        with self.assertRaisesRegex(ValueError,'bucket'):send(rs.RsPoolAudit(6,2,False),[3,1,2,6])
        with self.assertRaisesRegex(ValueError,'mass'):send(rs.RsPoolAudit(5,2,False),[3,1,2,9])

    def test_pair_immutability_full_keys_and_native_graph_parity(self):
        from build_budget_books import collect_subgraph_edges_rs
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);rows=[edge('','e4',10),edge('','d4',10),edge('e4','e5',6),edge('d4','d5',6)]
            twin=copy.deepcopy(rows[0]);twin['parent_hash']['hi']+=1;twin['total']=99;twin['white_wins']=99
            stats,aux,cfg,proof=published_fixture(root/'pub',rows+[twin])
            before=(stats.stat().st_mtime_ns,file_sha(stats));pub.publish(cfg,proof)
            self.assertEqual(before,(stats.stat().st_mtime_ns,file_sha(stats)))
            with self.assertRaisesRegex(ValueError,'changed'):pub.publish(cfg|dict(term_reasons='pooled'),proof)
            direct=root/'direct.parquet';write_rows(direct,'edges',outcome_rows(rows+[twin]))
            reader=KeyedParquet(stats,'parent_hash')
            with duckdb.connect(config={'threads':1}) as db:
                k=rows[0]['parent_hash'];found=reader.fetch(db,[k])
                self.assertEqual(sorted(found['total'].to_list()),[10,10])
                self.assertEqual(reader.fetch(db,[]).schema,found.schema)
                for white in (True,False):
                    key=keys();h=key.board(board())
                    a=collect_subgraph_edges_rs(db,str(stats),h,white,.00001,2,0.,1,keys=key)
                    b=collect_subgraph_edges_rs(db,str(direct),h,white,.00001,2,0.,1,keys=key)
                    self.assertTrue(a.sort(['parent_hash','move_san']).equals(b.sort(['parent_hash','move_san'])))
                target=root/'pub'/'buckets'/f'b{k["lo"]%512:03d}.edges.parquet'
                data=target.read_bytes();target.write_bytes(data[:-1]+bytes([data[-1]^1]))
                with self.assertRaisesRegex(ValueError,'hash differs'):reader.fetch(db,[k])
            reader.close()
            missing=root/'pub'/'buckets'/'b511.aux.parquet';missing.unlink()
            with self.assertRaisesRegex(ValueError,'missing'):KeyedParquet(aux,'position_hash')
            with self.assertRaisesRegex(ValueError,'missing'):KeyedParquet(stats,'parent_hash')


def uniform(boards):return [{m.uci():1/b.legal_moves.count() for m in b.legal_moves} for b in boards]


class RawCache(unittest.TestCase):
    def test_restart_deduplication_invalidation_and_corruption(self):
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/'raw.sqlite';identity=dict(ratings=[1450,2200],checkpoint='a',runtime='a')
            cache=PredictionCache(path,identity);calls=[]
            def infer(bs):calls.extend(bs);return uniform(bs)
            p=cache.predict([board(),board(),board('e4')],infer)
            self.assertEqual(len(calls),2);self.assertEqual(p[0],p[1]);cache.close()
            cache=PredictionCache(path,identity)
            self.assertEqual(cache.predict([board()],lambda bs:self.fail('cache miss')),p[:1])
            for changed in (identity|dict(ratings=[2600,2600]),identity|dict(checkpoint='b'),identity|dict(runtime='b')):
                c=PredictionCache(path,changed);self.assertNotEqual(cache.namespace,c.namespace)
                c.predict([board()],infer);c.close()
            request=cache.descriptor(board());key=digest(request)
            with cache.db:cache.db.execute('UPDATE raw_predictions SET value=? WHERE id=?',('[1]',key))
            with self.assertRaisesRegex(ValueError,'content hash'):cache.predict([board()],infer)
            cache.close()

    def test_completed_batches_survive_inference_failure_and_bad_vectors(self):
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/'raw.sqlite';cache=PredictionCache(path,{},batch_size=1)
            def interrupted(bs):
                if bs[0].turn is False:raise RuntimeError('worker stopped')
                return uniform(bs)
            with self.assertRaisesRegex(RuntimeError,'worker stopped'):cache.predict([board(),board('e4')],interrupted)
            cache.close();cache=PredictionCache(path,{},batch_size=1)
            self.assertEqual(cache.predict([board()],lambda bs:self.fail('lost transaction')),uniform([board()]))
            for invalid in ({}, {'e7e5':1.}, {m.uci():float('nan') for m in board('e4').legal_moves}):
                with self.assertRaises(ValueError):cache.predict([board('e4')],lambda bs:[invalid])
            self.assertEqual(cache.db.execute('SELECT COUNT(*) FROM raw_predictions').fetchone()[0],1)
            cache.close()

    def test_exact_fen_separates_forced_hash_aliases_and_concurrent_winner(self):
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/'raw.sqlite';a=PredictionCache(path,{});b=PredictionCache(path,{})
            with patch('sweep.prediction_cache.stored_key',return_value=dict(lo=1,hi=2)):
                a.predict([board(),board('e4')],uniform)
                self.assertEqual(a.db.execute('SELECT COUNT(*) FROM raw_predictions').fetchone()[0],2)
            first=uniform([board()])[0];first={u:float(u=='e2e4') for u in first}
            def race(bs):
                b.predict(bs,lambda xs:[first]);return uniform(bs)
            self.assertEqual(a.predict([board()],race),[first]);a.close();b.close()

    def test_maia_temperature_reuses_raw_without_worker(self):
        from sweep.maia_policy import MaiaPolicy
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);checkpoint=root/'model';checkpoint.write_bytes(b'fixture')
            cfg=dict(python=sys.executable,checkpoint=str(checkpoint),raw_cache=str(root/'raw.sqlite'))
            recipe=dict(ratings=[1450,2200],temperature=1.)
            a=MaiaPolicy(cfg,recipe,runtime=dict(fixture=True))
            raw={u:float(u=='e2e4')*.5+.5/20 for u in uniform([board()])[0]}
            with patch.object(a,'infer_raw',return_value=[raw]) as infer:
                first=a([board()]);infer.assert_called_once()
            a.close();b=MaiaPolicy(cfg,recipe|dict(temperature=2.),runtime=dict(fixture=True))
            with patch.object(b,'start',side_effect=AssertionError('warm model loaded')):
                second=b([board()]);self.assertEqual(b([]),[])
            self.assertGreater(first[0]['e2e4'],second[0]['e2e4']);self.assertEqual(b.raw_cache.stats['hits'],1);b.close()


if __name__=='__main__':unittest.main()
