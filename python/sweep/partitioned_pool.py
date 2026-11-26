"""Resumable per-bucket finalization; complete publication never needs a global sort."""
import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import time

import duckdb
import numpy as np
import pyarrow.parquet as pq
import pool_from_book as pool
from .spec import digest
from .stages import artifacts, atomic_json, file_sha

SCHEMA = 'partitioned-outcome-pool-128-v1'
GATES = {'term_vs_book', 'reimpl', 'conservation', 'evals'}


@contextmanager
def locked(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a+b') as stream:
        if not stream.tell():
            stream.write(b'0'); stream.flush()
        stream.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def checked_params(item):
    artifacts([item])
    params = json.loads(Path(item['path']).read_text(encoding='utf8'))
    if params.get('key_bits') != 128 or params.get('min_games') != 1 or not 1 <= params.get('max_ply', 0) <= 50:
        raise ValueError('requires an uncut full-key pool recipe')
    for k in ('book_meta_sha256', 'eval_lookup_sha256', 'eval_lookup128', 'events', 'elo_bands'):
        if not params.get(k):
            raise ValueError(f'missing pool recipe {k}')
    return params


def audit(path, bucket, cap, role):
    """Arrow batches feed Rust validation; ordered parent digests pair edge/aux sets."""
    from book_builder_rs import RsPoolAudit
    aux = role == 'aux'
    columns = pool.AUX_SCHEMA if aux else pool.POOL_SCHEMA
    pool.check_export(path, pool.output_schema(columns, True), None)
    key = 'position_hash' if aux else 'parent_hash'
    groups = ['term_normal', 'term_flag', 'term_other', 'horizon', 'other']
    counts = [f'{g}_{s}' for g in groups for s in ('white_wins', 'draws', 'black_wins', 'total')] if aux else list(pool.SUMS)
    native = RsPoolAudit(bucket, cap, aux)
    with pq.ParquetFile(path) as reader:
        for b in reader.iter_batches(batch_size=65536, use_threads=False):
            def col(n):
                a = b.column(b.schema.get_field_index(n))
                if a.null_count:
                    raise ValueError(f'null required pool column {n}')
                return a
            k = col(key)
            if k.field('lo').null_count or k.field('hi').null_count:
                raise ValueError('null key half')
            if not aux:
                c = col('child_hash')
                if c.field('lo').null_count or c.field('hi').null_count:
                    raise ValueError('null child half')
                score = col('white_score_avg').to_numpy()
                expected = (col('white_wins').to_numpy()+.5*col('draws').to_numpy())/col('total').to_numpy()
                if not np.array_equal(score, expected) or any(v != 'Pooled' for v in col('event').unique().to_pylist()) or np.any(col('elo_band').to_numpy()!=0):
                    raise ValueError('outcome projection differs')
            elif np.any(col('other_edges').to_numpy()!=0) or np.any(col('other_eval_cov').to_numpy()!=0):
                raise ValueError('uncut pool has Other evidence')
            native.batch(k.field('lo').to_numpy(), k.field('hi').to_numpy(),
                         ['']*len(b) if aux else col('move_san').to_pylist(),
                         ['']*len(b) if aux else col('parent_epd').to_pylist(),
                         np.zeros(len(b), np.int64) if aux else col('ply').to_numpy().astype(np.int64),
                         np.column_stack([col(c).to_numpy() for c in counts]))
    rows, parents, totals, keys = native.result()
    return dict(rows=rows, parents=parents, totals=dict(zip(counts, totals)), parent_keys_sha256=keys)


def prepare_bucket(config, bucket):
    params = checked_params(config['params'])
    source = config['sources'][str(bucket)]
    if not 0 <= bucket < 512 or not source.get('acceptance'):
        raise ValueError('bucket/source acceptance evidence required')
    inputs = [source[k] for k in ('edges', 'aux', 'stats')] + source['acceptance'] + [config['params']]
    artifacts(inputs)
    stats = json.loads(Path(source['stats']['path']).read_text())
    if stats['bucket'] != bucket or any(stats.get(k, 0) for k in ('coll_edges', 'coll_parents', 'other_edges', 'other_mass', 'aux_other_mass')):
        raise ValueError('bucket stats are not collision-free/uncut')
    out = Path(config['output']).resolve(); folder = out/'buckets'
    folder.mkdir(parents=True, exist_ok=True)
    implementation = {p.name:file_sha(p) for p in [Path(__file__), Path(pool.__file__), Path(pool.__file__).with_name('book_builder_rs.pyd')]}
    identity = digest(dict(params=config['params'], bucket=bucket, inputs=inputs, implementation=implementation))
    done = folder/f'b{bucket:03d}.json'
    with locked(folder/f'b{bucket:03d}.lock'):
        if done.exists():
            receipt = json.loads(done.read_text())
            if receipt['identity'] != identity:
                raise ValueError('bucket resume identity differs')
            artifacts([dict(path=str(out/f['file']), sha256=f['sha256']) for f in receipt['files'].values()])
            return receipt
        # Admission reserve covers a sorted copy plus bounded spill. Rechecked per role.
        tmp = Path(config['tmp']).resolve()
        if tmp.drive.upper() == 'F:':
            raise ValueError('pool spill on F is forbidden')
        tmp.mkdir(parents=True, exist_ok=True)
        reserve = int(config.get('reserve_bytes', 250 << 30))
        source_bytes = sum(Path(source[k]['path']).stat().st_size for k in ('edges', 'aux'))
        needed = 2*source_bytes+(16 << 30)
        for p in (out, tmp):
            if shutil.disk_usage(p).free < reserve+needed:
                raise ValueError('publication would breach its storage reserve')
        metadata = dict(pool_recipe=config['params']['sha256'], publication_intent=identity)
        intent = folder/f'b{bucket:03d}.intent.json'
        if intent.exists() and json.loads(intent.read_text()) != metadata:
            raise ValueError('unreceipted bucket belongs to another intent')
        atomic_json(intent, metadata)
        t = time.perf_counter(); files = {}
        for role, schema, order in [('edges', pool.POOL_SCHEMA, 'parent_hash, move_san'), ('aux', pool.AUX_SCHEMA, 'position_hash')]:
            for p in (out, tmp):
                if shutil.disk_usage(p).free < reserve+needed:
                    raise ValueError('publication storage reserve changed')
            dest = folder/f'b{bucket:03d}.{role}.parquet'
            if dest.exists():
                actual = pq.read_metadata(dest).metadata or {}
                if any(actual.get(k.encode()) != v.encode() for k,v in metadata.items()):
                    raise ValueError('unreceipted sorted output identity differs')
            else:
                with pool._duck(config.get('threads', 2), config.get('memory', '4GB'), tmp/f'b{bucket:03d}-{role}') as db:
                    db.execute("SET max_temp_directory_size='16GB'")
                    src = "read_parquet('"+str(Path(source[role]['path']).resolve()).replace('\\','/').replace("'","''")+"')"
                    projection = ', '.join(n for n,_ in schema)
                    if role == 'edges':
                        projection = ', '.join(n for n,_ in pool.POOL_SCHEMA[:-3])+", 'Pooled' AS event, 0::BIGINT AS elo_band, (white_wins::DOUBLE+0.5::DOUBLE*draws::DOUBLE)/total::DOUBLE AS white_score_avg"
                    pool.write_sorted_export(db, f'SELECT {projection} FROM {src} ORDER BY {order}', dest,
                                             pool.output_schema(schema, True), metadata, unique='position_hash' if role=='aux' else None)
            checked = audit(dest, bucket, params['max_ply'], role)
            files[role] = dict(file=dest.relative_to(out).as_posix(), bytes=dest.stat().st_size, sha256=file_sha(dest), audit=checked)
        e, a = [files[k]['audit'] for k in ('edges','aux')]
        if (e['parent_keys_sha256'] != a['parent_keys_sha256'] or e['parents'] != a['rows']
            or e['rows'] != stats['survivors'] or a['rows'] != stats['aux_rows']
            or e['totals']['total'] != stats['survivor_mass']
            or sum(a['totals'][f'term_{g}_total'] for g in ('normal','flag','other')) != stats['aux_term_mass']
            or a['totals']['horizon_total'] != stats['aux_horizon_mass']):
            raise ValueError('sorted edge/aux sets or flow totals differ from source stats')
        artifacts(inputs)
        receipt = dict(schema=SCHEMA, bucket=bucket, identity=identity, params_sha256=config['params']['sha256'],
                       inputs=inputs, implementation=implementation, files=files, seconds=time.perf_counter()-t)
        atomic_json(done, receipt)
        return receipt


def inventory(out, params_sha):
    out = Path(out); entries = []
    for b in range(512):
        p = out/'buckets'/f'b{b:03d}.json'
        r = json.loads(p.read_text())
        if r.get('schema') != SCHEMA or r['bucket'] != b or r['params_sha256'] != params_sha:
            raise ValueError('mixed bucket recipe')
        for role in ('edges','aux'):
            if r['files'][role]['file'] != f'buckets/b{b:03d}.{role}.parquet':
                raise ValueError('bucket path differs')
        entries.append(dict(bucket=b, receipt_sha256=file_sha(p), **r['files']))
    return entries


def publish(config, acceptance):
    params = checked_params(config['params']); out = Path(config['output']).resolve()
    if config.get('term_reasons') not in ('split', 'pooled'):
        raise ValueError('explicit termination classification required')
    with locked(out/'publication.lock'):
        entries = inventory(out, config['params']['sha256'])
        proof_sha = file_sha(acceptance)
        proof = json.loads(Path(acceptance).read_text())
        if (proof.get('schema') != 'partitioned-pool-acceptance-v1' or proof.get('params_sha256') != config['params']['sha256']
            or proof.get('inventory_sha256') != digest(entries) or set(proof.get('checks', {})) != GATES
            or any(v is not True for v in proof['checks'].values()) or set(proof.get('evidence', {})) != GATES):
            raise ValueError('independent acceptance does not bind this pool inventory')
        for items in proof['evidence'].values():
            if not items:raise ValueError('missing independent gate artifacts')
            artifacts(items)
        # Publication performs a complete byte check; readers subsequently verify opened files.
        for entry in entries:
            artifacts([dict(path=str(out/entry[r]['file']), sha256=entry[r]['sha256']) for r in ('edges','aux')])
        artifacts([config['params'],dict(path=str(acceptance),sha256=proof_sha)])
        pool_id = digest(dict(params_sha256=config['params']['sha256'], inventory=entries, acceptance=proof_sha))
        metadata = dict(key_bits=128, min_games=1, max_ply=params['max_ply'],
            population=dict(events=params['events'], elo_bands=params['elo_bands']),
            term_reasons=config['term_reasons'], event='Pooled', elo_band=0,
            eval_lookup128=params['eval_lookup128'], eval_lookup_sha256=params['eval_lookup_sha256'],
            book_meta_sha256=params['book_meta_sha256'], params=params, pool_id=pool_id)
        publication = dict(schema=SCHEMA, complete=True, pool_id=pool_id, acceptance=dict(path=str(Path(acceptance).resolve()),sha256=proof_sha))
        manifests = {}
        for role in ('edges','aux'):
            manifests[role] = dict(schema=SCHEMA, complete=True, buckets=512, role=role, pool_id=pool_id,
                            metadata=metadata, files=[dict(bucket=x['bucket'], **x[role]) for x in entries])
        if (out/'publication.json').exists():
            saved = json.loads((out/'publication.json').read_text())
            if any(saved.get(k) != v for k,v in publication.items()):
                raise ValueError('published generation is immutable; use a new output directory')
            for role, manifest in manifests.items():
                path = out/f'{role}.json'; sidecar = Path(str(path)+'.meta.json')
                if (json.loads(path.read_text()) != manifest or json.loads(sidecar.read_text()) != dict(metadata,role=role)
                    or saved[role] != dict(file=path.name,sha256=file_sha(path),metadata_sha256=file_sha(sidecar))):
                    raise ValueError('published generation changed')
            return saved
        for role, manifest in manifests.items():
            path = out/f'{role}.json'
            atomic_json(path, manifest)
            atomic_json(str(path)+'.meta.json', dict(metadata, role=role))
            publication[role] = dict(file=path.name, sha256=file_sha(path), metadata_sha256=file_sha(str(path)+'.meta.json'))
        atomic_json(out/'publication.json', publication)
        return publication


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config',type=Path,required=True)
    p.add_argument('--bucket',type=int)
    p.add_argument('--acceptance',type=Path)
    a=p.parse_args(); cfg=json.loads(a.config.read_text())
    if (a.bucket is None)==(a.acceptance is None):p.error('choose one bucket or final acceptance')
    result=prepare_bucket(cfg,a.bucket) if a.bucket is not None else publish(cfg,a.acceptance)
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
