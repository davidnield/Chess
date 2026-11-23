"""Source-independent archive verifier; optional sequential HDD staging / parallel audits.

--workers 1 preserves the serial path. With --deep --workers N --scratch DIR,
archive bytes are read once, sequentially and unbuffered, into bounded NVMe staging.
Native auditors run concurrently on the copies and must match BOTH logical digests
and the physical checksum. No verification check is skipped. Failed staging is kept.
"""
import argparse, concurrent.futures as cf, json, math, shutil, time, uuid
from pathlib import Path
from month_archive import read_json, sha, write_json, file_lock, disjoint
from chessdb_archive import EXPECTED, CHECK_FIELDS, verify_chain, validate_inventory, run_json, HERE
from chessdb_archive_windows import unbuffered_sha


def audit_file(path, receipt, executable):
    audit = run_json([str(executable), 'audit', str(path)])
    if any(audit[k] != receipt['source'][k] for k in CHECK_FIELDS):
        raise ValueError('deep logical audit mismatch')
    if audit['bytes'] != receipt['audit']['bytes'] or audit['file_sha256'] != receipt['audit']['file_sha256']:
        raise ValueError('deep physical audit mismatch')
    return audit


def audit_entries(root, entries, *, deep, executable, workers=1, scratch=None,
                  reserve_gb=250, staging_gb=24, progress=None):
    """Entries have already passed the complete archive's structural checks.

    At most workers copies exist in this invocation. All archive reads occur in
    this caller; audit threads only read the SSD. Scratch lock is held by the CLI.
    """
    if not 1 <= workers <= 8 or any(not math.isfinite(v) or v <= 0 for v in (reserve_gb, staging_gb)):
        raise ValueError('invalid verification limits')
    if workers > 1 and (not deep or scratch is None):
        raise ValueError('parallel verification needs --deep and --scratch')
    count = 0
    def done():
        nonlocal count
        count += 1
        if progress:
            write_json(progress, dict(state='verifying', completed_shards=count, total_shards=len(entries)))
    if workers == 1:
        for r in entries:
            f = root / r['file']
            if f.stat().st_size != r['audit']['bytes'] or unbuffered_sha(f) != r['audit']['file_sha256']:
                raise ValueError('archive bytes changed')
            if deep:
                audit_file(f, r, executable)
            done()
        return
    scratch = Path(scratch).resolve()
    disjoint(root, scratch, executable.parent)
    scratch.mkdir(parents=True, exist_ok=True)
    run = scratch / ('audit-' + uuid.uuid4().hex)
    run.mkdir()
    pending = {}
    held = 0
    def reap():
        nonlocal held
        finished, _ = cf.wait(pending, return_when=cf.FIRST_COMPLETED)
        for fut in finished:
            path, size = pending.pop(fut)
            fut.result()  # A failed copy/audit is never deleted or counted as success.
            if path.parent != run or not path.name.endswith('.parquet') or path.is_symlink():
                raise ValueError('unsafe audit staging cleanup')
            path.unlink()
            held -= size
            done()
    with cf.ThreadPoolExecutor(max_workers=workers) as executor:
        for i, r in enumerate(entries):
            size = r['audit']['bytes']
            if size > staging_gb * 1e9:
                raise ValueError('single shard exceeds staging cap')
            while pending and (len(pending) >= workers or held + size > staging_gb * 1e9):
                reap()
            if shutil.disk_usage(scratch).free < reserve_gb * 1e9 + size:
                raise ValueError('verification staging reserve reached')
            # Also account for retained files from interrupted/failed invocations.
            existing = sum(p.stat().st_size for p in scratch.glob('audit-*/*.parquet'))
            if existing + size > staging_gb * 1e9:
                raise ValueError('verification staging cap reached; inspect retained files')
            source = root / r['file']
            path = run / f'{i:06d}.parquet'
            if source.stat().st_size != size:
                raise ValueError('archive size changed')
            digest = unbuffered_sha(source, copy_to=path)
            if digest != r['audit']['file_sha256']:
                raise ValueError('archive bytes changed')
            pending[executor.submit(audit_file, path, r, executable)] = (path, size)
            held += size
        while pending:
            reap()
    run.rmdir()  # Only the now-empty, invocation-owned directory.


def verify(root, allow_bounded=False, deep=False, *, workers=1, scratch=None,
           reserve_gb=250, staging_gb=24, progress=None):
    root = Path(root)
    marker = root / 'ARCHIVE.COMPLETE.json'
    if not marker.exists() and allow_bounded:
        marker = root / 'BOUNDED.TEST.COMPLETE.json'
    marker_sha = sha(marker)
    m = read_json(marker)
    identity = read_json(root / 'identity.json')
    idh = sha(root / 'identity.json')
    if m['identity'] != idh or m['bounded'] != identity['bounded'] or (m['bounded'] and not allow_bounded):
        raise ValueError('wrong archive identity/scope')
    if not m['bounded'] and identity['expected_records'] != EXPECTED:
        raise ValueError('wrong full-snapshot size')
    inv = identity['inventory']
    validate_inventory(inv)
    if not m['bounded']:
        expected_plans = [dict(id=i, start=s, end=inv['starts'][i+1] if i+1 < len(inv['starts']) else None) for i, s in enumerate(inv['starts'])]
        if identity['plans'] != expected_plans:
            raise ValueError('source range coverage changed')
    elif len(identity['plans']) != 1 or identity['plans'][0]['start'] != inv['first_key'] or identity['plans'][0]['id'] != 0:
        raise ValueError('invalid bounded coverage')
    prov = root / 'provenance/manifest.json'
    if sha(prov) != m['provenance_manifest_sha256']:
        raise ValueError('provenance manifest changed')
    for name, digest in read_json(prov).items():
        p = (prov.parent / name).resolve()
        if prov.parent.resolve() not in p.parents or sha(p) != digest:
            raise ValueError('provenance file changed')
    actual = {p.relative_to(root).as_posix(): p for p in (root / 'receipts').rglob('*.json')}
    if set(actual) != set(m['receipts']):
        raise ValueError('archive receipt set changed')
    rows = []
    for name, path in actual.items():
        if sha(path) != m['receipts'][name]:
            raise ValueError('archive receipt changed')
        rows.append(read_json(path))
    files, entries, count, bytes_ = set(), [], 0, 0
    for plan in identity['plans']:
        records = sorted([r for r in rows if r['range'] == plan['id']], key=lambda r: r['index'])
        _, complete, n = verify_chain(plan, records, idh)
        if not complete:
            raise ValueError('incomplete source range')
        for r in records:
            name = f'parquet/r{plan["id"]:03d}/c{r["index"]:06d}.parquet'
            if r['file'] != name or name in files:
                raise ValueError('invalid archive file path')
            files.add(name)
            bytes_ += r['audit']['bytes']
            entries.append(r)
        count += n
    if count != m['records'] or count != identity['expected_records'] or len(files) != m['shards'] or bytes_ != m['parquet_bytes']:
        raise ValueError('final archive totals disagree')
    if len(rows) != len(files) or {p.relative_to(root).as_posix() for p in (root/'parquet').rglob('*.parquet')} != files:
        raise ValueError('extra archive data')
    executable = HERE / 'chessdb-archive.exe'
    if deep and sha(executable) != identity['producer']['chessdb-archive.exe']:
        raise ValueError('native auditor differs from archived producer')
    audit_entries(root, entries, deep=deep, executable=executable, workers=workers,
                  scratch=scratch, reserve_gb=reserve_gb, staging_gb=staging_gb, progress=progress)
    if sha(marker) != marker_sha or sha(root/'identity.json') != idh:
        raise ValueError('archive identity changed during verification')
    if {p.relative_to(root).as_posix() for p in (root/'parquet').rglob('*.parquet')} != files:
        raise ValueError('archive file set changed during verification')
    if {p.relative_to(root).as_posix() for p in (root/'receipts').rglob('*.json')} != set(m['receipts']):
        raise ValueError('archive receipt set changed during verification')
    for name, digest in m['receipts'].items():
        if sha(root/name) != digest:
            raise ValueError('receipt changed during verification')
    return dict(verified=True, bounded=m['bounded'], deep=deep, records=count,
                shards=len(files), bytes=bytes_, marker_sha256=marker_sha,
                workers=workers, original_source_required=False)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--allow-bounded', action='store_true')
    p.add_argument('--deep', action='store_true')
    p.add_argument('--report', type=Path)
    p.add_argument('--workers', type=int, default=1)
    p.add_argument('--scratch', type=Path)
    p.add_argument('--reserve-gb', type=float, default=250)
    p.add_argument('--staging-gb', type=float, default=24)
    p.add_argument('--progress', type=Path)
    a = p.parse_args()
    if a.workers > 1 and (not a.deep or a.scratch is None):
        p.error('parallel verification needs --deep and --scratch')
    from contextlib import ExitStack
    t = time.monotonic()
    with ExitStack() as stack:
        stack.enter_context(file_lock(a.out/'coordinator.lock'))
        if a.scratch:
            disjoint(a.out, a.scratch, HERE)
            stack.enter_context(file_lock(a.scratch/'verifier.lock'))
        r = verify(a.out, a.allow_bounded, a.deep, workers=a.workers, scratch=a.scratch,
                   reserve_gb=a.reserve_gb, staging_gb=a.staging_gb, progress=a.progress)
    r['seconds'] = time.monotonic() - t
    r['verifier_sha256'] = sha(Path(__file__))
    if a.report:
        write_json(a.report, r)
    if a.progress:
        write_json(a.progress, dict(state='complete', **r))
    print(json.dumps(r))


if __name__ == '__main__':
    main()
