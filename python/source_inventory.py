"""Portable, evidence-backed input inventories for monthly extraction.

Create explicitly from independent historical provenance. A missing package is
never silently replaced by a scan of the current source. Publication writes its
SHA-256 binding last; resume checks both the binding and the embedded evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath

import month_archive as archive

RELATIVE = Path('_source_metadata/inventory-v1.json')
EVENTS = ['Blitz', 'Bullet', 'Classical', 'Correspondence', 'Rapid', 'UltraBullet']


def encoded(obj):
    return json.dumps(obj, sort_keys=True, separators=(',', ':')).encode('utf-8')


def identity(obj):
    return hashlib.sha256(encoded(obj)).hexdigest()


def validate(obj):
    if obj.get('schema') != 'extraction-source-inventory-v1' or not obj.get('months'):
        raise ValueError('invalid source inventory schema/months')
    for tag, record in obj['months'].items():
        y, m = map(int, tag.split('_'))
        if tag != f'{y}_{m}' or not 1 <= m <= 12:
            raise ValueError('invalid inventory month')
        evidence = record['provenance']
        if identity(evidence) != record['provenance_sha256'] or evidence['month'] != tag:
            raise ValueError('source provenance digest/month mismatch')
        events = record['events']
        if events != evidence['flags']['events'] or events != [e for e in EVENTS if e in events]:
            raise ValueError('invalid inventory events')
        expected = set()
        for filename in evidence['files']:
            p = PurePosixPath(filename.replace('\\', '/'))
            if p.parent.name not in {'event=' + e for e in events}:
                raise ValueError('unexpected provenance event')
            expected.add(p.parent.name + '/' + p.name)
        actual = set()
        rows = 0
        for f in record['inventory']['files']:
            p = PurePosixPath(f['file'])
            if (len(p.parts) != 4 or p.parts[0] != f'year={y}'
                    or not p.parts[1].startswith('month=') or int(p.parts[1][6:]) != m
                    or p.parts[2] not in {'event=' + e for e in events}
                    or not p.name.endswith('.parquet') or '\\' in f['file']
                    or p.is_absolute() or '..' in p.parts
                    or any(type(f[k]) is not int or f[k] <= 0 for k in ('rows', 'bytes', 'mtime_ns'))):
                raise ValueError('invalid source file entry')
            key = p.parent.name + '/' + p.name
            if key in actual:
                raise ValueError('duplicate source file')
            actual.add(key)
            rows += f['rows']
        absent = sorted(set(events) - {PurePosixPath(f['file']).parent.name[6:] for f in record['inventory']['files']})
        if (actual != expected or rows != evidence['games_in_footers']
                or absent != record['inventory']['absent_events']):
            raise ValueError('inventory disagrees with independent source evidence')
    return obj


def load(path):
    obj = archive.read_json(path)
    bound = archive.read_json(path.with_suffix('.binding.json'))
    if bound != {'schema': 'source-inventory-binding-v1', 'sha256': archive.sha(path), 'identity': identity(obj)}:
        raise ValueError(f'{path}: source inventory binding changed')
    return validate(obj)


def publish(path, obj, *, dry=False):
    validate(obj)
    # A crash before binding publication leaves an unusable file. Only an exact
    # match to the independently supplied expected object may complete it.
    archive.bind(path, obj, dry)
    if not dry:
        archive.bind(path.with_suffix('.binding.json'), {
            'schema': 'source-inventory-binding-v1', 'sha256': archive.sha(path),
            'identity': identity(obj)})
        load(path)
    elif path.with_suffix('.binding.json').exists():
        if identity(load(path)) != identity(obj):
            raise ValueError('source inventory changed')


def create(source, reference, months):
    records = {}
    for ym in months:
        tag = f'{ym[0]}_{ym[1]}'
        p = reference / f'month={tag}.json'
        original = p.read_bytes()
        evidence = json.loads(original)
        events = evidence['flags']['events']
        inv = archive.inventory(source, ym, events, reference)
        if p.read_bytes() != original:
            raise ValueError('independent provenance changed during inventory creation')
        records[tag] = dict(events=events, inventory=inv, provenance=evidence,
                            provenance_sha256=identity(evidence))
    return validate(dict(schema='extraction-source-inventory-v1', months=records,
                         verification='independent filenames, footer rows, file size and mtime; no full source-byte hash'))


def select(obj, ym, events):
    record = obj['months'].get(f'{ym[0]}_{ym[1]}')
    if record is None or not set(events) <= set(record['events']):
        raise ValueError('source inventory does not cover requested month/events')
    inv = record['inventory']
    return {'files': [f for f in inv['files'] if PurePosixPath(f['file']).parent.name[6:] in events],
            'absent_events': sorted(set(inv['absent_events']) & set(events))}


def resolve(a, months):
    explicit = getattr(a, 'source_manifest', None)
    legacy = getattr(a, 'source_provenance', None)
    if explicit and legacy:
        raise ValueError('choose source manifest or legacy provenance, not both')
    embedded = [root / RELATIVE for root in (a.dest, a.stage) if (root / RELATIVE).exists()]
    complete = [p for p in embedded if p.with_suffix('.binding.json').exists()]
    preferred = explicit or (complete[0] if complete else a.source / RELATIVE)
    if legacy:
        obj = create(a.source, legacy, months)
    elif preferred.exists():
        obj = load(preferred)
    elif explicit:
        raise ValueError('explicit source manifest is missing')
    else:
        raise ValueError('source inventory required; create from independent provenance first')
    for path in embedded:
        # Interrupted copy with no binding can be repaired only from a separately
        # valid source package, by publish after all input validations succeed.
        existing = archive.read_json(path)
        if identity(existing) != identity(obj):
            raise ValueError('embedded source inventory changed')
        if path.with_suffix('.binding.json').exists():
            load(path)
    for ym in months:
        select(obj, ym, a.events)
    return obj


def attach(obj, root, *, dry=False):
    """Attach to an existing extraction after reconciling every saved inventory.

    This checks input metadata/receipts, not the archive's TB of payload again.
    Existing settings, per-month receipts and completion hashes are untouched.
    """
    settings = archive.read_json(root / '_launcher_settings.json')
    events = settings['events']
    for p in (root / '_archive_receipts').glob('*.json'):
        ym = tuple(map(int, p.stem.split('_')))
        if archive.read_json(p)['source'] != select(obj, ym, events):
            raise ValueError(f'{p}: legacy archive source differs')
    for p in root.glob('_month=*.DONE'):
        tag = p.stem.removeprefix('_month=')
        if not ((root/'_archive_receipts'/f'{tag}.json').exists()
                or (root/'_source_inventory'/f'{tag}.json').exists()):
            raise ValueError('completed month has no saved source inventory')
    for p in (root / '_source_inventory').glob('*.json'):
        ym = tuple(map(int, p.stem.split('_')))
        if archive.read_json(p) != select(obj, ym, events):
            raise ValueError(f'{p}: staged source differs')
    publish(root / RELATIVE, obj, dry=dry)


def main():
    from run_month_extraction import parse_months
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest='command', required=True)
    c = sub.add_parser('create')
    c.add_argument('--source', type=Path, required=True)
    c.add_argument('--reference', type=Path, required=True)
    c.add_argument('--months', nargs='+', required=True)
    c.add_argument('--output', type=Path)
    t = sub.add_parser('attach')
    t.add_argument('--manifest', type=Path, required=True)
    t.add_argument('--root', type=Path, required=True)
    t.add_argument('--dry-run', action='store_true')
    a = ap.parse_args()
    if a.command == 'create':
        obj = create(a.source.resolve(), a.reference, parse_months(a.months))
        path = a.output or a.source / RELATIVE
        with archive.file_lock(path.parent/'inventory.lock'):
            publish(path, obj)
    else:
        obj = load(a.manifest)
        if a.dry_run:
            attach(obj, a.root, dry=True)
        else:
            with archive.root_lock(a.root):
                attach(obj, a.root)
    print(json.dumps({'identity': identity(obj), 'months': len(obj['months']), 'command': a.command}))


if __name__ == '__main__':
    main()
