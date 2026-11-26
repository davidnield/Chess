"""Verify the deployed release without reading the old provenance directory."""
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import sys
import json

repo = next(p for p in Path(__file__).resolve().parents if (p/'python/source_inventory.py').is_file())
release = Path('D:/chess/bin/source-inventory-launcher-20261125-v1')
sys.path.insert(0, str(release))
import source_inventory as inv
import month_archive as archive
from run_month_extraction import parse_months

source = Path('D:/data/chess/standard-chess-games-compressed')
dest = Path('F:/chess/explorer_brc_p51_pilot')
stage = Path('D:/chess/explorer_brc_p51_pilot_stage')
months = parse_months(['2013_1..2026_7'])
read = archive.read_json
reads = []
def guarded(path):
    path = Path(path)
    assert 'rust_p30k' not in str(path), 'Unexpected old-dataset dependency'
    reads.append(str(path))
    return read(path)

with patch.object(archive, 'read_json', side_effect=guarded):
    a = SimpleNamespace(source=source, dest=dest, stage=stage,
                        events=['Blitz', 'Classical', 'Rapid'],
                        source_manifest=None, source_provenance=None)
    obj = inv.resolve(a, months)
    for ym in months:
        archive.inventory(source, ym, a.events, obj)
    for root in (stage, dest):
        inv.attach(obj, root, dry=True)
    for root in (source, stage, dest):
        assert inv.identity(inv.load(root/inv.RELATIVE)) == inv.identity(obj)
    pin = archive.read_json(release/'release.json')
    for name, digest in pin['files'].items():
        assert archive.sha(release/name) == digest
        assert archive.sha(repo/'python'/name) == digest
    for path, digest in pin['extractor'].items():
        assert archive.sha(Path(path)) == digest

result = dict(months_verified=len(months), identity=inv.identity(obj),
              legacy_provenance_reads=0, metadata_reads=len(reads),
              source_check='all BRC file lists, footers, sizes and mtimes; no full source-byte hash',
              existing_payload_rehashed=False, frozen_release_verified=True)
archive.bind(repo/'docs/evidence/source-inventory-migration-20261125/verification.json',result)
print(json.dumps(result,indent=2))
