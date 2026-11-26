"""One-use, metadata-only deployment. No extraction and no payload mutation."""
import json
import shutil
from pathlib import Path
import sys

repo = next(p for p in Path(__file__).resolve().parents if (p/'python/source_inventory.py').is_file())
sys.path.insert(0, str(repo/'python'))
import source_inventory as inv
import month_archive as archive

source = Path('D:/data/chess/standard-chess-games-compressed')
dest = Path('F:/chess/explorer_brc_p51_pilot')
stage = Path('D:/chess/explorer_brc_p51_pilot_stage')
release = Path('D:/chess/bin/source-inventory-launcher-20261125-v1')
evidence = repo/'docs/evidence/source-inventory-migration-20261125'
evidence.mkdir(parents=True, exist_ok=True)
obj = inv.load(repo/'scratch/source-inventory-package-20261125/inventory-v1.json')
assert len(obj['months']) == 163

def snapshot():
    paths = [dest/'_extract_params.json', dest/'_launcher_settings.json']
    paths += list(dest.glob('_month=*.DONE'))
    paths += list((dest/'_archive_receipts').glob('*.json'))
    paths += list((dest/'_provenance').glob('*.json'))
    paths += [stage/'_launcher_settings.json', stage/'_extract_params.json']
    paths += list((stage/'_source_inventory').glob('*.json'))
    book=Path('E:/chess/position-stats/brc-p51-train-201301-202607-20261115')
    paths += [book/f for f in ('_BOOK.DONE','_book.meta.json','_merge_params.json')]
    return {str(p):archive.sha(p) for p in paths}

before=snapshot()
archive.bind(evidence/'before.json',before)
for root in (stage,dest):
    inv.attach(obj,root,dry=True)
with archive.file_lock(source/'_source_metadata/inventory.lock'):
    inv.publish(source/inv.RELATIVE,obj)
for root in (stage,dest):
    with archive.root_lock(root):
        inv.attach(obj,root)
after=snapshot()
assert before==after, 'Existing publication metadata changed'
archive.bind(evidence/'after.json',after)

# New standalone release; old releases and live jobs are deliberately untouched.
release.mkdir(exist_ok=True)
hashes={}
for name in ('source_inventory.py','month_archive.py','run_month_extraction.py'):
    src=repo/'python'/name; target=release/name
    if target.exists():
        assert archive.sha(target)==archive.sha(src), 'Existing release differs'
    else:
        shutil.copy2(src,target)
    hashes[name]=archive.sha(target)
exe=Path('D:/chess/bin/ply51-pilot-20261115/explorer-extract.exe')
settings=archive.read_json(dest/'_launcher_settings.json')
assert settings['exe']=={str(exe):archive.sha(exe)}
archive.bind(release/'release.json',dict(files=hashes,extractor=settings['exe'],source_inventory_identity=inv.identity(obj)))
result=dict(identity=inv.identity(obj),months=len(obj['months']),
            files=sum(len(r['inventory']['files']) for r in obj['months'].values()),
            games=sum(sum(f['rows'] for f in r['inventory']['files']) for r in obj['months'].values()),
            inventory_bytes=(source/inv.RELATIVE).stat().st_size,
            copies={str(root/inv.RELATIVE):archive.sha(root/inv.RELATIVE) for root in (source,stage,dest)},
            existing_metadata_files_unchanged=len(before),release=str(release),
            release_sha256=archive.sha(release/'release.json'))
archive.bind(evidence/'deployment.json',result)
print(json.dumps(result,indent=2))
