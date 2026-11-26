"""One-use WSL source retirement; literal target and accepted parity required."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

p = argparse.ArgumentParser()
p.add_argument('--execute', action='store_true')
a = p.parse_args()
root = Path('/var/lib/chessdb/chess-20260702/data')
evidence = Path('/mnt/d/chess/acceptance/chessdb-archive-query-20261125')
accept = json.loads((evidence/'frozen-acceptance.json').read_text())
assert accept['verified'] and accept['native_exact'] and accept['rows'] == 971
assert root.resolve(strict=True) == root and not root.is_symlink()
files = sorted(root.iterdir())
assert len(files) == 582 and all(f.is_file() and not f.is_symlink() for f in files)
total = sum(f.stat().st_size for f in files)
assert total == 1188168027771
for fd in Path('/proc').glob('[0-9]*/fd/*'):
    try:
        link = os.readlink(fd)
    except FileNotFoundError:
        continue
    assert not (link.startswith(str(root)) or link.startswith('/mnt/chessdb-ro/')), (fd, link)
for mp in Path('/proc').glob('[0-9]*/maps'):
    try:
        text = mp.read_text()
    except (FileNotFoundError, ProcessLookupError):
        continue
    assert str(root) not in text and '/mnt/chessdb-ro/' not in text, mp
inventory = dict(target=str(root), files=len(files), bytes=total, utc=time.time(),
                 entries=[dict(name=f.name, bytes=f.stat().st_size) for f in files])
print(json.dumps({k:v for k,v in inventory.items() if k != 'entries'}), flush=True)
if not a.execute:
    raise SystemExit(0)
assert not (evidence/'native-retirement.json').exists()
# Preserve the last native executable and copy provenance outside the VHD too.
shutil.copy2('/opt/chessdb-pilot/import/cdb_lookup', evidence/'native-cdb_lookup')
shutil.copy2('/var/lib/chessdb/COPY.DONE', evidence/'native-COPY.DONE')
assert hashlib.sha256((evidence/'native-cdb_lookup').read_bytes()).hexdigest() == 'a535119c844395cd9ea52ef9b1b12edaae5e0191d508070622dc97707986fe14'
(evidence/'native-retirement-preflight.json').write_text(json.dumps(inventory, indent=2))
# There can be stacked read-only binds. Unmount only this exact mountpoint,
# requiring each layer to refer to this exact source.
for _ in range(4):
    if subprocess.run(['mountpoint', '-q', '/mnt/chessdb-ro']).returncode:
        break
    mounts = subprocess.check_output(['findmnt', '-rn', '-o', 'SOURCE', '/mnt/chessdb-ro'], text=True).splitlines()
    assert mounts and all('['+str(root)+']' in x for x in mounts), mounts
    subprocess.run(['umount', '/mnt/chessdb-ro'], check=True)
assert subprocess.run(['mountpoint', '-q', '/mnt/chessdb-ro']).returncode != 0
assert root.resolve(strict=True) == root
shutil.rmtree(root)
assert not root.exists()
Path('/var/lib/chessdb/COPY.DONE').rename('/var/lib/chessdb/COPY.RETIRED-20261125')
inventory['removed'] = True
(evidence/'native-retirement.json').write_text(json.dumps(inventory, indent=2))
Path('/var/lib/chessdb/RAW.RETIRED-20261125.json').write_text(json.dumps(dict(archive='F:/chess/chessdb-archive-20260702-v6-zstd3', evidence=str(evidence), replacement='D:/chess/bin/chessdb-archive-query-20261125-v1'), indent=2))
subprocess.run(['sync'], check=True)
print('Native raw copy retired; Ubuntu and all tools preserved.', flush=True)
