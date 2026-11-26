"""Read-only directory metadata inventory; does not open dataset contents."""
import collections
import datetime
import json
import os
from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

OUT = Path(__file__).with_suffix('')
OUT.mkdir(exist_ok=True)
groups = collections.defaultdict(lambda: {'files': 0, 'directories': 0, 'bytes': 0, 'rounded_file_allocation_bytes': 0, 'newest_mtime': 0})
errors = []
stack = [Path('F:/')]
started = time.time()
visited = 0
def scan(directory):
    records = []
    problems = []
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                p = Path(entry.path)
                rel = p.relative_to(Path('F:/'))
                if entry.is_symlink():
                    problems.append({'path': str(p), 'reason': 'link skipped'})
                    continue
                is_dir = entry.is_dir(follow_symlinks=False)
                parts = rel.parts if is_dir else rel.parts[:-1]
                keys = ['.'] + ['/'.join(parts[:n]) for n in range(1, min(3, len(parts)) + 1)]
                st = entry.stat(follow_symlinks=False)
                records.append((p, is_dir, keys, st.st_size, st.st_mtime))
    except (PermissionError, OSError) as error:
        problems.append({'path': str(directory), 'reason': repr(error)})
    return records, problems

with ThreadPoolExecutor(max_workers=8) as pool:
    active = set()
    while stack or active:
        while stack and len(active) < 32:
            active.add(pool.submit(scan, stack.pop()))
        finished, active = wait(active, return_when=FIRST_COMPLETED)
        for future in finished:
            records, problems = future.result()
            errors.extend(problems)
            for p, is_dir, keys, size, mtime in records:
                for key in keys:
                    row = groups[key]
                    if is_dir:
                        row['directories'] += 1
                    else:
                        row['files'] += 1
                        row['bytes'] += size
                        row['rounded_file_allocation_bytes'] += ((size + 262143) // 262144) * 262144
                        row['newest_mtime'] = max(row['newest_mtime'], mtime)
                if is_dir:
                    stack.append(p)
            visited += 1
            if visited % 1000 == 0:
                (OUT/'progress.json').write_text(json.dumps({'directories_visited':visited,'pending':len(stack)+len(active),'seconds':time.time()-started,'files':groups['.']['files']}))

result = {'started_utc':datetime.datetime.fromtimestamp(started,datetime.timezone.utc).isoformat(), 'seconds':time.time()-started, 'scope':'file sizes and directory metadata; no byte verification or backup verification', 'groups':dict(sorted(groups.items())), 'errors':errors}
(OUT/'inventory.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
print(json.dumps({'seconds': result['seconds'], 'errors':errors, 'groups':{k:v for k,v in result['groups'].items() if k.count('/')<=1}}, indent=2))
