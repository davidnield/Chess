"""Content identities for immutable ChessDB generations; no mtime-only acceptance."""
from pathlib import Path
from month_archive import sha

def stamp(path):
    path = Path(path)
    s = path.stat()
    if not path.is_file():
        raise ValueError(f"not a regular file: {path}")
    return dict(bytes=s.st_size, mtime_ns=s.st_mtime_ns, file_id=s.st_ino)

def fingerprint(path, expected=None):
    before = stamp(path)
    digest = sha(Path(path))
    if stamp(path) != before:
        raise ValueError(f"file changed while hashing: {path}")
    if expected is not None and digest != expected:
        raise ValueError(f"content differs from receipt: {path}")
    return dict(sha256=digest, **before)

def unchanged(path, identity):
    if stamp(path) != {k: identity[k] for k in ("bytes", "mtime_ns", "file_id")}:
        raise ValueError(f"file changed during build: {path}")

def exact_buckets(directory, pattern, names):
    actual = {p.name for p in Path(directory).glob(pattern)}
    if actual != set(names):
        raise ValueError(f"wrong bucket set in {directory}: missing={sorted(set(names)-actual)[:8]}, extra={sorted(actual-set(names))[:8]}")
