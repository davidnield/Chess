"""Download one folder of a Hugging Face dataset repository, pinned to a revision, verified, resumable.

    python hf_fetch.py --repo Lichess/chess-position-evaluations --prefix data --out H:/chess/evals/cloud_new
                       [--rev <sha>] [--workers 4] [--flat] [--manifest-prefix cloud]

Lists the folder through the public tree API (no token, no extra packages), pins the repository's
current commit unless --rev is given, and for every file: check the pinned hash and size in its local receipt; else
download to `<file>.part` (resuming a partial one with a Range request), check the size and the
hash the listing gave (sha256 for LFS files, the git blob sha1 for small ones), rename, and write
`<file>.ok`. When every file is verified it writes `manifest.tsv` (relative path, bytes, hash,
pinned URL: the layout of H:/chess/evals/manifest.tsv) and `_DOWNLOAD.DONE` last.

--flat drops the prefix's directories from the local paths (data/data_0000.parquet ->
data_0000.parquet). Stdlib only; a generic User-Agent; nothing about this machine is sent.
"""
from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

API = "https://huggingface.co/api/datasets"
UA = {"User-Agent": "hf-fetch/1.0"}
CHUNK = 8 << 20
_lock = threading.Lock()


def log(msg: str) -> None:
    with _lock:
        print(f"{time.strftime('%Y-%m-%dT%H:%M:%S')}  {msg}", flush=True)


def get_json(url: str):
    for attempt in range(8):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60) as r:
                return json.load(r), r.headers.get("Link")
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            log(f"retry listing ({e})")
            time.sleep(5 * (attempt + 1))
    raise SystemExit(f"FATAL: cannot list {url}")


def listing(repo: str, rev: str, prefix: str) -> list[dict]:
    url = f"{API}/{repo}/tree/{rev}/{urllib.parse.quote(prefix)}?recursive=true&limit=1000"
    out = []
    while url:
        page, link = get_json(url)
        out += [x for x in page if x["type"] == "file"]
        url = None
        if link and 'rel="next"' in link:
            url = link.split(";")[0].strip().strip("<>")
    return out


def digest(path: Path, kind: str, size: int) -> str:
    h = hashlib.sha256() if kind == "sha256" else hashlib.sha1(f"blob {size}\0".encode())
    with open(path, "rb") as f:
        while True:
            b = f.read(CHUNK)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def atomic_text(path: Path, value: str) -> None:
    part = path.with_name(path.name + ".tmp")
    with part.open("w", encoding="utf-8", newline="\n") as out:
        out.write(value)
        out.flush()
        os.fsync(out.fileno())
    part.replace(path)


def fetch(job: tuple, *, rehash=False, offline=False) -> bool:
    url, dst, size, kind, want = job
    ok = dst.with_name(dst.name + ".ok")
    expected = f"{kind} {want}"
    if dst.is_file() and dst.stat().st_size == size:
        marked = ok.is_file() and ok.read_text(encoding="utf-8").strip() == expected
        if marked and not (rehash or offline):
            return True
        if digest(dst, kind, size) == want:
            if not marked:
                atomic_text(ok, expected + "\n")
            return True
    if offline and size:
        raise ValueError(f"{dst}: missing or corrupt pinned file, local verification stopped")
    dst.parent.mkdir(parents=True, exist_ok=True)
    part = dst.with_name(dst.name + ".part")
    if not size:
        if dst.exists() and dst.stat().st_size:
            raise ValueError(f"{dst}: refusing to replace nonempty file with empty blob")
        empty_digest = hashlib.sha256(b"") if kind == "sha256" else hashlib.sha1(b"blob 0\0")
        if empty_digest.hexdigest() != want:
            raise ValueError(f"{dst}: wrong advertised digest for empty blob")
        part.write_bytes(b"")
    for attempt in range(1, 21):
        have = part.stat().st_size if part.exists() else 0
        if have > size:
            part.unlink()
            have = 0
        try:
            if have < size:
                hdr = dict(UA)
                if have:
                    hdr["Range"] = f"bytes={have}-"
                t0, got = time.time(), 0
                with urllib.request.urlopen(urllib.request.Request(url, headers=hdr), timeout=120) as r:
                    if have and r.status != 206:
                        have = 0
                    with open(part, "ab" if have else "wb") as f:
                        while True:
                            b = r.read(CHUNK)
                            if not b:
                                break
                            f.write(b)
                            got += len(b)
                log(f"GOT  {dst.name} ({size / 1e9:.2f} GB, {got / 1e6 / max(time.time() - t0, 1e-9):.1f} MB/s)")
            if part.stat().st_size != size:
                raise OSError(f"size {part.stat().st_size} != {size}")
            got_hash = digest(part, kind, size)
            if got_hash != want:
                part.unlink()
                raise OSError(f"{kind} mismatch")
            part.replace(dst)
            atomic_text(ok, expected + "\n")
            return True
        except (urllib.error.URLError, http.client.HTTPException, TimeoutError, ConnectionError, OSError) as e:
            log(f"retry {attempt} {dst.name}: {e}")
            time.sleep(min(10 * attempt, 120))
    log(f"FAILED {dst.name}")
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--repo", required=True)
    ap.add_argument("--prefix", required=True)
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--rev", default=None)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--flat", action="store_true")
    ap.add_argument("--manifest-prefix", default="")
    ap.add_argument("--rehash", action="store_true", help="Read and verify existing file contents again.")
    ap.add_argument("--verify-local", action="store_true", help="Verify existing data without fetching payloads; permit reconstructing a pinned empty blob.")
    a = ap.parse_args()
    if str(a.out).upper().startswith("F:"):
        raise SystemExit("FATAL: not onto F:")
    done = a.out / "_DOWNLOAD.DONE"
    rev = a.rev or get_json(f"{API}/{a.repo}")[0]["sha"]
    if not re.fullmatch(r"[0-9a-f]{40}", rev):
        raise SystemExit("FATAL: a full commit SHA is required")
    if done.exists() and f"({a.repo}@{rev})" not in done.read_text(encoding="utf-8"):
        raise SystemExit(f"FATAL: {done} belongs to a different revision; use a new generation")
    files = listing(a.repo, rev, a.prefix)
    if not files:
        raise SystemExit(f"FATAL: nothing under {a.prefix} at {rev}")
    jobs, lines = [], []
    for x in sorted(files, key=lambda x: x["path"]):
        rel = x["path"][len(a.prefix):].lstrip("/") if a.flat else x["path"]
        kind, want = ("sha256", x["lfs"]["oid"]) if x.get("lfs") else ("sha1", x["oid"])
        url = f"https://huggingface.co/datasets/{a.repo}/resolve/{rev}/{urllib.parse.quote(x['path'])}"
        jobs.append((url, a.out / rel, x["size"], kind, want))
        name = "\\".join(p for p in (a.manifest_prefix, rel.replace("/", "\\")) if p)
        lines.append(f"{name}\t{x['size']}\t{want}\t{url}")
    if len({j[1].resolve() for j in jobs}) != len(jobs) or any(
            not j[1].resolve().is_relative_to(a.out.resolve()) for j in jobs):
        raise SystemExit("FATAL: duplicate or escaping output path")
    manifest = a.out / "manifest.tsv"
    listing_text = "\n".join(lines) + "\n"
    if done.exists() and (not manifest.exists() or manifest.read_text(encoding="utf-8") != listing_text):
        raise SystemExit("FATAL: completed folder has a different manifest")
    total = sum(j[2] for j in jobs)
    log(f"===== {a.repo}@{rev} {a.prefix}: {len(jobs)} files, {total / 1e9:.1f} GB -> {a.out} ({a.workers} workers)")
    a.out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=a.workers) as ex:
        res = list(ex.map(lambda job: fetch(job, rehash=a.rehash or a.verify_local, offline=a.verify_local), jobs))
    if not all(res):
        log(f"{res.count(False)} file(s) failed; rerun to resume")
        return 1
    atomic_text(manifest, listing_text)
    atomic_text(done, f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {len(jobs)} files verified ({a.repo}@{rev})\n")
    log(f"{len(jobs)} files verified, {total / 1e9:.1f} GB in {(time.time() - t0) / 3600:.2f} h")
    return 0


if __name__ == "__main__":
    sys.exit(main())
