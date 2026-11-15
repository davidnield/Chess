"""Validation and publication receipts for the month extraction launcher."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from contextlib import contextmanager
from pathlib import Path

import pyarrow.parquet as pq


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def sha(path):
    with path.open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


@contextmanager
def root_lock(root):
    root.mkdir(parents=True, exist_ok=True)
    with (root / "_launcher.lock").open("a+b") as f:
        if f.tell() == 0:
            f.write(b"0")
            f.flush()
        f.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def disjoint(*roots):
    roots = [p.resolve() for p in roots]
    for i, p in enumerate(roots):
        if p == Path(p.anchor) or any(p == q or p in q.parents or q in p.parents for q in roots[:i]):
            raise ValueError(f"source, staging and archive must be separate directories: {roots}")


def bind(path, expected, dry=False):
    if path.exists():
        if read_json(path) != expected:
            raise ValueError(f"{path}: settings or source inventory changed")
    elif not dry:
        write_json(path, expected)


def root_binding(root, settings, dry):
    p = root / "_launcher_settings.json"
    if root.exists() and not p.exists() and any(f.name != "_launcher.lock" for f in root.iterdir()):
        raise ValueError(f"{root}: pre-existing data without a launcher settings receipt")
    bind(p, settings, dry)


def inventory(source, ym, events, reference=None):
    y, m = ym
    dirs = [p for p in (source / f"year={y}").glob("month=*")
            if p.name[6:].isdigit() and int(p.name[6:]) == m]
    if len(dirs) != 1:
        raise ValueError(f"{y}_{m}: expected one source directory, found {dirs}")
    paths = sorted(f for e in dirs[0].iterdir() if e.name in {"event=" + s for s in events}
                   for f in e.glob("*.parquet") if f.is_file())
    present = {p.parent.name[6:] for p in paths}
    missing = sorted(set(events) - present)
    if reference:
        provenance = read_json(reference / f"month={y}_{m}.json")
        if provenance.get("month") != f"{y}_{m}" or not set(events) <= set(provenance["flags"]["events"]):
            raise ValueError(f"{y}_{m}: independent provenance does not cover the requested population")
        recorded = provenance["files"]
        expected = set()
        for filename in recorded:
            partition = Path(filename.replace(chr(92), "/"))
            if partition.parent.name in {"event=" + e for e in events}:
                expected.add(partition.parent.name + "/" + partition.name)
        if expected != {f"{p.parent.name}/{p.name}" for p in paths}:
            raise ValueError(f"{y}_{m}: source partitions disagree with independent provenance")
    if not paths or (missing and not reference):
        raise ValueError(f"{y}_{m}: no independent provenance for missing speeds {missing}")
    entries = []
    for p in paths:
        stat = p.stat()
        rows = pq.ParquetFile(p).metadata.num_rows
        if not rows:
            raise ValueError(f"{p}: empty input partition")
        entries.append({"file": p.relative_to(source).as_posix(), "rows": rows,
                        "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns})
    return {"files": entries, "absent_events": missing}


def check_root_params(root, a):
    lock = read_json(root / "_extract_params.json")
    expected = {"producer": "rust-month", "ply_key": True, "buckets": 512, "events": a.events,
                "max_ply": a.max_ply, "chunk_games": 250000, "min_elo": 0, "exclude_bots": True,
                "excluded_terminations": ["Abandoned", "Rules infraction"]}
    if (any(lock.get(k) != v for k,v in expected.items())
            or lock.get("key_bits", 64) != (128 if a.key128 else 64)
            or lock.get("term_slices", False) != a.term_slices):
        raise ValueError(f"{root}: extractor settings disagree with the launcher request")
    return lock


def check_month(parts, ym, a, source):
    root = parts["dir"].parent
    lock = check_root_params(root, a)
    prov = read_json(parts["provenance"])
    expected = {"events": a.events, "max_ply": a.max_ply, "ply_key": True}
    if any(lock.get(k) != v or prov["flags"].get(k) != v for k, v in expected.items()):
        raise ValueError(f"{root}: extract settings disagree with request")
    if (prov["key_bits"] != lock.get("key_bits", 64)
            or prov.get("term_slices", False) != a.term_slices):
        raise ValueError(f"{root}: key width or terminal schema disagrees with request")
    tag = f"{ym[0]}_{ym[1]}"
    if (prov["month"] != tag or prov["flags"]["months"] != [tag]
            or Path(prov["flags"]["source"]).resolve() != a.source
            or {Path(f).resolve() for f in prov["files"]} != {a.source / x["file"] for x in source["files"]}
            or prov["games_in_footers"] != sum(x["rows"] for x in source["files"])):
        raise ValueError(f"{tag}: source files or row counts disagree with provenance")
    manifest, = pq.ParquetFile(parts["manifest"]).read().to_pylist()
    marker = read_json(parts["sentinel"])
    # JSON and Arrow may round the fractional extraction duration differently.
    if any(marker[k] != v for k, v in manifest.items() if k != "seconds"):
        raise ValueError(f"{tag}: manifest disagrees with completion marker")
    buckets = sorted(p for p in parts["dir"].rglob("*") if p.is_file())
    if (manifest["year"], manifest["month"]) != ym or any(
            p.name != "part-0000.parquet" or p.parent.parent != parts["dir"] or
            not p.parent.name.startswith("bkt=") or not 0 <= int(p.parent.name[4:]) < lock["buckets"]
            for p in buckets):
        raise ValueError(f"{tag}: unexpected bucket layout")
    if (len(buckets) != manifest["files"] or sum(p.stat().st_size for p in buckets) != manifest["bytes"]
            or prov["rows"] != manifest["rows"] or prov["output_bytes"] != manifest["bytes"]
            or pq.ParquetFile(parts["term"]).metadata.num_rows != prov["term_rows"]):
        raise ValueError(f"{tag}: incomplete bucket or terminal files")
    conflicts = sorted(p for p in parts["conflicts"].rglob("*") if p.is_file())
    if bool(conflicts) != bool(prov["conflicts"]):
        raise ValueError(f"{tag}: conflict file and provenance disagree")
    return buckets + conflicts + [parts[k] for k in ("manifest", "provenance", "term", "sentinel")]


def safe_child(root, p):
    if p.resolve() != root.resolve() / p.relative_to(root) or p.resolve() == root.resolve():
        raise ValueError(f"{p}: unsafe path under {root}")


def release(root, parts):
    for k in ("sentinel", "dir", "conflicts", "manifest", "provenance", "term"):
        p = parts[k]
        safe_child(root, p)
        if p.is_dir():
            shutil.rmtree(p)
        else:
            p.unlink(missing_ok=True)

