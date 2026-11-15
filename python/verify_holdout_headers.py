"""Reconcile the held-out Parquets to the original monthly PGN, using the ingestion event map."""
from __future__ import annotations

import argparse
import ast
import collections
import re
import shutil
import subprocess
from pathlib import Path

import pyarrow as pa

from month_archive import inventory, read_json, sha, write_json

SOURCES = {
    8: (91912325, "6bf6fa8a5dee7bb81d1874ac312160060daf12f18a29dc2740a3bf6f5e5e6248"),
    9: (89616462, "1de8eb64fed443f4be310c779e8d60a13bcfc6e3b104bcb39a018eb056edec89"),
}
EVENT = re.compile(br'^\[Event "([^\n]*)"\]\r?$', re.MULTILINE)


def headers(reader, block=8 << 20):
    carry = b""
    while chunk := reader.read(block):
        data = carry + chunk
        cut = data.rfind(b"\n") + 1
        yield from (m[1] for m in EVENT.finditer(data, 0, cut))
        carry = data[cut:]
    yield from (m[1] for m in EVENT.finditer(carry))


def mapped_counts(stream, mapping):
    tally = collections.Counter()
    other = collections.Counter()
    for event in headers(stream):
        if event in mapping:
            tally[mapping[event]] += 1
        else:
            other[event] += 1
    return tally, other


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "recipe", "work", "out"):
        ap.add_argument("--" + name, type=Path, required=True)
    args = ap.parse_args()
    recipe = ast.parse(args.recipe.read_text(encoding="utf-8-sig"))
    mapping, = [ast.literal_eval(stmt.value) for stmt in recipe.body if isinstance(stmt,ast.Assign)
                and any(isinstance(t,ast.Name) and t.id=="EVENT_MAP" for t in stmt.targets)]
    byte_map = {k.encode():v for k,v in mapping.items()}
    args.work.mkdir(parents=True,exist_ok=True)
    for month,(expected,digest) in SOURCES.items():
        tag=f"2026_{month}"
        name=f"lichess_db_standard_rated_2026-{month:02d}.pgn.zst"
        url="https://database.lichess.org/standard/" + name
        compressed=args.work/name
        output=args.out/f"month={tag}.json"
        local=inventory(args.source,(2026,month),sorted(set(mapping.values())))
        recipe_sha=sha(args.recipe)
        if output.exists():
            prev=read_json(output)
            if prev["source_inventory"]!=local or prev["pgn_sha256"]!=digest or prev["recipe_sha256"]!=recipe_sha:
                raise ValueError(f"{tag}: completed holdout control no longer matches its inputs")
            continue
        if shutil.disk_usage(args.work).free < 150*10**9:
            raise ValueError("holdout verification needs scratch space for one resumable PGN")
        print(f"{tag}: fetching pinned PGN into resume cache",flush=True)
        if not (compressed.is_file() and sha(compressed) == digest):
            subprocess.run(["curl.exe","--fail","--location","--silent","--show-error","--retry","3",
                            "--continue-at","-","--output",str(compressed),url],check=True)
            if sha(compressed)!=digest:
                raise ValueError(f"{compressed}: SHA-256 differs from Lichess's published checksum")
        with pa.input_stream(str(compressed), compression="zstd") as stream:
            counts, other=mapped_counts(stream,byte_map)
        measured=collections.Counter()
        for entry in local["files"]:
            measured[Path(entry["file"]).parent.name[6:]]+=entry["rows"]
        if sum(counts.values())+sum(other.values())!=expected or counts!=measured:
            raise ValueError(f"{tag}: raw accepted speeds {counts}, local {measured}, rejected {sum(other.values())}, official {expected}")
        record={"month":tag,"flags":{"events":sorted(set(mapping.values()))},
                "files":[str((args.source/entry["file"]).resolve()) for entry in local["files"]],
                "source_inventory":local,"game_counts":counts,"pgn":url,"pgn_sha256":digest,
                "recipe_sha256":recipe_sha,"official_standard_total":expected,
                "excluded_event_rows":sum(other.values()),
                "most_common_excluded_labels":[[k.decode("utf-8",errors="replace"),v] for k,v in other.most_common(20)]}
        write_json(output,record)
        compressed.unlink()
        print(f"{tag}: independently reconciled {sum(counts.values()):,} local games",flush=True)


if __name__=="__main__":
    main()
