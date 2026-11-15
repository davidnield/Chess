"""Compare every bucket digest and fixed pilot slices from two published explorer layouts."""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from explorer_storage import LOGICAL, read_columns, relation
from month_archive import read_json, sha, write_json

SAMPLE = (7, 201, 421)


def same_fields(a, b, fields, label):
    for field in fields:
        if a[field] != b[field]:
            raise ValueError(f"{label}: different {field}")


def compare(plain, compact, out):
    plain_meta, compact_meta = [read_json(p / "_book.meta.json") for p in (plain, compact)]
    if (plain_meta["schema"], compact_meta["schema"]) != ("book-v3", "book-v4"):
        raise ValueError("expected a v3 reference and a v4 candidate")
    same_fields(plain_meta, compact_meta, ("input", "term_schema", "term_event_ids", "collisions"), "metadata")
    for name in [f"bkt{i:03}.DONE" for i in range(512)] + ["term.DONE"]:
        a,b = [read_json(p / "_done" / name) for p in (plain,compact)]
        fields = ["white_wins", "draws", "black_wins", "total", "rows_in", "rows_out", "digest"]
        if name != "term.DONE":
            fields += ["bucket","positions","ply1_total","collision_hashes","collision_rows"]
        same_fields(a,b,fields,name)
    nrows = nfiles = 0
    with duckdb.connect(config={"threads": 2, "preserve_insertion_order":True}) as con:
        for b in SAMPLE:
            pattern = f"event=*/elo_band=*/bkt{b:03}.parquet"
            paths = sorted(p.relative_to(plain / "ps") for p in (plain / "ps").glob(pattern))
            if not paths or paths != sorted(p.relative_to(compact / "ps") for p in (compact / "ps").glob(pattern)):
                raise ValueError(f"slice set differs for bucket {b}")
            for path in paths:
                left, right = [p/"ps"/path for p in (plain,compact)]
                ref = pq.ParquetFile(left).read(columns=list(LOGICAL))
                arrow = read_columns(right, LOGICAL)
                sql = con.execute(f"SELECT * FROM {relation([right])}").to_arrow_table()
                for name in LOGICAL:
                    if not ref[name].equals(arrow[name]) or not ref[name].equals(sql[name]):
                        raise ValueError(f"{path}: column {name} differs")
                nfiles += 1
                nrows += len(ref)
            print(f"Exact Arrow and SQL comparisons passed for bucket {b}",flush=True)
    result = {"plain_meta":sha(plain/"_book.meta.json"), "compact_meta":sha(compact/"_book.meta.json"),
              "move_bucket_digests":512,"terminal_digest":True,"exact_files":nfiles,"exact_rows":nrows,
              "exact_buckets":SAMPLE,"logical_columns":LOGICAL}
    write_json(out, result)
    print(result,flush=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for arg in ("plain", "compact", "report"):
        parser.add_argument(arg, type=Path)
    args = parser.parse_args()
    compare(args.plain, args.compact, args.report)
