"""Weak-population move counts at the positions a main pool reaches (2026-10-18).

weak_stop.py needs "how a weaker population moves" only at positions the budget DP
can value out of book, and every one of those is a position of the MAIN pool (a
parent or a child of its edges). A full pool_from_book build of the weak bands is
the wrong tool: the 1000-1799 Blitz/Rapid/Classical population is ~64M edges per
bucket and projected at ~200 h, almost all of it at positions 1800+ games never
reach. This instead keeps only the weak slices' rows whose parent is a main-pool
position, summed over event, elo band and ply:

    out: parent_hash, move_san, child_hash, total     (no min-games floor; the
                                                       consumer applies its own)

One pass per bucket (the book and the key set are both bucketed by parent hash),
resumable: bucket outputs are written atomically and skipped if present, and the
final concatenation is written last.

    .venv/Scripts/python.exe python/build_weak_edges.py \\
        --pool E:/chess/position-stats/position_stats_pooled_ge1800_2013_2026_brc.parquet \\
        --bands 1000 1200 1400 1600 --out E:/chess/position-stats/weak_edges_b1000_1800_on_ge1800.parquet \\
        --work D:/chess/pool_work/weak_edges_b1000_1800
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import duckdb
import polars as pl

BOOK = Path("E:/chess/position-stats/explorer_banded_2013_2026")
EVENTS = ("Blitz", "Rapid", "Classical")


def log(m: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pool", required=True, help="Main pool: its positions are the keys.")
    ap.add_argument("--book", default=str(BOOK))
    ap.add_argument("--events", nargs="+", default=list(EVENTS))
    ap.add_argument("--bands", nargs="+", type=int, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--work", required=True)
    ap.add_argument("--buckets", nargs="*", type=int, default=None,
                    help="Only these buckets (timing); the final file needs all 512.")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--mem", default="8GB")
    a = ap.parse_args()

    work = Path(a.work)
    (work / "b").mkdir(parents=True, exist_ok=True)
    params = {"pool": a.pool, "book": a.book, "events": a.events, "bands": a.bands}
    pf = work / "_params.json"
    if pf.exists() and json.loads(pf.read_text()) != params:
        log(f"FATAL: {work} was started with other settings: {pf.read_text()}")
        return 1
    pf.write_text(json.dumps(params))

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.mem}'; SET threads={a.threads}; "
                "SET preserve_insertion_order=false;")
    keys = work / "_keys.parquet"
    if not keys.exists():
        log("key set: every main-pool parent and child")
        pool = a.pool.replace("\\", "/")
        con.execute(f"""
            COPY (SELECT DISTINCT h, ((h % 512) + 512) % 512 AS bkt FROM (
                    SELECT parent_hash AS h FROM read_parquet('{pool}')
                    UNION ALL
                    SELECT child_hash FROM read_parquet('{pool}') WHERE child_hash IS NOT NULL))
            TO '{keys.as_posix()}.tmp' (FORMAT parquet)""")
        Path(f"{keys}.tmp").replace(keys)
    n_keys = con.execute(f"SELECT COUNT(*) FROM read_parquet('{keys.as_posix()}')").fetchone()[0]
    log(f"{n_keys:,} main-pool positions")

    todo = a.buckets if a.buckets is not None else list(range(512))
    t0 = time.time()
    done = 0
    for i in todo:
        out = work / "b" / f"b{i:03d}.parquet"
        if out.exists():
            continue
        files = [Path(a.book) / "ps" / f"event={e}" / f"elo_band={b}" / f"bkt{i:03d}.parquet"
                 for e in a.events for b in a.bands]
        files = [f.as_posix() for f in files if f.exists()]
        if not files:
            continue
        t1 = time.time()
        flist = ", ".join(f"'{f}'" for f in files)
        con.execute(f"""
            COPY (
              SELECT s.parent_hash, s.move_san, s.child_hash, SUM(s.total) AS total
              FROM read_parquet([{flist}], hive_partitioning=false) s
              SEMI JOIN (SELECT h FROM read_parquet('{keys.as_posix()}') WHERE bkt = {i}) k
                ON k.h = s.parent_hash
              GROUP BY 1, 2, 3
            ) TO '{out.as_posix()}.tmp' (FORMAT parquet, COMPRESSION zstd)""")
        Path(f"{out}.tmp").replace(out)
        done += 1
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')").fetchone()[0]
        log(f"bucket {i:3d}: {n:,} rows ({time.time()-t1:.0f}s; {done} done, "
            f"{(time.time()-t0)/done:.0f}s/bucket)")

    have = sorted(work.glob("b/b*.parquet"))
    if a.buckets is not None or len(have) < 512:
        log(f"{len(have)}/512 buckets present; not writing {a.out}")
        return 0
    outp = Path(a.out)
    con.execute(f"""COPY (SELECT * FROM read_parquet('{(work / 'b').as_posix()}/b*.parquet')
                          ORDER BY parent_hash)
                    TO '{outp.as_posix()}.tmp' (FORMAT parquet, COMPRESSION zstd)""")
    Path(f"{outp}.tmp").replace(outp)
    meta = {**params, "producer": "build_weak_edges", "keys": n_keys,
            "rows": con.execute(f"SELECT COUNT(*) FROM read_parquet('{outp.as_posix()}')").fetchone()[0],
            "built": time.strftime("%Y-%m-%d %H:%M:%S")}
    Path(f"{outp}.meta.json").write_text(json.dumps(meta, indent=2))
    log(f"wrote {outp} ({meta['rows']:,} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
