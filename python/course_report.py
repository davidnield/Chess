"""Repertoire statistics for every course book and for our own books, side by side
(stage 3 of the course benchmarks).

Loads the 1800+ pool once and runs repertoire_stats.book_stats over every book in
<work>/books (course books, named <course_id>_<side>.parquet) plus any of our
books passed with --ours LABEL=COLOUR=PATH. Writes <work>/course_stats.parquet
(one row per book; nested fields as JSON strings) and prints a summary.

    .venv/Scripts/python.exe python/course_report.py \\
        --ours "price400=white=E:/chess/repertoire/_budget_price/price_white_b400.parquet"
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from repertoire_stats import POOL, PoolIndex, book_stats, load_book  # noqa: E402

WORK = Path("E:/chess/courses")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--work", default=str(WORK))
    ap.add_argument("--pool", default=POOL)
    ap.add_argument("--ours", action="append", default=[], metavar="LABEL=COLOUR=PATH")
    ap.add_argument("--limit", type=int, default=0)
    a = ap.parse_args()
    work = Path(a.work)
    t0 = time.time()
    idx = PoolIndex(a.pool)
    print(f"pool loaded ({time.time() - t0:,.0f}s)", flush=True)
    from eval_arrays import MISSING, lookup_evals, open_eval_arrays
    from stage3_backwards_induction import LICHESS_CP_SCALE
    mm = open_eval_arrays()

    def evals(hs):
        cp = lookup_evals(hs, *mm).astype(np.float64)
        es = 1.0 / (1.0 + np.exp(-LICHESS_CP_SCALE * cp))
        return np.where(cp == MISSING, np.nan, es)

    meta = pl.read_parquet(work / "course_books.parquet")
    jobs = [(r["course_id"], r["side"], work / "books" / f"{r['course_id']}_{r['side']}.parquet",
             {k: r[k] for k in ("course_name", "publisher", "category", "subcategory")})
            for r in meta.iter_rows(named=True)]
    for spec in a.ours:
        label, colour, path = spec.split("=", 2)
        jobs.insert(0, (f"ours:{label}", colour, Path(path),
                        {"course_name": f"OURS: {label}", "publisher": "generated",
                         "category": "ours", "subcategory": None}))
    if a.limit:
        jobs = jobs[:a.limit]
    rows = []
    for i, (cid, side, path, info) in enumerate(jobs, 1):
        if not path.exists():
            continue
        try:
            s = book_stats(load_book(path, side), side, idx, evals)
        except Exception as e:  # noqa: BLE001
            print(f"  {cid} {side}: {e}", flush=True)
            continue
        rows.append({"course_id": cid, "side": side, **info,
                     **{k: (json.dumps(v) if isinstance(v, dict) else v) for k, v in s.items()}})
        if i % 50 == 0:
            print(f"  [{i}/{len(jobs)}] ({time.time() - t0:,.0f}s)", flush=True)
    df = pl.DataFrame(rows, infer_schema_length=None)
    df.write_parquet(work / "course_stats.parquet")
    print(f"{df.height} books in {time.time() - t0:,.0f}s -> {work / 'course_stats.parquet'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
