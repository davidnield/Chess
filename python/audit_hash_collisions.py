"""Do any two distinct positions share a Zobrist hash in our pool? (#113 step 1)

Everything downstream keys on `parent_hash`: position_stats, the aux sidecar,
the winpos histograms, Stage 3's whole graph, the repertoire parquets, the
trainer. A collision does not error — it silently merges two unrelated positions
into one node, averaging their move distributions and their scores. Nobody has
checked, and the cost of checking is a few hours against a rebuild we cannot
casually redo.

THE TEST
--------
Same hash, different EPD => a genuine collision. This direction is sound because
the hash discriminates AT LEAST as finely as the EPD on every field: polyglot
sets the en-passant key on pawn ADJACENCY while board.epd() prints the ep square
only when the capture is LEGAL, so hash -> EPD is a function while EPD -> hash is
not (the same argument `_test_epd_memo.py` rests on, in the direction that
holds). Castling rights and side-to-move are in both. Halfmove/fullmove counters
are in neither.

HOW MANY TO EXPECT
------------------
Birthday bound on a 64-bit space: E[collisions] ~ n^2 / 2^65. At 1e9 distinct
positions that is 0.03; at 1e10 it is ~2.7. So zero is the expected result for
the >=50-game pool, and a small handful is unsurprising in the unpruned tail.
A large number means something is wrong with the hashing, not with luck.

A LIMIT WORTH KNOWING BEFORE YOU TRUST A CLEAN RESULT
-----------------------------------------------------
Both `_agg_ps` (extract) and `consolidate_monthly` (merge) group by
(parent_hash, move_san) and keep `any_value(parent_epd)`, so per-ply rows are
never persisted ungrouped. If two colliding positions were only ever played with
the SAME move_san, their rows collapse and one EPD is discarded — that collision
is invisible here. Detection therefore requires the two positions to differ in at
least one played move, which is likely (most nodes carry several distinct moves)
but not guaranteed. This audit bounds the problem; it does not prove absence.

Running it against the per-file partials rather than the monthlies does NOT
improve sensitivity — the same grouping already happened at extract time.

PERFORMANCE
-----------
COUNT(DISTINCT parent_epd) per group would build a hash set per group over ~1e9
groups. MIN(parent_epd) != MAX(parent_epd) is exactly equivalent for the ">= 2
distinct" test and needs two running strings per group, so that is the screen;
the expensive per-hash detail query runs only on whatever it flags.

Even that screen does not fit one pass. The monthlies are UNPRUNED, so the
group count is the full below-floor tail (~1e9), and two ~60-byte strings per
group is well over 100 GB of hash table. DuckDB's memory_limit does not bind
on this shape -- a 12GB limit spilled nothing and reached 197 GB of private
bytes -- so the GROUP BY is partitioned into --buckets disjoint slices of
parent_hash instead. Bucketing on a column of the grouping key is exact.

Usage:
    .venv/Scripts/python.exe python/audit_hash_collisions.py \\
        --dir E:/chess/position-stats/_pooled_partials_ge1800_2013_2026_brc/_monthly
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import duckdb

DEFAULT_DIR = ("E:/chess/position-stats/_pooled_partials_ge1800_2013_2026_brc"
               "/_monthly")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", default=DEFAULT_DIR,
                    help="Directory of *.ps.parquet (monthlies or partials).")
    ap.add_argument("--glob", default="*.ps.parquet")
    ap.add_argument("--min-games", type=int, default=50,
                    help="Pool floor. A collision below it never reaches Stage 3.")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--mem", default="16GB")
    ap.add_argument("--buckets", type=int, default=32,
                    help="Hash-partitions for the GROUP BY. One pass over the "
                         "unpruned pool does not fit in RAM; see the loop.")
    ap.add_argument("--tmp", default=None,
                    help="DuckDB spill directory. Default: alongside --dir.")
    ap.add_argument("--show", type=int, default=25)
    a = ap.parse_args()

    d = Path(a.dir)
    files = sorted(d.glob(a.glob))
    if not files:
        print(f"FATAL: no {a.glob} under {d}")
        return 1
    src = f"'{(d / a.glob).as_posix()}'"
    tmp = Path(a.tmp) if a.tmp else d.parent / "_collision_audit_tmp"
    tmp.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.mem}'")
    con.execute(f"SET threads={a.threads}")
    con.execute(f"SET temp_directory='{tmp.as_posix()}'")
    con.execute("SET preserve_insertion_order=false")

    print(f"source : {len(files):,} file(s) under {d}")
    print("test   : same parent_hash, different parent_epd")
    print(f"buckets: {a.buckets} (arithmetic partition on parent_hash)")
    print("", flush=True)

    t0 = time.time()
    # BUCKETED ON PURPOSE (2026-08-25). The previous single GROUP BY over every
    # distinct parent_hash in the UNPRUNED monthlies holds two ~60-byte EPD
    # strings per group across ~1e9 groups. memory_limit did not bind on that
    # shape -- nothing ever spilled to temp_directory -- so it grew to 197 GB of
    # private bytes, drove the pagefile to 100%, and ran 7 h at one core with no
    # output before being killed. Partitioning on a column of the grouping key is
    # exact (every row of a given parent_hash lands in one bucket), which is the
    # remedy CLAUDE.md prescribes; the arithmetic form mirrors
    # build_pooled_stats._bucket_expr so bucket assignment is DuckDB-version
    # stable rather than tied to hash().
    nb_ = a.buckets
    bkt = f"((parent_hash % {nb_}) + {nb_}) % {nb_}"

    # Derive `bad`'s schema from the real query so column types cannot drift.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE bad AS
        SELECT parent_hash,
               MIN(parent_epd) AS epd_lo,
               MAX(parent_epd) AS epd_hi,
               SUM(total)::HUGEINT AS games,
               COUNT(*)::BIGINT   AS rows_
        FROM read_parquet({src})
        WHERE false
        GROUP BY parent_hash
    """)

    n_hash = n_rows = n_games = 0
    for b in range(nb_):
        tb = time.time()
        # The MIN/MAX screen is exact for ">=2 distinct EPDs" and costs two
        # running strings per group instead of a per-group hash set.
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE g AS
            SELECT parent_hash,
                   MIN(parent_epd) AS epd_lo,
                   MAX(parent_epd) AS epd_hi,
                   SUM(total)::HUGEINT AS games,
                   COUNT(*)::BIGINT   AS rows_
            FROM read_parquet({src})
            WHERE {bkt} = {b}
            GROUP BY parent_hash
        """)
        bh, br, bg = con.execute(
            "SELECT COUNT(*), COALESCE(SUM(rows_), 0), COALESCE(SUM(games), 0) "
            "FROM g").fetchone()
        con.execute("INSERT INTO bad SELECT * FROM g WHERE epd_lo <> epd_hi")
        so_far = con.execute("SELECT COUNT(*) FROM bad").fetchone()[0]
        con.execute("DROP TABLE g")
        n_hash += bh
        n_rows += br
        n_games += int(bg)
        # Per-bucket progress: the failure this replaces was silent for 7 hours.
        print(f"  bucket {b:>3}/{nb_}: {bh:>13,} hashes  {br:>14,} rows  "
              f"bad so far {so_far:>6,}  ({(time.time() - tb) / 60:.1f} min)",
              flush=True)

    print("")
    print(f"distinct parent_hash : {n_hash:,}")
    print(f"edge rows            : {n_rows:,}")
    print(f"games (summed)       : {n_games:,}")
    exp = (n_hash ** 2) / float(2 ** 65)
    print(f"birthday expectation : {exp:.3f} collision(s) at this cardinality")
    print("                       (n^2 / 2^65, 64-bit space)")
    print("", flush=True)

    n_bad, bad_games = con.execute(
        "SELECT COUNT(*), COALESCE(SUM(games), 0) FROM bad").fetchone()
    n_bad_floor = con.execute(
        f"SELECT COUNT(*) FROM bad WHERE games >= {a.min_games}").fetchone()[0]

    print(f"COLLIDING HASHES     : {n_bad:,}")
    print(f"  affected games     : {bad_games:,} "
          f"({100.0*bad_games/max(n_games,1):.6f}% of pool mass)")
    print(f"  surviving the >={a.min_games}-game floor : {n_bad_floor:,}"
          f"   <- the ones Stage 3 would actually see")

    if n_bad:
        print(f"\nworst offenders by games (up to {a.show}):")
        rows = con.execute(f"""
            SELECT parent_hash, games, rows_, epd_lo, epd_hi
            FROM bad ORDER BY games DESC LIMIT {a.show}
        """).fetchall()
        for h, g, r, lo, hi in rows:
            print(f"  hash {h:>21}  games {g:>12,}  rows {r:>4}")
            print(f"    {lo}")
            print(f"    {hi}")

    dt = time.time() - t0
    print(f"\nscanned in {dt/60:,.1f} min")
    if n_bad_floor:
        print("\nFAIL — collisions survive the pool floor. Downstream nodes merge "
              "two distinct positions; do not build reps on this pool until the "
              "affected hashes are understood.")
    elif n_bad:
        print("\nPASS (with a tail) — collisions exist only below the floor, so "
              "no Stage-3 node is affected. Consistent with the birthday bound.")
    else:
        print("\nPASS — no hash carries two distinct EPDs anywhere in the pool.")
    print("Reminder: rows are grouped by (parent_hash, move_san) upstream with "
          "any_value(parent_epd), so a collision between positions that only "
          "ever played the SAME move is invisible to this test.")
    return 1 if n_bad_floor else 0


if __name__ == "__main__":
    sys.exit(main())
