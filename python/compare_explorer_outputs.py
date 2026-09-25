"""Compare two producers' explorer outputs: partial dirs, bucketed months, term monthlies.

The explorer book can come from three producers -- the Python extract + EPD
backfill (d79a0c7), the Python extract at --epd-max-ply 30 + bucket_month (B1),
and the Rust explorer-extract -- and a book must come from ONE of them. This is
how any two are held equal before one is trusted, here and on the remote
machines, so it reads only what the files contain and never imports a producer.

    partials A B     two --partial-dir trees, file by file: the same ps/term
                     file names and _DONE sentinels, the same Arrow schemas
                     (large_string included), and per file COUNT(*) plus
                     SUM(hash(every column)) -- a multiset digest, exact
                     including ply. On a mismatch, EXCEPT ALL both ways, <= 20
                     rows each.

    month A B --month Y_M
                     two bucketed months (<dir>/month=Y_M/bkt=i/...): the same
                     bucket dirs, and per bucket every column but ply exact as
                     a multiset. ply is picked differently by each producer
                     (any_value vs MIN), so it is checked for PARITY only,
                     joined on the key, plus optionally:
                       --ply-le            B.ply <= A.ply on every key
                       --min-ply-from DIR  B.ply == MIN(ply) over DIR's ps
                                           partials (use --buckets: it scans
                                           every partial once)

    term A B [--month Y_M]
                     two term monthlies (files, or dirs holding
                     year=Y_month=M.term.parquet): exact, with schemas.

Every difference in `month` is classified:
    quarantine  A's book quarantined the key (A\\_quarantine\\month=Y_M), B kept it
    collision   the key's parent_hash carries two EPDs across A and B: 64-bit
                collision twins, which the A2 merge resolves by EPD
    bug         anything else
Exit status: 0 identical, 2 only quarantine/collision differences, 1 a bug or a
structural mismatch.

DuckDB is throttled for use beside a running pipeline: --threads 2 --mem 4GB,
temp on D: by default (never F:).

Usage:
    python compare_explorer_outputs.py partials E:\\chess\\_pilot_full_202406 E:\\chess\\ab_rust\\r16_202406
    python compare_explorer_outputs.py month D:\\chess\\_epd_pilot E:\\chess\\ab_rust\\month --month 2024_6 --ply-le
    python compare_explorer_outputs.py term E:\\chess\\_pilot_full_202406\\_monthly E:\\chess\\ab_rust\\term --month 2024_6
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PS_COLS = ("parent_hash", "move_san", "event", "elo_band", "parent_epd",
           "child_hash", "child_eval", "ply", "white_wins", "draws",
           "black_wins", "total")
TERM_COLS = ("position_hash", "kind", "reason", "white_wins", "draws",
             "black_wins", "total")
KEY = ("parent_hash", "move_san", "event", "elo_band")
MONTH_BUCKETS = 512
SHOW = 20
# A classifier that fetched every differing row of a truly broken bucket could
# pull millions of rows; past this many it reports the count and samples.
CLASSIFY_CAP = 200_000
DEFAULT_TMP = Path("D:/chess_duckdb_tmp_compare")


def _p(path: Path) -> str:
    return str(path).replace("\\", "/")


def connect(threads: int, mem: str, tmp: Path) -> duckdb.DuckDBPyConnection:
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads={threads}")
    con.execute(f"SET memory_limit='{mem}'")
    con.execute(f"SET temp_directory='{_p(tmp)}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    return con


def _src(files) -> str:
    if isinstance(files, Path):
        return f"read_parquet('{_p(files)}')"
    return "read_parquet([" + ", ".join(f"'{_p(f)}'" for f in files) + "])"


def digest(con, files, cols) -> tuple[int, int]:
    """(rows, SUM(hash(cols))) -- equal multisets give equal digests."""
    n, h = con.execute(f"SELECT COUNT(*), COALESCE(SUM(hash({', '.join(cols)}))::HUGEINT, 0) "
                       f"FROM {_src(files)}").fetchone()
    return int(n), int(h)


def except_all(con, a, b, cols, limit: int | None = SHOW) -> list[tuple]:
    lim = f"LIMIT {limit}" if limit else ""
    c = ", ".join(cols)
    return con.execute(f"SELECT {c} FROM {_src(a)} EXCEPT ALL "
                       f"SELECT {c} FROM {_src(b)} {lim}").fetchall()


def schema_diff(a: Path, b: Path) -> str | None:
    sa, sb = pq.read_schema(a), pq.read_schema(b)
    if sa.equals(sb):
        return None
    fa = [f"{f.name}:{f.type}{'' if f.nullable else ' not null'}" for f in sa]
    fb = [f"{f.name}:{f.type}{'' if f.nullable else ' not null'}" for f in sb]
    return f"A {fa}\n      B {fb}"


def show_rows(label: str, rows: list[tuple], cols) -> None:
    print(f"    {label} ({len(rows)} shown):")
    for r in rows[:SHOW]:
        print("      " + ", ".join(f"{c}={v!r}" for c, v in zip(cols, r)))


# ── partials ──────────────────────────────────────────────────────────────────

def cmd_partials(a: Path, b: Path, kinds: list[str], con) -> int:
    t0 = time.time()
    bad = 0
    for kind in kinds:
        cols = PS_COLS if kind == "ps" else TERM_COLS
        na = {f.name for f in a.glob(f"*.{kind}.parquet")}
        nb = {f.name for f in b.glob(f"*.{kind}.parquet")}
        only_a, only_b = sorted(na - nb), sorted(nb - na)
        print(f"[{kind}] A {len(na):,} files, B {len(nb):,} files")
        if only_a or only_b:
            bad += 1
            print(f"  FAIL  file names differ: {len(only_a)} only in A {only_a[:5]}, "
                  f"{len(only_b)} only in B {only_b[:5]}")
        n_ok = n_rows = 0
        for name in sorted(na & nb):
            fa, fb = a / name, b / name
            sd = schema_diff(fa, fb)
            if sd:
                bad += 1
                print(f"  FAIL  {name}: schema\n      {sd}")
                continue
            da, db = digest(con, fa, cols), digest(con, fb, cols)
            if da == db:
                n_ok += 1
                n_rows += da[0]
                continue
            bad += 1
            print(f"  FAIL  {name}: A {da[0]:,} rows, B {db[0]:,} rows, digests "
                  f"{'equal' if da[1] == db[1] else 'differ'}")
            show_rows("in A, not B", except_all(con, fa, fb, cols), cols)
            show_rows("in B, not A", except_all(con, fb, fa, cols), cols)
        print(f"  {n_ok:,}/{len(na & nb):,} common files identical ({n_rows:,} rows, "
              f"all {len(cols)} columns)")
    sa = {f.name for f in a.glob("_*.DONE")}
    sb = {f.name for f in b.glob("_*.DONE")}
    if sa != sb:
        bad += 1
        print(f"  FAIL  sentinels differ: {sorted(sa - sb)[:5]} only in A, "
              f"{sorted(sb - sa)[:5]} only in B")
    else:
        print(f"[sentinels] the same {len(sa):,}")
    print(f"\n{'IDENTICAL' if not bad else f'{bad} MISMATCHES'} ({time.time()-t0:,.0f}s)")
    return 0 if not bad else 1


# ── month ─────────────────────────────────────────────────────────────────────

def _buckets(root: Path, tag: str) -> dict[int, list[Path]]:
    out: dict[int, list[Path]] = {}
    for d in (root / f"month={tag}").glob("bkt=*"):
        out[int(d.name.split("=")[1])] = sorted(d.glob("*.parquet"))
    return out


def _bucket_sql(col: str, n: int) -> str:
    return f"((({col}) % {n}) + {n}) % {n}"


def classify(con, a_files, b_files, q_files, cols) -> dict[str, list[tuple]]:
    """Every row in one side only, labelled quarantine / collision / bug."""
    only_a = except_all(con, a_files, b_files, cols, CLASSIFY_CAP)
    only_b = except_all(con, b_files, a_files, cols, CLASSIFY_CAP)
    ki = [cols.index(k) for k in KEY]
    ei, hi = cols.index("parent_epd"), cols.index("parent_hash")
    q_keys: set[tuple] = set()
    if q_files:
        q_keys = set(con.execute(f"SELECT {', '.join(KEY)} FROM {_src(q_files)}").fetchall())
    # EPDs per hash, over both sides, for the hashes that differ at all.
    hashes = sorted({r[hi] for r in only_a + only_b})
    epds: dict[int, set] = {}
    if hashes:
        con.register("dh", pa.table({"h": pa.array(hashes, pa.int64())}))
        for side in (a_files, b_files):
            for h, e in con.execute(f"SELECT DISTINCT parent_hash, parent_epd FROM "
                                    f"{_src(side)} SEMI JOIN dh ON parent_hash = dh.h"
                                    ).fetchall():
                epds.setdefault(h, set()).add(e)
        con.unregister("dh")
    out: dict[str, list[tuple]] = {"quarantine": [], "collision": [], "bug": []}
    for side, rows in (("A", only_a), ("B", only_b)):
        for r in rows:
            key = tuple(r[i] for i in ki)
            if key in q_keys:
                why = "quarantine"
            elif len(epds.get(r[hi], ())) > 1:
                why = "collision"
            else:
                why = "bug"
            out[why].append((side,) + r)
    return out


def cmd_month(a: Path, b: Path, tag: str, con, ply_le: bool, min_from: Path | None,
              only: list[int] | None, nbuckets: int = MONTH_BUCKETS) -> int:
    t0 = time.time()
    ba, bb = _buckets(a, tag), _buckets(b, tag)
    if not ba or not bb:
        print(f"FATAL: no month={tag} buckets under {a if not ba else b}")
        return 1
    bad = explained = 0
    if set(ba) != set(bb):
        bad += 1
        print(f"  FAIL  bucket dirs differ: {sorted(set(ba) - set(bb))[:10]} only in A, "
              f"{sorted(set(bb) - set(ba))[:10]} only in B")
    common = sorted(set(ba) & set(bb))
    if only:
        common = [i for i in common if i in set(only)]
    sa, sb = pq.read_schema(ba[common[0]][0]), pq.read_schema(bb[common[0]][0])
    if not sa.equals(sb):
        bad += 1
        print(f"  FAIL  bucket schema\n      {schema_diff(ba[common[0]][0], bb[common[0]][0])}")
    q_files = sorted((a / "_quarantine" / f"month={tag}").rglob("*.parquet"))
    cols = tuple(c for c in PS_COLS if c != "ply")
    kj = " AND ".join(f"x.{k} = y.{k}" for k in KEY)
    mp = None
    if min_from is not None:
        parts = sorted(min_from.glob("*.ps.parquet"))
        if not parts:
            print(f"FATAL: no *.ps.parquet under {min_from}")
            return 1
        print(f"  MIN(ply) reference: {len(parts):,} partials, buckets {common[:8]}"
              f"{'...' if len(common) > 8 else ''}", flush=True)
        con.execute(f"""
            CREATE OR REPLACE TEMP TABLE mp AS
            SELECT {', '.join(KEY)}, MIN(ply) AS mp
            FROM {_src(parts)}
            WHERE {_bucket_sql('parent_hash', nbuckets)} IN ({', '.join(map(str, common))})
            GROUP BY {', '.join(KEY)}
        """)
        mp = nbuckets
    tot = {"rows_a": 0, "rows_b": 0, "parity": 0, "b_gt_a": 0, "b_lt_a": 0,
           "joined": 0, "min_bad": 0, "min_missing": 0}
    classes = {"quarantine": 0, "collision": 0, "bug": 0}
    samples: dict[str, list] = {"quarantine": [], "collision": [], "bug": []}
    for i in common:
        fa, fb = ba[i], bb[i]
        da, db = digest(con, fa, cols), digest(con, fb, cols)
        tot["rows_a"] += da[0]
        tot["rows_b"] += db[0]
        if da != db:
            cl = classify(con, fa, fb, q_files, cols)
            for k, v in cl.items():
                classes[k] += len(v)
                samples[k].extend(v[: SHOW - len(samples[k])])
        p = con.execute(f"""
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE x.ply % 2 <> y.ply % 2),
                   COUNT(*) FILTER (WHERE y.ply > x.ply),
                   COUNT(*) FILTER (WHERE y.ply < x.ply)
            FROM {_src(fa)} x JOIN {_src(fb)} y ON {kj}
        """).fetchone()
        tot["joined"] += p[0]
        tot["parity"] += p[1]
        tot["b_gt_a"] += p[2]
        tot["b_lt_a"] += p[3]
        if mp is not None:
            m = con.execute(f"""
                SELECT COUNT(*) FILTER (WHERE x.mp IS NULL),
                       COUNT(*) FILTER (WHERE x.mp IS NOT NULL AND x.mp <> y.ply)
                FROM {_src(fb)} y LEFT JOIN mp x ON {kj}
            """).fetchone()
            tot["min_missing"] += m[0]
            tot["min_bad"] += m[1]
    print(f"[month {tag}] {len(common)} buckets compared: A {tot['rows_a']:,} rows, "
          f"B {tot['rows_b']:,} rows")
    if classes["bug"]:
        bad += 1
    explained = classes["quarantine"] + classes["collision"]
    print(f"  rows (every column but ply): "
          + ("IDENTICAL" if not any(classes.values()) else
             f"{classes['bug']:,} bug, {classes['collision']:,} collision, "
             f"{classes['quarantine']:,} quarantine"))
    for k in ("bug", "collision", "quarantine"):
        if samples[k]:
            show_rows(k, samples[k], ("side",) + cols)
    print(f"  ply: {tot['joined']:,} keys joined; parity differs on {tot['parity']:,}; "
          f"B>A on {tot['b_gt_a']:,}, B<A on {tot['b_lt_a']:,}")
    if tot["parity"]:
        bad += 1
    if ply_le:
        print(f"  --ply-le: {'PASS' if not tot['b_gt_a'] else 'FAIL'}")
        bad += bool(tot["b_gt_a"])
    if mp is not None:
        ok = not tot["min_bad"] and not tot["min_missing"]
        print(f"  --min-ply-from: B.ply == MIN over the partials on every key: "
              f"{'PASS' if ok else 'FAIL'} ({tot['min_bad']:,} differ, "
              f"{tot['min_missing']:,} keys absent from the partials)")
        bad += not ok
    verdict = ("IDENTICAL (ply excepted)" if not bad and not explained else
               f"{bad} MISMATCHES" if bad else
               f"EXPLAINED DIFFERENCES ONLY ({explained:,} rows)")
    print(f"\n{verdict} ({time.time()-t0:,.0f}s)")
    return 1 if bad else (2 if explained else 0)


# ── term ──────────────────────────────────────────────────────────────────────

def _term_file(p: Path, tag: str | None) -> Path:
    if p.is_file():
        return p
    if not tag:
        raise SystemExit(f"FATAL: {p} is a directory; pass --month Y_M")
    y, m = tag.split("_")
    f = p / f"year={int(y)}_month={int(m)}.term.parquet"
    if not f.exists():
        raise SystemExit(f"FATAL: no {f.name} in {p}")
    return f


def cmd_term(a: Path, b: Path, tag: str | None, con) -> int:
    t0 = time.time()
    fa, fb = _term_file(a, tag), _term_file(b, tag)
    bad = 0
    sd = schema_diff(fa, fb)
    if sd:
        bad += 1
        print(f"  FAIL  schema\n      {sd}")
    da, db = digest(con, fa, TERM_COLS), digest(con, fb, TERM_COLS)
    if da != db:
        bad += 1
        print(f"  FAIL  A {da[0]:,} rows, B {db[0]:,} rows")
        show_rows("in A, not B", except_all(con, fa, fb, TERM_COLS), TERM_COLS)
        show_rows("in B, not A", except_all(con, fb, fa, TERM_COLS), TERM_COLS)
    else:
        print(f"[term] {da[0]:,} rows identical on all {len(TERM_COLS)} columns")
    print(f"\n{'IDENTICAL' if not bad else f'{bad} MISMATCHES'} ({time.time()-t0:,.0f}s)")
    return 0 if not bad else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["partials", "month", "term"])
    ap.add_argument("a", type=Path)
    ap.add_argument("b", type=Path)
    ap.add_argument("--month", default=None, metavar="Y_M")
    ap.add_argument("--kinds", nargs="+", default=["ps", "term"])
    ap.add_argument("--ply-le", action="store_true",
                    help="month: also require B.ply <= A.ply on every key.")
    ap.add_argument("--min-ply-from", type=Path, default=None, metavar="PARTIAL_DIR",
                    help="month: also require B.ply == MIN(ply) over these partials.")
    ap.add_argument("--buckets", type=int, nargs="*", default=None,
                    help="month: compare only these buckets.")
    ap.add_argument("--nbuckets", type=int, default=MONTH_BUCKETS,
                    help="month: the months' bucket count, for --min-ply-from's "
                         "bucket filter (default 512, the explorer book's).")
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--mem", default="4GB")
    ap.add_argument("--tmp-dir", type=Path, default=None)
    a = ap.parse_args()
    tmp = a.tmp_dir or (DEFAULT_TMP if DEFAULT_TMP.drive and Path(DEFAULT_TMP.drive + "/").exists()
                        else Path.cwd() / "_compare_duckdb_tmp")
    if str(tmp).upper().startswith("F:"):
        print("FATAL: never put DuckDB temp on F: (USB spinning disk)")
        return 1
    for p in (a.a, a.b):
        if not p.exists():
            print(f"FATAL: {p} does not exist")
            return 1
    con = connect(a.threads, a.mem, tmp)
    print(f"A: {a.a}\nB: {a.b}\nduckdb: {a.threads} threads, {a.mem}, temp {tmp}\n",
          flush=True)
    try:
        if a.mode == "partials":
            return cmd_partials(a.a, a.b, a.kinds, con)
        if a.mode == "month":
            if not a.month or not re.fullmatch(r"\d{4}_\d{1,2}", a.month):
                print("FATAL: month needs --month Y_M")
                return 1
            y, m = a.month.split("_")
            return cmd_month(a.a, a.b, f"{int(y)}_{int(m)}", con, a.ply_le,
                             a.min_ply_from, a.buckets, a.nbuckets)
        return cmd_term(a.a, a.b, a.month, con)
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
