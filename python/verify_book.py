"""Verify an explorer book (`explorer-extract merge`) against its months, independently of the tool.

Nothing here shares code with the Rust merge: DuckDB's own hash for the
digests, python-chess (zobrist.py) for positions, and the book tree as found on
disk rather than as the tool's sentinels list it. The months are the ones the
book's settings lock (_merge_params.json) names.

    python verify_book.py <book> <months-root> [--digest-buckets SPEC] [--dup-buckets SPEC]
                          [--sums] [--scan] [--collisions] [--sample [N]]

  --digest-buckets SPEC  per bucket, over the months' bkt=i files and the
                         book's ps/*/*/bkt{i:03d}.parquet:
                         SUM(hash(parent_hash, parent_epd, move_san, event,
                         elo_band, ply, child_hash)::HUGEINT * c) for each of
                         the 4 counts, and again with a salted hash. Equal on
                         both sides. (Reads ~7.7 GB of F: per bucket at full
                         scale.)
  --dup-buckets SPEC     GROUP BY the 6-column key HAVING COUNT(*) > 1 returns
                         nothing (spills; ~176M groups per bucket at scale).
  --sums                 book-wide sums of the 4 counts, and SUM(total) at
                         ply 1, against the months' manifests (5 columns
                         read). Needs a complete book.
  --scan                 every book row: no NULLs, the EPD's side to move
                         against ply parity, total = W + D + B >= 1, and
                         white_score_avg within 1e-12 of the formula; every
                         file's event/elo_band/bucket equal to its path. One
                         query per bucket, aggregated per file (12.3M rows/s
                         at 6 threads on the pilot: ~2 h for the full book).

Each bucket gets a fresh DuckDB connection: in one long-lived connection the
per-bucket time decayed 16 s -> 91 s over 13 buckets (CLAUDE.md, DuckDB decay).
  --collisions           _collisions: every EPD hashes to its parent_hash, every
                         hash has >= 2 EPDs, and the months' own collision
                         records (_conflicts, kind parent-epd) are a subset.
  --sample [N]           N book rows (default 1,000,000) spread over up to 16
                         buckets: zobrist(Board(parent_epd)) == parent_hash,
                         pushing move_san gives child_hash, and the side to
                         move matches ply parity.

SPEC is bucket numbers and ranges: 0-7, or 0,17,511. With no check named, runs
--collisions --scan --sample, plus --sums on a complete book. Exit 0 if every
check run passes.

A position whose en-passant capture is pseudo-legal but illegal (a pinned
pawn) hashes WITH its ep file (polyglot's rule) while its EPD omits it
(python-chess prints only legal ep squares). The hash test tries the ep file
back on such EPDs and counts those rows separately; they are not failures.

A book-v2 or book-v3 (the lock's schema; months built with `month --key128`) carries the
128-bit key: parent_hash_hi / child_hash_hi beside the 64-bit columns. Then the
digests cover both halves of both keys, the unique key is (parent_hash,
parent_hash_hi, move_san, event, elo_band, ply), a key must have one EPD (and an
EPD one key within its 64-bit hash), the
ply cap is the months' --max-ply, and the sample holds both halves to
zobrist128.py. _collisions is still the list of 64-bit hashes with two or more
positions; such months have no conflict records to compare it with.

DuckDB is throttled (2 threads, 4 GB) with temp on D:\\chess_duckdb_tmp, never F:.
"""
from __future__ import annotations

from explorer_storage import relation
import argparse
import json
import math
import random
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import chess
import duckdb
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from zobrist import zobrist_int64  # noqa: E402
from zobrist128 import zobrist128  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

BUCKETS = 512
DEFAULT_TMP = Path("D:/chess_duckdb_tmp")
KEY_TUPLE = "parent_hash, parent_epd, move_san, event, elo_band, ply, child_hash"
KEY_TUPLE_WIDE = KEY_TUPLE + ", parent_hash_hi, child_hash_hi"
COUNTS = ("white_wins", "draws", "black_wins", "total")
SALT = "verify_book salt 2"
SHOW = 10

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> bool:
    _checks.append((bool(ok), label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}", flush=True)
    return bool(ok)


def _p(path) -> str:
    return str(path).replace("\\", "/")


def _lit(files) -> str:
    return "[" + ", ".join("'" + _p(f).replace("'", "''") + "'" for f in files) + "]"


def parse_buckets(spec: str) -> list[int]:
    out: set[int] = set()
    for tok in (t.strip() for t in re.split(r"[,\s]+", spec)):
        if not tok:
            continue
        a, _, b = tok.partition("-")
        lo, hi = int(a), int(b or a)
        if not 0 <= lo <= hi < BUCKETS:
            raise SystemExit(f"FATAL: bucket spec {tok!r} is not within 0-{BUCKETS - 1}")
        out.update(range(lo, hi + 1))
    return sorted(out)


_CONNECT_ARGS: tuple = ()


def connect(threads: int, mem: str, tmp: Path) -> duckdb.DuckDBPyConnection:
    global _CONNECT_ARGS
    _CONNECT_ARGS = (threads, mem, tmp)
    if _p(tmp).upper().startswith("F:"):
        raise SystemExit("FATAL: never put DuckDB temp on F: (USB spinning disk)")
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads={threads}")
    con.execute(f"SET memory_limit='{mem}'")
    con.execute(f"SET temp_directory='{_p(tmp)}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    return con


class Book:
    def __init__(self, book: Path, root: Path):
        self.book, self.root = book, root
        lock = book / "_merge_params.json"
        if not lock.exists():
            raise SystemExit(f"FATAL: {lock} not found: not a merged book")
        self.lock = json.loads(lock.read_text(encoding="utf-8"))
        self.months: list[str] = self.lock["months"]
        schema = self.lock.get("schema", "book-v1")
        self.schema = schema
        if schema not in ("book-v1", "book-v2", "book-v3", "book-v4"):
            raise SystemExit(f"FATAL: {lock} says schema {schema!r}: expected book-v1 through book-v4")
        self.wide = schema in ("book-v2", "book-v3", "book-v4")
        self.max_ply = int(self.lock.get("input_params", {}).get("max_ply", 30))
        if Path(self.lock["months_root"]).resolve() != root.resolve():
            print(f"  note: the book was built from {self.lock['months_root']}; reading {root}")
        self.ps: dict[int, list[Path]] = {}
        for f in sorted((book / "ps").glob("event=*/elo_band=*/*.parquet")):
            m = re.fullmatch(r"bkt(\d{3})\.parquet", f.name)
            if not m:
                raise SystemExit(f"FATAL: unexpected file {f}")
            self.ps.setdefault(int(m.group(1)), []).append(f)
        self.done = sorted(int(p.stem[3:]) for p in (book / "_done").glob("bkt*.DONE"))
        self.complete = (book / "_BOOK.DONE").exists() or (
            len(self.done) == BUCKETS and (book / "_done" / "term.DONE").exists())

    def month_file(self, tag: str, b: int) -> Path:
        return self.root / f"month={tag}" / f"bkt={b}" / "part-0000.parquet"

    def all_ps(self) -> list[Path]:
        return [f for b in sorted(self.ps) for f in self.ps[b]]


# ── digests and duplicates ────────────────────────────────────────────────────

def _digest(con, files: list[Path], wide: bool = False, *, key: str | None = None) -> tuple:
    if not files:
        return (0,) * 9
    if key is None:
        key = KEY_TUPLE_WIDE if wide else KEY_TUPLE
    parts = [f"COALESCE(SUM(hash({key})::HUGEINT * {c}), 0)" for c in COUNTS]
    parts += [f"COALESCE(SUM(hash({key}, '{SALT}')::HUGEINT * {c}), 0)" for c in COUNTS]
    row = con.execute(f"SELECT COALESCE(SUM(total), 0), {', '.join(parts)} FROM {relation(files)}").fetchone()
    return tuple(int(x) for x in row)


def check_digests(con, bk: Book, buckets: list[int]) -> None:
    print(f"\ndigest identity, months vs book, {len(buckets)} bucket(s)", flush=True)
    bad, n = [], 0
    t0 = time.time()
    for b in buckets:
        ins = [p for t in bk.months if (p := bk.month_file(t, b)).exists()]
        outs = bk.ps.get(b, [])
        if b not in bk.done:
            if outs:
                bad.append(f"bucket {b}: {len(outs)} book files but no sentinel")
            continue
        n += 1
        bcon = connect(*_CONNECT_ARGS)   # per bucket: DuckDB decays in a long-lived connection
        di, do = _digest(bcon, ins, bk.wide), _digest(bcon, outs, bk.wide)
        bcon.close()
        if di != do:
            bad.append(f"bucket {b}: months {di[:2]}.. vs book {do[:2]}..")
        print(f"    bkt {b:03d}: {len(ins)} month files, {len(outs)} book files, total "
              f"{di[0]:,}: {'equal' if di == do else 'DIFFERENT'} ({time.time() - t0:,.0f}s)", flush=True)
    for s in bad[:SHOW]:
        print(f"    {s}")
    check(not bad and n > 0, f"the 8 hash-weighted sums (two salts x 4 counts{', both key halves' if bk.wide else ''}) "
                             f"are equal in {n} bucket(s) with sentinels")


def check_term_digest(con, bk: Book) -> None:
    if bk.schema not in ("book-v3", "book-v4"):
        raise ValueError("the sliced terminal digest requires sliced terminals")
    meta = json.loads((bk.book / "_book.meta.json").read_text(encoding="utf-8"))
    events = ["Blitz", "Bullet", "Classical", "Correspondence", "Rapid", "UltraBullet"]
    if meta.get("term_event_ids") != events:
        raise ValueError("unexpected terminal event ID mapping")
    key = "position_hash, position_hash_hi, kind, reason, end_ply, event_id, elo_band"
    inputs = []
    for month in bk.months:
        year, number = month.split("_")
        inputs.append(bk.root / "_term" / f"year={year}_month={number}.term.parquet")
    outputs = sorted((bk.book / "term").glob("*.parquet"))
    if outputs != [bk.book / "term" / f"bkt{b:03d}.parquet" for b in range(BUCKETS)]:
        raise ValueError("expected exactly 512 terminal bucket files")
    source = _digest(con, inputs, key=key)
    merged = _digest(con, outputs, key=key)
    check(source == merged, f"terminal counts and sliced key digests agree over {len(inputs)} months ({source[0]:,} outcomes)")


def parse_dup_shard(value: str) -> tuple[int, int]:
    try:
        index, count = map(int, value.split("/"))
        if count < 1 or count > 256 or count & (count - 1) or not 0 <= index < count:
            raise ValueError()
    except ValueError:
        raise argparse.ArgumentTypeError("duplicate shard must be INDEX/COUNT; COUNT is a power of two <=256")
    return index, count


def shard_source(source: str, shard: tuple[int, int] | None) -> str:
    if shard is None: return source
    index, count = shard
    parse_dup_shard(f"{index}/{count}")
    width = (1 << 64) // count
    low = -(1 << 63) + index * width
    high = low + width - 1
    # Every checked grouping contains parent_hash, so a group cannot straddle a
    # boundary. Include NULL in the first slice to preserve the unsliced query.
    predicate = f"parent_hash BETWEEN {low} AND {high}"
    if index == 0: predicate += " OR parent_hash IS NULL"
    return f"(SELECT * FROM {source} WHERE {predicate})"


def duplicate_counts(con, source: str, wide: bool):
    pos = "parent_hash, parent_hash_hi" if wide else "parent_hash, parent_epd"
    r = con.execute(f"SELECT COUNT(*) FILTER (WHERE n > 1), SUM(n) FROM ("
                    f"SELECT COUNT(*) AS n FROM {source} "
                    f"GROUP BY {pos}, move_san, event, elo_band, ply)").fetchone()
    return int(r[0] or 0), int(r[1] or 0)


def mapping_conflicts(con, source: str, key: str, value: str) -> int:
    # COUNT(DISTINCT text) keeps a second high-cardinality aggregate state. We only
    # need to know whether any two nonnull values differ: MIN != MAX is exact,
    # spillable, and the two mapping directions run sequentially.
    return int(con.execute(f"SELECT COUNT(*) FROM (SELECT 1 FROM {source} "
                           f"GROUP BY {key} HAVING MIN({value}) != MAX({value}))").fetchone()[0])


def check_dups(con, bk: Book, buckets: list[int], shard=None) -> None:
    print(f"\nduplicate keys, {len(buckets)} bucket(s), signed-low-key shard {shard or 'all'}", flush=True)
    dup, rows, two = 0, 0, 0
    for b in buckets:
        outs = bk.ps.get(b, [])
        if not outs:
            continue
        bcon = connect(*_CONNECT_ARGS)   # per bucket: DuckDB decays in a long-lived connection
        source = shard_source(relation(outs), shard)
        started = time.monotonic()
        print(f"    bkt {b:03d}: counting duplicate move keys", flush=True)
        r = duplicate_counts(bcon, source, bk.wide)
        print(f"    bkt {b:03d}: move keys checked, {r[1]:,} rows ({time.monotonic()-started:.1f}s)", flush=True)
        t = 0
        if bk.wide:
            # A 128-bit key is a position: one EPD per key, and within a 64-bit hash one key
            # per EPD. (Across hashes one EPD can have two keys: a pinned en-passant capture
            # hashes with its ep file, which the EPD cannot show.)
            for key, value in (("parent_hash, parent_hash_hi", "parent_epd"),
                               ("parent_hash, parent_epd", "parent_hash_hi")):
                bcon.close()
                bcon = connect(*_CONNECT_ARGS)
                started = time.monotonic()
                print(f"    bkt {b:03d}: mapping {key} -> {value}", flush=True)
                t += mapping_conflicts(bcon, source, key, value)
                print(f"    bkt {b:03d}: mapping checked ({time.monotonic()-started:.1f}s)", flush=True)
        bcon.close()
        dup += int(r[0] or 0)
        rows += int(r[1] or 0)
        two += int(t or 0)
        print(f"    bkt {b:03d}: {int(r[1] or 0):,} rows, {int(r[0] or 0)} duplicated keys"
              f"{f', {int(t or 0)} keys or EPDs naming two positions' if bk.wide else ''}", flush=True)
    check(dup == 0, f"the 6-column key is unique ({rows:,} rows)")
    if bk.wide:
        check(two == 0, "every 128-bit key has one EPD, and every EPD one key within its 64-bit hash")


# ── book-wide ─────────────────────────────────────────────────────────────────

def check_sums(con, bk: Book) -> None:
    print("\nbook-wide sums vs the months' manifests", flush=True)
    if not bk.complete:
        print(f"  SKIP  the book is incomplete ({len(bk.done)}/{BUCKETS} buckets)")
        return
    mans = [bk.root / "_manifest" / f"month={t}.parquet" for t in bk.months]
    m = con.execute(f"SELECT SUM(total), SUM(white_wins), SUM(draws), SUM(black_wins), "
                    f"SUM(ply1_games) FROM read_parquet({_lit(mans)})").fetchone()
    b = con.execute(f"SELECT SUM(total), SUM(white_wins), SUM(draws), SUM(black_wins), "
                    f"SUM(total) FILTER (WHERE ply = 1) FROM read_parquet({_lit(bk.all_ps())}, "
                    f"hive_partitioning=false)").fetchone()
    m, b = tuple(int(x or 0) for x in m), tuple(int(x or 0) for x in b)
    print(f"    manifests: total {m[0]:,} W {m[1]:,} D {m[2]:,} B {m[3]:,} ply-1 {m[4]:,}")
    print(f"    book:      total {b[0]:,} W {b[1]:,} D {b[2]:,} B {b[3]:,} ply-1 {b[4]:,}")
    check(m == b, f"the book's 4 sums and ply-1 total equal the {len(mans)} manifests'")


SCAN_NULLS = " OR ".join(f"{c} IS NULL" for c in (
    "parent_hash", "move_san", "event", "elo_band", "parent_epd", "child_hash", "ply",
    "white_wins", "draws", "black_wins", "total", "white_score_avg"))


def _scan_bucket(job: tuple) -> list:
    """One bucket's scan query, run in a fresh child process."""
    files, threads, mem, tmp, max_ply, wide = job
    bcon = connect(threads, mem, Path(tmp))
    nulls = SCAN_NULLS + (" OR parent_hash_hi IS NULL OR child_hash_hi IS NULL" if wide else "")
    rows = bcon.execute(f"""
        SELECT filename, COUNT(*),
               COUNT(*) FILTER (WHERE {nulls}),
               COUNT(*) FILTER (WHERE split_part(parent_epd, ' ', 2) NOT IN ('w', 'b')
                                   OR (split_part(parent_epd, ' ', 2) = 'w') <> (ply % 2 = 1)),
               COUNT(*) FILTER (WHERE total <> white_wins + draws + black_wins OR total < 1
                                   OR white_wins < 0 OR draws < 0 OR black_wins < 0),
               COUNT(*) FILTER (WHERE abs(white_score_avg - (white_wins::DOUBLE + 0.5::DOUBLE
                                          * draws::DOUBLE) / total::DOUBLE) > 1e-12),
               COUNT(*) FILTER (WHERE ply < 1 OR ply > {int(max_ply)}),
               MIN(event), MAX(event), MIN(elo_band), MAX(elo_band),
               MIN(((parent_hash % {BUCKETS}) + {BUCKETS}) % {BUCKETS}),
               MAX(((parent_hash % {BUCKETS}) + {BUCKETS}) % {BUCKETS})
        FROM {relation(files, filename=True)}
        GROUP BY filename""").fetchall()
    bcon.close()
    return rows


def check_scan(con, bk: Book) -> None:
    """One query per bucket, aggregated per file: the row checks count
    violations, and each file's event, elo_band and bucket come back as
    MIN/MAX to be held against its path here. Each bucket runs in a fresh
    child process (max_tasks_per_child=1). A fresh connection per bucket in
    one process was not enough: after the full run's digest, dup and sums
    checks, the scan crawled at 700-1,000 s per bucket on one core (2026-09-30)
    against 14 s in the pilot -- CLAUDE.md's DuckDB decay is process-wide. One
    query over every file ran about 4 h at one core."""
    files = bk.all_ps()
    print(f"\nbook-wide scan: {len(files):,} files in {len(bk.ps)} buckets", flush=True)
    if not files:
        check(False, "the book has ps files")
        return
    tot = {"rows": 0, "nulls": 0, "parity": 0, "counts": 0, "wsa": 0, "ply": 0, "path": 0, "bucket": 0}
    bad_files: list[str] = []
    t0 = time.time()
    buckets = sorted(bk.ps)
    threads, mem, tmp = _CONNECT_ARGS
    jobs = [(bk.ps[b], threads, mem, str(tmp), bk.max_ply, bk.wide) for b in buckets]
    pool = ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1)
    tb = time.time()
    for b, rows in zip(buckets, pool.map(_scan_bucket, jobs)):
        n_b = 0
        for fn, n, nul, par, cnt, wsa, ply, ev0, ev1, bd0, bd1, k0, k1 in rows:
            m = re.search(r"event=([^/\\]+)[/\\]elo_band=(-?\d+)[/\\]bkt(\d{3})\.parquet$", fn)
            path_ok = bool(m) and ev0 == ev1 == m.group(1) and bd0 == bd1 == int(m.group(2))
            bkt_ok = bool(m) and k0 == k1 == int(m.group(3)) == b
            for k, v in (("rows", n), ("nulls", nul), ("parity", par), ("counts", cnt), ("wsa", wsa),
                         ("ply", ply), ("path", 0 if path_ok else n), ("bucket", 0 if bkt_ok else n)):
                tot[k] += int(v or 0)
            if nul or par or cnt or wsa or ply or not path_ok or not bkt_ok:
                bad_files.append(fn)
            n_b += int(n)
        if len(rows) != len(bk.ps[b]):
            tot["path"] += 1
            bad_files.append(f"bucket {b}: {len(rows)} files read of {len(bk.ps[b])}")
        dt = time.time() - tb
        print(f"    bkt {b:03d}: {len(rows)} files, {n_b:,} rows, {dt:,.0f}s "
              f"({n_b / max(dt, 1e-9) / 1e6:,.1f}M rows/s)", flush=True)
        tb = time.time()
    pool.shutdown()
    el = time.time() - t0
    print(f"    {tot['rows']:,} rows in {el:,.0f}s ({tot['rows'] / max(el, 1e-9) / 1e6:,.1f}M rows/s): "
          f"NULLs {tot['nulls']}, parity {tot['parity']}, counts {tot['counts']}, white_score_avg "
          f"{tot['wsa']}, ply range {tot['ply']}, rows in a file whose path disagrees {tot['path']}, "
          f"rows in a file outside its bucket {tot['bucket']} {bad_files[:3]}")
    check(tot["rows"] > 0 and not any(v for k, v in tot.items() if k != "rows"),
          "every book row: no NULLs, parity, total = W + D + B, white_score_avg, ply; every file's "
          "event, elo_band and bucket match its path")


# ── positions (python-chess) ──────────────────────────────────────────────────

def _key(board: chess.Board, wide: bool):
    return zobrist128(board) if wide else zobrist_int64(board)


def hash_matches(board: chess.Board, h, wide: bool = False) -> tuple[bool, bool]:
    """(matches, via an illegal ep square the EPD cannot show). `h` is the 64-bit hash, or
    with `wide` the (low, high) pair of the 128-bit key."""
    if _key(board, wide) == h:
        return True, False
    if board.ep_square is None:
        rank = 5 if board.turn == chess.WHITE else 2
        for f in range(8):
            b2 = board.copy(stack=False)
            b2.ep_square = chess.square(f, rank)
            if _key(b2, wide) == h:
                return True, True
    return False, False


def _check_rows(rows: list[tuple]) -> tuple[int, int, dict, list]:
    """(rows, ep-illegal rows, failures by kind, examples)."""
    fails: dict[str, int] = {}
    ex: list[str] = []
    ep = 0
    for row in rows:
        h, epd, san, child, ply = row[:5]
        wide = len(row) == 7
        if wide:
            h, child = (h, row[5]), (child, row[6])
        why = None
        try:
            board = chess.Board(epd)
        except ValueError:
            why = "epd"
        if why is None:
            ok, via_ep = hash_matches(board, h, wide)
            ep += via_ep
            if not ok:
                why = "hash"
            elif (board.turn == chess.WHITE) != (ply % 2 == 1):
                why = "parity"
            else:
                try:
                    board.push(board.parse_san(san))
                    if _key(board, wide) != child:
                        why = "child"
                except ValueError:
                    why = "san"
        if why:
            fails[why] = fails.get(why, 0) + 1
            if len(ex) < SHOW:
                ex.append(f"{why}: hash {h} epd {epd!r} san {san!r} child {child} ply {ply}")
    return len(rows), ep, fails, ex


def check_collisions(con, bk: Book) -> None:
    print("\n_collisions", flush=True)
    whole = bk.book / "_collisions.parquet"
    files = [whole] if whole.exists() else sorted((bk.book / "_collisions").glob("bkt*.parquet"))
    rows = []
    for f in files:
        # ParquetFile, not read_table: pyarrow's default hive partitioning
        # would parse key=value directories into columns.
        t = pq.ParquetFile(f).read(columns=["parent_hash", "parent_epd"])
        rows += list(zip(t["parent_hash"].to_pylist(), t["parent_epd"].to_pylist()))
    per: dict[int, set] = {}
    bad, ep = [], 0
    for h, e in rows:
        per.setdefault(h, set()).add(e)
        try:
            ok, via = hash_matches(chess.Board(e), h)
        except ValueError:
            ok, via = False, False
        ep += via
        if not ok:
            bad.append(f"{h} {e!r}")
    check(not bad, f"every _collisions EPD ({len(rows)} rows, {len(per)} hashes) hashes to its "
                   f"parent_hash{f' ({ep} via an illegal ep square)' if ep else ''} {bad[:3]}")
    check(all(len(v) >= 2 for v in per.values()), "every collision hash has >= 2 EPDs")
    pairs = set(rows)
    have = set(bk.done)
    missing, n = [], 0
    for t in bk.months:
        d = bk.root / "_conflicts" / f"month={t}"
        for f in sorted(d.glob("*.parquet")) if d.is_dir() else []:
            c = pq.ParquetFile(f).read().to_pylist()
            for r in c:
                if r["kind"] != "parent-epd" or (r["hash"] % BUCKETS) not in have:
                    continue
                n += 1
                if (r["hash"], r["epd_a"]) not in pairs or (r["hash"], r["epd_b"]) not in pairs:
                    missing.append(f"{t} {r['hash']}")
    if bk.wide:
        # A --key128 month fails on a conflict, so finished months have no records.
        check(n == 0, f"128-bit months carry no collision records ({n} found)")
    else:
        check(not missing, f"the months' {n} collision record(s) in finished buckets are in "
                           f"_collisions with both EPDs {missing[:5]}")


def check_sample(con, bk: Book, n: int, seed: int, workers: int) -> None:
    done = [b for b in bk.done if bk.ps.get(b)]
    rng = random.Random(seed)
    picks = sorted(rng.sample(done, min(16, len(done))))
    print(f"\nsample: {n:,} rows over buckets {picks}", flush=True)
    if not picks:
        check(False, "the book has finished buckets to sample")
        return
    per = math.ceil(n / len(picks))
    his = ", parent_hash_hi, child_hash_hi" if bk.wide else ""
    rows: list[tuple] = []
    for b in picks:
        rows += con.execute(f"""
            SELECT parent_hash, parent_epd, move_san, child_hash, ply{his}
            FROM {relation(bk.ps[b])}
            USING SAMPLE reservoir({per} ROWS) REPEATABLE ({seed})""").fetchall()
    chunks = [rows[i:i + 20_000] for i in range(0, len(rows), 20_000)]
    tot, ep, fails, ex = 0, 0, {}, []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=max(1, workers)) as pool:
        for r, e, f, x in pool.map(_check_rows, chunks):
            tot += r
            ep += e
            for k, v in f.items():
                fails[k] = fails.get(k, 0) + v
            ex += x[: SHOW - len(ex)]
    for s in ex:
        print(f"    {s}")
    check(tot > 0 and not fails,
          f"{tot:,} sampled rows: the EPD hashes to {'the 128-bit parent key' if bk.wide else 'parent_hash'}, "
          f"move_san leads to {'the 128-bit child key' if bk.wide else 'child_hash'}, "
          f"the side to move matches ply ({ep} via an illegal ep square; failures {fails or 'none'}; "
          f"{time.time() - t0:,.0f}s)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("book", type=Path)
    ap.add_argument("months_root", type=Path)
    ap.add_argument("--digest-buckets", default=None, metavar="SPEC")
    ap.add_argument("--dup-buckets", default=None, metavar="SPEC")
    ap.add_argument("--dup-shard", type=parse_dup_shard, metavar="INDEX/COUNT",
                    help="check one disjoint signed-low-key range; all shards are required for full coverage")
    ap.add_argument("--term-digest", action="store_true", help="compare book-v3 terminal rows to the months, including speed, band and both key halves")
    ap.add_argument("--sums", action="store_true")
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--collisions", action="store_true")
    ap.add_argument("--sample", nargs="?", type=int, const=1_000_000, default=None, metavar="N")
    ap.add_argument("--seed", type=int, default=20260928)
    ap.add_argument("--workers", type=int, default=4, help="python-chess processes for --sample")
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--mem", default="4GB")
    ap.add_argument("--tmp-dir", type=Path, default=DEFAULT_TMP)
    a = ap.parse_args()
    if a.dup_shard and not a.dup_buckets: ap.error("--dup-shard requires --dup-buckets")
    t0 = time.time()
    bk = Book(a.book, a.months_root)
    con = connect(a.threads, a.mem, a.tmp_dir)
    print(f"book {a.book} ({bk.schema}, ply cap {bk.max_ply}): "
          f"{len(bk.done)}/{BUCKETS} buckets done, {sum(map(len, bk.ps.values())):,} "
          f"ps files, {'complete' if bk.complete else 'incomplete'}; {len(bk.months)} months under "
          f"{a.months_root}\nduckdb: {a.threads} threads, {a.mem}, temp {a.tmp_dir}", flush=True)
    named = any(x is not None and x is not False for x in
                (a.digest_buckets, a.dup_buckets, a.term_digest or None, a.sums or None, a.scan or None,
                 a.collisions or None, a.sample))
    try:
        if a.digest_buckets:
            check_digests(con, bk, parse_buckets(a.digest_buckets))
        if a.dup_buckets:
            check_dups(con, bk, parse_buckets(a.dup_buckets), a.dup_shard)
        if a.sums or (not named and bk.complete):
            check_sums(con, bk)
        if a.term_digest or (not named and bk.complete and bk.schema in ("book-v3", "book-v4")):
            check_term_digest(con, bk)
        if a.scan or not named:
            check_scan(con, bk)
        if a.collisions or not named:
            check_collisions(con, bk)
        if a.sample is not None or not named:
            check_sample(con, bk, a.sample or 1_000_000, a.seed, a.workers)
    finally:
        con.close()
    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks, "
          f"{time.time() - t0:,.0f}s)")
    return 0 if n_fail == 0 and _checks else 1


if __name__ == "__main__":
    sys.exit(main())
