"""Independent check of an eval DB built by `explorer-extract evals` (DuckDB + python-chess; no code shared
with the Rust tool). The contract is the blog repo's docs/eval-db-spec.md, sections 3-5 and 8.

    python/verify_evals.py <out> [--book B] [--cloud C] [--fishnet F] [--buckets N] [--positive N]
                                 [--negative N] [--threads T] [--mem 24GB] [--tmp-dir D] [--seed S] [--no-sha]

The inputs default to the paths `<out>/_build.meta.json` records; the source files, output buckets and
child-source buckets always come from there, so a pilot is checked against exactly what it read.

Checks (each prints PASS/FAIL; exit 0 only if every one passes):

  structure   _DONE; every bucket file present with its manifest's rows and bytes (and sha256 unless
              --no-sha); per sampled bucket: rows unique and strictly sorted on (position_hash, epd), in
              their bucket, exactly one of cp/mate, value sets, hash_ambiguous exactly on child-only
              hashes with >= 2 EPDs.
  positive    >= N output rows (default 100,000) from up to --buckets sampled buckets, stratified: cloud-
              and fishnet-chosen, child-only, ep-variant candidates, collision twins, ambiguous rows.
              For each: python-chess parses the EPD back to itself, and zobrist_int64 of the board (or of
              the board with an ep square the EPD cannot show) is position_hash; in_book holds against the
              book (parent: the (hash, EPD) is a book parent; child: the hash is a book child_hash and not
              a book parent_hash); and DuckDB recomputes the eval from the raw sources by EPD string match
              (cloud: fen = the EPD or the EPD with an ep square it cannot show; fishnet: the first 4 FEN
              fields, the same), with every column required equal.
  negative    >= N book parent positions (default 100,000) of the sampled buckets with no output row: no
              raw source row with a usable score may carry that EPD. Catches silent misses.
  collisions  every book _collisions hash in the output has its twins as separate rows, each EPD's own
              eval (they are all in the positive sample, so the recompute checks them).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import chess
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from zobrist import zobrist_int64  # noqa: E402

BUCKETS = 512
CAP = 2000
NNUE_FROM = (2021, 1)
CLASSICAL_FROM = (2016, 1)
I32 = 2**31 - 1
SHOW = 8
_FAILS: list[str] = []
_N = 0


def check(ok: bool, label: str) -> bool:
    global _N
    _N += 1
    print(f"  {'PASS' if ok else 'FAIL'}  {label}", flush=True)
    if not ok:
        _FAILS.append(label)
    return ok


def _p(path) -> str:
    return str(path).replace("\\", "/")


def lit(files) -> str:
    return "[" + ", ".join(f"'{_p(f)}'" for f in files) + "]"


def bucket(h: int) -> int:
    return ((h % BUCKETS) + BUCKETS) % BUCKETS


# ── the rules, written from the spec ──────────────────────────────────────────

def order_key(cp, mate, white: bool) -> tuple:
    """White-POV total order: (class, value). Mates for White above every cp, shorter higher; mates for
    Black below every cp, shorter lower; mate 0 = the side to move is mated."""
    if mate is None:
        return (1, cp)
    if mate == 0:
        return (0, 0) if white else (2, 0)
    if mate > 0:
        return (2, -mate)
    return (0, -mate)


def to_eval_cp(cp, mate, white: bool) -> int:
    if mate is None:
        return max(-CAP, min(CAP, cp))
    if mate == 0:
        return -CAP if white else CAP
    return CAP if mate > 0 else -CAP


def tier(y: int, m: int) -> str:
    if (y, m) >= NNUE_FROM:
        return "nnue"
    if (y, m) >= CLASSICAL_FROM:
        return "classical"
    return "early"


TIER_RANK = {"nnue": 0, "classical": 1, "early": 2}


def sign(x: int) -> int:
    return (x > 0) - (x < 0)


# ── positions ─────────────────────────────────────────────────────────────────

def ep_alternatives(epd: str) -> tuple[list[str], list[int]]:
    """(strings a source FEN4 may print for this EPD, extra hashes the book may key it by): the EPD
    itself, plus each ep square the position could carry that leaves its legal EPD unchanged; the
    hash of such a board is an ep variant when it differs (a pseudo-legal but illegal capture)."""
    b = chess.Board(epd + " 0 1")
    strs, hashes = [epd], []
    if b.ep_square is not None:
        return strs, hashes
    them = not b.turn
    to_rank, ep_rank, from_rank = (4, 5, 6) if b.turn == chess.WHITE else (3, 2, 1)
    base_h = zobrist_int64(b)
    head = epd.rsplit(" ", 1)[0]
    for f in range(8):
        to, ep, frm = chess.square(f, to_rank), chess.square(f, ep_rank), chess.square(f, from_rank)
        if b.piece_at(to) != chess.Piece(chess.PAWN, them) or b.piece_at(ep) or b.piece_at(frm):
            continue
        v = b.copy(stack=False)
        v.ep_square = ep
        if not v.is_valid() or v.epd() != epd:
            continue
        strs.append(f"{head} {chess.square_name(ep)}")
        h = zobrist_int64(v)
        if h != base_h:
            hashes.append(h)
    return strs, hashes


def check_identity(rows: list[dict]) -> tuple[int, int, list[str]]:
    """(rows ok, ep-variant rows, failures)."""
    bad, var = [], 0
    for r in rows:
        epd, h = r["epd"], r["position_hash"]
        try:
            b = chess.Board(epd + " 0 1")
        except ValueError:
            bad.append(f"unparseable {epd!r}")
            continue
        if not b.is_valid() or b.epd() != epd:
            bad.append(f"not canonical {epd!r}")
            continue
        if bucket(h) != r["bucket"]:
            bad.append(f"bucket {h} {r['bucket']}")
            continue
        if zobrist_int64(b) == h:
            continue
        if h in ep_alternatives(epd)[1]:
            var += 1
            continue
        bad.append(f"hash {h} {epd!r}")
    return len(rows) - len(bad), var, bad


# ── raw scans (each in a fresh process) ───────────────────────────────────────

def scan_file(job: tuple) -> list:
    """One raw source file against the target strings: cloud rows (epd, depth, knodes, cp, mate, line, file,
    row) or fishnet (epd, cp, mate, count) groups."""
    kind, path, tgt_path, threads, mem, tmp = job
    con = duckdb.connect()
    con.execute(f"SET threads={threads}")
    con.execute(f"SET memory_limit='{mem}'")
    con.execute(f"SET temp_directory='{_p(tmp)}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    con.execute(f"CREATE TEMP TABLE tgt AS SELECT * FROM read_parquet('{_p(tgt_path)}')")
    if kind == "cloud":
        name = Path(path).name
        return con.execute(f"""
            SELECT t.epd, c.depth::INTEGER, c.knodes::BIGINT, c.cp::INTEGER, c.mate::INTEGER, c.line,
                   '{name}', c.file_row_number
            FROM read_parquet('{_p(path)}', file_row_number=true) c
            JOIN tgt t ON c.fen = t.alt
            WHERE (c.cp IS NULL) <> (c.mate IS NULL) AND c.depth IS NOT NULL AND c.knodes IS NOT NULL
              AND c.line IS NOT NULL""").fetchall()
    con.execute("CREATE TEMP TABLE tgt_pl AS SELECT DISTINCT split_part(alt, ' ', 1) placement FROM tgt")
    return con.execute(f"""
        SELECT t.epd, f.cp, f.mate, COUNT(*)
        FROM (SELECT fen, cp, mate FROM read_parquet('{_p(path)}')
              WHERE split_part(fen, ' ', 1) IN (SELECT placement FROM tgt_pl)
                AND (cp IS NULL) <> (mate IS NULL)) f
        JOIN tgt t ON regexp_extract(f.fen, '^(\\S+ \\S+ \\S+ \\S+)', 1) = t.alt
        GROUP BY ALL""").fetchall()


# ── child-only membership (each group of book buckets in a fresh process) ────────

CHILD_GROUP = 8          # book buckets per process: Windows process start-up (~1-2 s) would dominate 1 per job
CHILD_MEM = "4GB"        # the parent keeps its own --mem connection alive


def book_index(book: Path) -> dict[int, list[Path]]:
    """Every book ps file by bucket, from one walk of ps/. Where the book's per-bucket sentinel
    (_done/bkt<iii>.DONE) exists, the walk must find exactly the files it lists."""
    idx: dict[int, list[Path]] = {}
    for p in book.glob("ps/event=*/elo_band=*/bkt*.parquet"):
        m = re.fullmatch(r"bkt(\d{3})\.parquet", p.name)
        if m:
            idx.setdefault(int(m.group(1)), []).append(p)
    for b, files in idx.items():
        files.sort()
        sent = book / "_done" / f"bkt{b:03d}.DONE"
        if sent.is_file():
            want = {f["path"] for f in json.loads(sent.read_text(encoding="utf-8"))["files"]}
            have = {p.relative_to(book).as_posix() for p in files}
            if want != have:
                raise SystemExit(f"FATAL: book bucket {b}: {len(have)} files on disk, its sentinel lists {len(want)}")
    return idx


def _run_isolated(fn, job):
    """fn(job) in a fresh single-worker process (the repo's build_pooled_stats._run_isolated pattern, copied so
    this checker shares no code with the pipeline): its result, or its exception, now."""
    with ProcessPoolExecutor(max_workers=1) as ex:
        return ex.submit(fn, job).result()


def _member_scan(job: tuple) -> tuple[list[int], list[int]]:
    """One group of book buckets: which target hashes occur as a book child_hash (buckets flagged for the
    child scan), and which as a book parent_hash (only the targets that hash into the bucket)."""
    groups, targets, threads, mem, tmp = job
    con = duckdb.connect()
    con.execute(f"SET threads={threads}")
    con.execute(f"SET memory_limit='{mem}'")
    con.execute(f"SET temp_directory='{_p(tmp)}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    con.execute("CREATE TEMP TABLE ch AS SELECT UNNEST(?::BIGINT[]) h", [targets])
    as_child: set[int] = set()
    as_parent: set[int] = set()
    for _b, files, scan_child, parent_targets in groups:
        if not files:
            continue
        src = f"read_parquet({lit(files)}, hive_partitioning=false)"
        if scan_child:
            as_child.update(r[0] for r in con.execute(
                f"SELECT DISTINCT child_hash FROM {src} WHERE child_hash IN (SELECT h FROM ch)").fetchall())
        if parent_targets:
            as_parent.update(r[0] for r in con.execute(
                f"SELECT DISTINCT parent_hash FROM {src} WHERE parent_hash IN (SELECT UNNEST(?::BIGINT[]))",
                [parent_targets]).fetchall())
    con.close()
    return sorted(as_child), sorted(as_parent)


def check_children(idx: dict[int, list[Path]], child_src: list[int], chi: list[int], a) -> None:
    """The sampled child-only hashes must be book child_hash values (any child-source bucket) and no book
    parent_hash. In-process, one query over all 27,648 book files crawled at ~1 core after the sampling
    phase (2026-10-01, ~6.5 h projected): CLAUDE.md's process-wide DuckDB decay. Each group of buckets now
    runs in its own fresh process."""
    tc = time.time()
    targets = sorted(chi)
    by_b: dict[int, list[int]] = {}
    for h in targets:
        by_b.setdefault(bucket(h), []).append(h)
    src = set(child_src)
    buckets = sorted(src | set(by_b))
    jobs = []
    for i in range(0, len(buckets), CHILD_GROUP):
        g = [(b, [str(p) for p in idx.get(b, [])], b in src, by_b.get(b, [])) for b in buckets[i:i + CHILD_GROUP]]
        jobs.append((g, targets, a.threads, CHILD_MEM, str(a.tmp_dir)))
    got: set[int] = set()
    par_h: set[int] = set()
    for k, job in enumerate(jobs, 1):
        c, p = _run_isolated(_member_scan, job)
        got.update(c)
        par_h.update(p)
        if k % 8 == 0 or k == len(jobs):
            el = time.time() - tc
            print(f"    child check {min(k * CHILD_GROUP, len(buckets))}/{len(buckets)} book buckets, {el:,.0f}s, "
                  f"ETA {(len(jobs) - k) * el / k / 60:,.1f} min", flush=True)
    missing = sorted(set(targets) - got)
    for h in missing[:SHOW]:
        print(f"    not a book child_hash: {h}")
    check(not missing and not par_h,
          f"{len(targets):,} sampled child-only hashes are book child_hash values ({len(missing)} are not) "
          f"and no book parent_hash ({len(par_h)} are) ({time.time() - tc:,.0f}s)")


# ── main ──────────────────────────────────────────────────────────────────────

def connect(a) -> duckdb.DuckDBPyConnection:
    if _p(a.tmp_dir).upper().startswith("F:"):
        raise SystemExit("FATAL: never put DuckDB temp on F:")
    Path(a.tmp_dir).mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET threads={a.threads}")
    con.execute(f"SET memory_limit='{a.mem}'")
    con.execute(f"SET temp_directory='{_p(a.tmp_dir)}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    return con


def book_files(book: Path, b: int) -> list[Path]:
    return sorted(book.glob(f"ps/event=*/elo_band=*/bkt{b:03d}.parquet"))


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        while chunk := f.read(8 << 20):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("out", type=Path)
    ap.add_argument("--book", type=Path)
    ap.add_argument("--cloud", type=Path)
    ap.add_argument("--fishnet", type=Path)
    ap.add_argument("--buckets", type=int, default=16, help="output buckets to sample (default 16)")
    ap.add_argument("--positive", type=int, default=100_000)
    ap.add_argument("--negative", type=int, default=100_000)
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--mem", default="24GB")
    ap.add_argument("--tmp-dir", type=Path, default=Path("D:/chess_duckdb_tmp_evals"))
    ap.add_argument("--seed", type=int, default=20260930)
    ap.add_argument("--no-sha", action="store_true")
    ap.add_argument("--dump-targets", type=Path, help=argparse.SUPPRESS)
    # Debug/timing: sample N child-only rows from the sampled buckets and run only the child check.
    ap.add_argument("--child-check-only", type=int, default=None, metavar="N", help=argparse.SUPPRESS)
    a = ap.parse_args()
    t0 = time.time()
    out = a.out
    meta = json.loads((out / "_build.meta.json").read_text(encoding="utf-8"))
    params = meta["params"]
    book = a.book or Path(meta["inputs"]["book"])
    cloud_dir = a.cloud or Path(meta["inputs"]["cloud"])
    fish_dir = a.fishnet or Path(meta["inputs"]["fishnet"])
    cloud_files = [cloud_dir / f for f in meta["inputs"]["cloud_files"]]
    fish_files = [fish_dir / f for f in meta["inputs"]["fishnet_files"]]
    all_b = list(range(BUCKETS))
    out_buckets = all_b if params["buckets"] == "all" else list(params["buckets"])
    child_src = all_b if params["child_sources"] == "all" else list(params["child_sources"])
    rng = random.Random(a.seed)
    picks = sorted(rng.sample(out_buckets, min(a.buckets, len(out_buckets))))
    print(f"eval DB {out}: {len(out_buckets)} buckets, sampling {picks}; book {book}; "
          f"{len(cloud_files)} cloud + {len(fish_files)} fishnet files; child sources "
          f"{'all' if len(child_src) == BUCKETS else child_src}", flush=True)
    con = connect(a)

    if a.child_check_only is not None:
        files = [out / f"bkt{b:03d}.parquet" for b in picks]
        chi = sorted({r[0] for r in con.execute(
            f"SELECT * FROM (SELECT position_hash FROM read_parquet({lit(files)}) WHERE in_book = 'child') "
            f"USING SAMPLE reservoir({a.child_check_only} ROWS) REPEATABLE ({a.seed})").fetchall()})
        print(f"\nchild check only: {len(chi):,} child-only hashes from {len(picks)} buckets", flush=True)
        if chi:
            check_children(book_index(book), child_src, chi, a)
        else:
            check(False, "the sampled buckets hold child-only rows")
        print(f"\n{'ALL PASS' if not _FAILS else f'{len(_FAILS)} FAILURES'} ({_N} checks, "
              f"{time.time() - t0:,.0f}s)", flush=True)
        return 0 if not _FAILS else 1

    # ── structure ──
    print("\nstructure", flush=True)
    check((out / "_DONE").is_file(), "_DONE exists")
    man = pq.ParquetFile(out / "_manifest.parquet").read().to_pylist()
    mb = {r["bucket"]: r for r in man}
    check(sorted(mb) == sorted(out_buckets), f"_manifest has exactly the {len(out_buckets)} buckets")
    bad = []
    for b in out_buckets:
        p = out / f"bkt{b:03d}.parquet"
        if not p.is_file():
            bad.append(f"{p.name} missing")
            continue
        md = pq.ParquetFile(p).metadata
        if b in mb and (md.num_rows != mb[b]["rows"] or p.stat().st_size != mb[b]["bytes"]):
            bad.append(f"{p.name}: rows/bytes vs manifest")
        if not a.no_sha and b in mb and sha256(p) != mb[b]["sha256"]:
            bad.append(f"{p.name}: sha256 vs manifest")
    check(not bad, f"every bucket file matches its manifest row (rows, bytes{'' if a.no_sha else ', sha256'}) {bad[:3]}")
    files = [out / f"bkt{b:03d}.parquet" for b in picks]
    probs = {}
    for b, f in zip(picks, files):
        r = con.execute(f"""
            WITH t AS (SELECT *, lag(position_hash) OVER w ph, lag(epd) OVER w pe, row_number() OVER w rn
                       FROM read_parquet('{_p(f)}', file_row_number=true) WINDOW w AS (ORDER BY file_row_number))
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE ph IS NOT NULL AND (ph > position_hash OR (ph = position_hash AND pe >= epd))),
                   COUNT(*) FILTER (WHERE ((position_hash % {BUCKETS}) + {BUCKETS}) % {BUCKETS} <> {b}),
                   COUNT(*) FILTER (WHERE (cp IS NULL) = (mate IS NULL)),
                   COUNT(*) FILTER (WHERE in_book NOT IN ('parent', 'child') OR source NOT IN ('cloud', 'fishnet')
                                     OR (source = 'cloud' AND cloud_depth IS NULL)
                                     OR (source = 'fishnet' AND (cloud_depth IS NOT NULL OR fishnet_tier IS NULL))
                                     OR (fishnet_disagrees AND source <> 'cloud')
                                     OR (hash_ambiguous AND in_book <> 'child'))
            FROM t""").fetchone()
        amb = con.execute(f"""
            WITH c AS (SELECT position_hash, COUNT(*) n, bool_and(hash_ambiguous) a, bool_or(hash_ambiguous) o
                       FROM read_parquet('{_p(f)}') WHERE in_book = 'child' GROUP BY 1)
            SELECT COUNT(*) FILTER (WHERE (n >= 2) <> a OR a <> o) FROM c""").fetchone()[0]
        for k, v in zip(("rows", "order", "bucket", "cp_mate", "values"), r):
            probs[k] = probs.get(k, 0) + v
        probs["ambiguous"] = probs.get("ambiguous", 0) + amb
    check(not any(v for k, v in probs.items() if k != "rows"),
          f"{len(picks)} sampled buckets, {probs.get('rows', 0):,} rows: strictly sorted and unique on "
          f"(position_hash, epd), in bucket, one of cp/mate, value sets, hash_ambiguous {probs}")

    # ── samples ──
    print("\nsamples", flush=True)
    colls = pq.ParquetFile(book / "_collisions.parquet").read(columns=["parent_hash", "parent_epd"]).to_pylist() \
        if (book / "_collisions.parquet").is_file() else []
    coll_h = sorted({r["parent_hash"] for r in colls})
    con.execute("CREATE TEMP TABLE coll AS SELECT UNNEST(?::BIGINT[]) h", [coll_h])
    per = max(1, a.positive // 10)
    fl = lit(files)
    cols = "position_hash, epd, in_book, source, cp, mate, eval_cp, cloud_depth, cloud_knodes, cloud_cp, " \
           "cloud_mate, cloud_line, cloud_n_evals, fishnet_cp, fishnet_mate, fishnet_tier, fishnet_n_tier, " \
           "fishnet_n, hash_ambiguous, fishnet_disagrees"
    # ep-variant candidates: an own pawn next to an enemy pawn on the rank where an ep capture starts.
    ranks = ("regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace("
             "regexp_replace(regexp_replace(split_part(split_part(epd, ' ', 1), '/', {i}), '8', '........', 'g'), "
             "'7', '.......', 'g'), '6', '......', 'g'), '5', '.....', 'g'), '4', '....', 'g'), '3', '...', 'g'), "
             "'2', '..', 'g'), '1', '.', 'g')")
    cand = (f"((split_part(epd, ' ', 2) = 'w' AND regexp_matches({ranks.format(i=4)}, 'Pp|pP') "
            f"OR split_part(epd, ' ', 2) = 'b' AND regexp_matches({ranks.format(i=5)}, 'Pp|pP')) "
            f"AND split_part(epd, ' ', 4) = '-')")
    strata = {
        "cloud": "source = 'cloud'", "fishnet": "source = 'fishnet'", "child": "in_book = 'child'",
        "ep_candidate": cand, "collision": "position_hash IN (SELECT h FROM coll)", "ambiguous": "hash_ambiguous",
    }
    # Oversampled so that the strata's overlaps still leave >= --positive distinct rows.
    quota = {"cloud": 9 * per // 2, "fishnet": 9 * per // 2, "child": per, "ep_candidate": per, "collision": 10**9,
             "ambiguous": 20_000}
    samp = {}
    # ep variants proper: python-chess finds them among up to 3M candidates (the hash of the EPD's own
    # board differs from position_hash); every one found joins the sample.
    cands = con.execute(f"SELECT * FROM (SELECT position_hash, epd FROM read_parquet({fl}) WHERE {cand}) "
                        f"USING SAMPLE reservoir(3000000 ROWS) REPEATABLE ({a.seed})").fetchall()
    var_keys = [(h, e) for h, e in cands if zobrist_int64(chess.Board(e + " 0 1")) != h]
    con.register("vk", pa.table({"h": [k[0] for k in var_keys], "e": [k[1] for k in var_keys]}))
    rows = con.execute(f"SELECT {cols}, ((position_hash % {BUCKETS}) + {BUCKETS}) % {BUCKETS} AS bucket "
                       f"FROM read_parquet({fl}) o SEMI JOIN vk ON o.position_hash = vk.h AND o.epd = vk.e"
                       ).to_arrow_table().to_pylist()
    con.unregister("vk")
    n_var_found = len(rows)
    print(f"    ep_variant: {n_var_found:,} rows (of {len(cands):,} candidates checked)", flush=True)
    for r in rows:
        samp[(r["position_hash"], r["epd"])] = r
    for name, where in strata.items():
        q = quota[name]
        # The sample goes on a subquery: DuckDB samples FROM before WHERE.
        rows = con.execute(f"SELECT * FROM (SELECT {cols}, ((position_hash % {BUCKETS}) + {BUCKETS}) % {BUCKETS} "
                           f"AS bucket FROM read_parquet({fl}) WHERE {where})"
                           + (f" USING SAMPLE reservoir({q} ROWS) REPEATABLE ({a.seed})" if q < 10**9 else "")
                           ).to_arrow_table().to_pylist()
        print(f"    {name}: {len(rows):,} rows", flush=True)
        for r in rows:
            samp[(r["position_hash"], r["epd"])] = r
    pos = list(samp.values())
    print(f"    positive sample: {len(pos):,} distinct rows", flush=True)
    ok_n, var_n, bad = check_identity(pos)
    check(not bad, f"{ok_n:,} sampled rows: the EPD is canonical python-chess, zobrist_int64 gives position_hash "
                   f"({var_n} via an ep square the EPD cannot show) {bad[:SHOW]}")
    check(var_n >= n_var_found, f"every ep-variant row found ({n_var_found:,}) hashes via an ep square its EPD cannot show")
    check(len(pos) >= a.positive or sum(mb[b]["rows"] for b in picks if b in mb) < a.positive,
          f"the positive sample has >= {a.positive:,} rows (or all the sampled buckets hold fewer)")

    # Negative sample: book parents of the sampled buckets with no output row.
    neg = []
    per_b = max(1, (2 * a.negative) // max(1, len(picks)))
    for b, f in zip(picks, files):
        bf = book_files(book, b)
        if not bf:
            continue
        rows = con.execute(f"""
            WITH s AS (SELECT DISTINCT parent_hash, parent_epd FROM (SELECT parent_hash, parent_epd
                       FROM read_parquet({lit(bf)}, hive_partitioning=false)
                       USING SAMPLE reservoir({per_b} ROWS) REPEATABLE ({a.seed})))
            SELECT s.parent_hash, s.parent_epd FROM s
            ANTI JOIN read_parquet('{_p(f)}') o ON o.position_hash = s.parent_hash AND o.epd = s.parent_epd""").fetchall()
        neg += rows
    rng.shuffle(neg)
    neg = neg[: max(a.negative, 0)]
    print(f"    negative sample: {len(neg):,} book parents without an output row", flush=True)
    check(len(neg) >= min(a.negative, 1), f"the negative sample is non-empty ({len(neg):,})")

    # Targets: every string a source FEN4 may print for each EPD.
    tgt_epd, tgt_alt = [], []
    for e in {r["epd"] for r in pos} | {e for _, e in neg}:
        for s in ep_alternatives(e)[0]:
            tgt_epd.append(e)
            tgt_alt.append(s)
    print(f"    {len(tgt_alt):,} target strings for {len(set(tgt_epd)):,} EPDs", flush=True)
    if a.dump_targets:
        pq.write_table(pa.table({"epd": tgt_epd, "alt": tgt_alt}), a.dump_targets)
        print(f"    targets written to {a.dump_targets}; stopping (debug)")
        return 2

    # ── raw recompute ──
    print("\nraw sources", flush=True)
    # Every raw scan runs in a fresh child process, one file per process (CLAUDE.md: a process that has done
    # hours of multi-GB DuckDB work decays). In-process, the fishnet scans ran ~0.8M rows/s against ~14M
    # standalone (2026-09-30), whether as one query over all 144 files or one query and connection per file.
    tgt_path = Path(a.tmp_dir) / f"verify_targets_{os.getpid()}.parquet"
    pq.write_table(pa.table({"epd": tgt_epd, "alt": tgt_alt}), tgt_path)
    jobs = [("cloud", str(f), str(tgt_path), a.threads, a.mem, str(a.tmp_dir)) for f in cloud_files] +            [("fishnet", str(f), str(tgt_path), a.threads, a.mem, str(a.tmp_dir)) for f in fish_files]
    sizes = {str(f): pq.ParquetFile(f).metadata.num_rows for f in cloud_files + fish_files}
    total = {k: sum(sizes[j[1]] for j in jobs if j[0] == k) for k in ("cloud", "fishnet")}
    crow, fishes = [], {}
    done = {"cloud": 0, "fishnet": 0}
    nfiles = {"cloud": 0, "fishnet": 0}
    t0s = time.time()
    with ProcessPoolExecutor(max_workers=1, max_tasks_per_child=1) as pool:
        for job, rows in zip(jobs, pool.map(scan_file, jobs)):
            kind, path = job[0], job[1]
            if kind == "cloud":
                crow += rows
            else:
                m = re.search(r"standard_rated_(\d{4})_(\d{2})", Path(path).name)
                t = tier(int(m.group(1)), int(m.group(2)))
                for e, cp, mate, n in rows:
                    fishes.setdefault(e, {}).setdefault(t, []).append((cp, mate, n))
            done[kind] += sizes[path]
            nfiles[kind] += 1
            el = time.time() - t0s
            all_done, all_total = sum(done.values()), sum(total.values())
            if nfiles[kind] % 12 == 0 or (kind == "fishnet" and nfiles[kind] == len(fish_files)) or                     (kind == "cloud" and nfiles[kind] == len(cloud_files)):
                print(f"    {kind} {nfiles[kind]} files, {done[kind] / 1e9:,.2f}/{total[kind] / 1e9:,.2f} B rows; "
                      f"all scans {el:,.0f}s, {all_done / max(el, 1e-9) / 1e6:,.1f}M rows/s, ETA "
                      f"{(all_total - all_done) / max(all_done, 1) * el / 60:,.0f} min", flush=True)
    tgt_path.unlink(missing_ok=True)
    print(f"    cloud: {len(crow):,} matching rows; fishnet: {sum(len(v) for d in fishes.values() for v in d.values()):,} "
          f"(EPD, month, score) groups ({time.time() - t0s:,.0f}s)", flush=True)
    clouds: dict[str, list] = {}
    for e, d, kn, cp, mate, line, fname, rn in crow:
        clouds.setdefault(e, []).append((d, kn, cp, mate, line, fname, rn))

    def expected(epd: str) -> dict | None:
        white = epd.split(" ")[1] == "w"
        x = {k: None for k in ("cloud_depth", "cloud_knodes", "cloud_cp", "cloud_mate", "cloud_line",
                               "cloud_n_evals", "fishnet_cp", "fishnet_mate", "fishnet_tier", "fishnet_n_tier",
                               "fishnet_n")}
        c = clouds.get(epd)
        if c:
            best = sorted(c, key=lambda r: (-r[0], -r[1], r[5], r[6]))[0]
            x.update(cloud_depth=best[0], cloud_knodes=best[1], cloud_cp=best[2], cloud_mate=best[3],
                     cloud_line=best[4], cloud_n_evals=len({(r[0], r[1]) for r in c}))
        fz = fishes.get(epd)
        if fz:
            t = min(fz, key=lambda k: TIER_RANK[k])
            vals = sorted(fz[t], key=lambda r: order_key(r[0], r[1], white))
            n_t = sum(r[2] for r in vals)
            want, seen = (n_t - 1) // 2, 0
            for cp, mate, n in vals:
                seen += n
                if seen > want:
                    break
            x.update(fishnet_cp=cp, fishnet_mate=mate, fishnet_tier=t, fishnet_n_tier=min(n_t, I32),
                     fishnet_n=min(sum(r[2] for v in fz.values() for r in v), I32))
        if not c and not fz:
            return None
        if c:
            x.update(source="cloud", cp=x["cloud_cp"], mate=x["cloud_mate"])
        else:
            x.update(source="fishnet", cp=x["fishnet_cp"], mate=x["fishnet_mate"])
        x["eval_cp"] = to_eval_cp(x["cp"], x["mate"], white)
        dis = False
        if c and fz:
            fe = to_eval_cp(x["fishnet_cp"], x["fishnet_mate"], white)
            dis = abs(fe) == CAP and x["fishnet_n_tier"] >= 5 and sign(fe) != sign(x["eval_cp"])
        x["fishnet_disagrees"] = dis
        return x

    fields = ["source", "cp", "mate", "eval_cp", "cloud_depth", "cloud_knodes", "cloud_cp", "cloud_mate",
              "cloud_line", "cloud_n_evals", "fishnet_cp", "fishnet_mate", "fishnet_tier", "fishnet_n_tier",
              "fishnet_n", "fishnet_disagrees"]
    diffs, n_cmp, by_src = [], 0, {}
    for r in pos:
        x = expected(r["epd"])
        n_cmp += 1
        if x is None:
            diffs.append(f"no raw eval for {r['epd']!r}")
            continue
        by_src[x["source"]] = by_src.get(x["source"], 0) + 1
        d = [f"{k}: db {r[k]!r} raw {x[k]!r}" for k in fields if r[k] != x[k]]
        if d:
            diffs.append(f"{r['position_hash']} {r['epd']!r}: " + "; ".join(d))
    for s in diffs[:SHOW]:
        print(f"    {s}")
    check(not diffs, f"{n_cmp:,} positive rows ({by_src}): every eval column equals the recompute from the raw "
                     f"sources ({len(diffs)} differ)")
    miss = [(h, e) for h, e in neg if e in clouds or e in fishes]
    for h, e in miss[:SHOW]:
        print(f"    miss: {h} {e!r} cloud {len(clouds.get(e, []))} fishnet {sum(len(v) for v in fishes.get(e, {}).values())}")
    check(not miss, f"{len(neg):,} book parents without an output row: no raw source row carries their EPD "
                    f"({len(miss)} do)")

    # ── in_book against the book ──
    print("\nin_book against the book", flush=True)
    par = [(r["position_hash"], r["epd"], r["bucket"]) for r in pos if r["in_book"] == "parent"]
    chi = sorted({r["position_hash"] for r in pos if r["in_book"] == "child"})
    bad = 0
    by_b: dict[int, list] = {}
    for h, e, b in par:
        by_b.setdefault(b, []).append((h, e))
    for b, v in by_b.items():
        bf = book_files(book, b)
        con.register("pp", pa.table({"h": [x[0] for x in v], "e": [x[1] for x in v]}))
        found = con.execute(f"""SELECT COUNT(*) FROM pp WHERE EXISTS (SELECT 1 FROM read_parquet({lit(bf)},
                                hive_partitioning=false) k WHERE k.parent_hash = pp.h AND k.parent_epd = pp.e)""").fetchone()[0] if bf else 0
        bad += len(v) - found
        con.unregister("pp")
    check(bad == 0, f"{len(par):,} sampled parent rows are book (parent_hash, parent_epd) ({bad} are not)")
    if chi:
        check_children(book_index(book), child_src, chi, a)
    else:
        print("    (no child rows in the sample)")

    # ── collisions ──
    print("\ncollisions", flush=True)
    twins: dict[int, set] = {}
    for r in colls:
        twins.setdefault(r["parent_hash"], set()).add(r["parent_epd"])
    in_out = [r for r in pos if r["position_hash"] in twins]
    bad = [f"{r['position_hash']} {r['epd']!r} {r['in_book']}" for r in in_out
           if r["in_book"] != "parent" or r["epd"] not in twins[r["position_hash"]]]
    per_h: dict[int, list] = {}
    for r in in_out:
        per_h.setdefault(r["position_hash"], []).append(r["epd"])
    dup = [h for h, v in per_h.items() if len(v) != len(set(v))]
    sampled_coll = [h for h in twins if bucket(h) in picks]
    check(not bad and not dup,
          f"{len(sampled_coll)} book collision hashes in the sampled buckets; {len(per_h)} have output rows "
          f"({len(in_out)} rows, all in the positive sample and recomputed above): each twin is its own row "
          f"under its own EPD {bad[:3]} {dup[:3]}")

    el = time.time() - t0
    print(f"\n{'ALL PASS' if not _FAILS else f'{len(_FAILS)} FAILURES'} ({_N} checks, {el:,.0f}s)", flush=True)
    return 0 if not _FAILS else 1


if __name__ == "__main__":
    raise SystemExit(main())
