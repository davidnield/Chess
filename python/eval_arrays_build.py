"""Eval arrays (eval_arrays.py's format) from an eval DB DIRECTORY.

The source is D:/chess/eval_full, built by `explorer-extract evals` (blog repo docs/eval-db-spec.md):
512 bkt<iii>.parquet files, ~5.9B rows keyed by (position_hash, epd), bucket = ((h % 512) + 512) % 512,
each file sorted by (position_hash, epd). Entry point: python/eval_arrays.py --eval-db <dir> --out-dir <dir>.

OUTPUT (the existing format, so open_eval_arrays / lookup_evals work unchanged):
  eval_hash.npy           int64, strictly increasing over the whole hash space
  eval_cp.npy             int16, the DB's eval_cp (the old DB's +-2000 scale)
  excluded.parquet        hashes deliberately answered MISSING, and why
  terminal_mates.parquet  the checkmate positions taken from the book (added = not already in the DB)
  eval_arrays.meta.json   directory fingerprint + book fingerprint + counts + validation, written LAST

EXCLUDED. A hash-only lookup must not pick between EPDs (book README, "Hash-only consumers"):
  book_collision  every hash in the book's _collisions.parquet (two EPDs under one hash), even where the
                  DB evaluated only one of them -- the hash alone cannot say which position is meant
  db_duplicate    a hash with >= 2 DB rows
  hash_ambiguous  a hash whose DB rows carry hash_ambiguous (two eval EPDs under a child-only hash)
  mate_conflict   a '#' child reached at both ply parities (impossible for one real position; a guard)
  mate_hash_collision  a mated child hash whose DB row is a different (non-mated) position

CHECKMATES. Neither Lichess source evaluates the position after mate, so the DB has none. The book's
move_san is the PGN token, so a mating move ends in '#'; the side to move in the child is the mated side,
and the side to move is the ply parity (verify_book.py holds that on every row; ply 1 = White's first
move). So a '#' at an odd ply means White mated: +2000; at an even ply: -2000 -- the old DB's terminal
rule (build_terminal_evals: mate by the mated side). No parent_epd is read. check_terminal_sample replays a
sample with python-chess. Where the DB already holds a different value for a mated hash, python-chess checks
the DB row's EPD: a checkmate takes the mate value (the full DB had 98, each a single classical-era fishnet
row of 10-58 cp), anything else is another position under that hash, so the hash is excluded.

BUILD. mates (groups of 8 book buckets, each group in a fresh DuckDB process; skip-gated per bucket) ->
pass 1 per eval bucket (asserts, exclusions, mates merged; one temp sorted pair each, skip-gated) ->
pass 2 per hash range (256 ranges by the top byte: slice every bucket's pair, sort, append) ->
validate -> sidecars -> meta. The .npy headers are written for the final length and the data appended with
tofile(): a memmap'd output could not be renamed on Windows.
"""
from __future__ import annotations

import gc
import json
import random
import re
import shutil
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
from eval_arrays import (DIR_KIND, MISSING, _paths, _sha256, _write_meta,  # noqa: E402
                         book_fingerprint, dir_fingerprint, lookup_evals)

BUCKETS = 512
EVAL_CAP = 2000
RANGES = 256              # by the top byte: ~23M entries each at full scale
GROUP = 8                 # book buckets per fresh DuckDB process
RELEASE_EVERY = 25        # CLAUDE.md: pyarrow tight loops need gc + release_unused
REASONS = ("book_collision", "db_duplicate", "hash_ambiguous", "mate_conflict", "mate_hash_collision")


def _p(p) -> str:
    return str(p).replace("\\", "/")


def _lit(files) -> str:
    return "[" + ", ".join(f"'{_p(f)}'" for f in files) + "]"


def _bucket(h: np.ndarray) -> np.ndarray:
    """((h % 512) + 512) % 512 for int64: the low 9 bits of the two's-complement value."""
    return (h & (BUCKETS - 1)).astype(np.int64)


def _release() -> None:
    import pyarrow as pa
    gc.collect()
    pa.default_memory_pool().release_unused()


def _run_isolated(fn, job):
    """fn(job) in a fresh single-worker process: its result, or its exception, now
    (build_pooled_stats._run_isolated; no max_tasks_per_child)."""
    with ProcessPoolExecutor(max_workers=1) as ex:
        return ex.submit(fn, job).result()


def _save_npy(path: Path, arr: np.ndarray) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as fh:
        np.save(fh, arr)
    tmp.replace(path)


def book_index(book: Path) -> dict[int, list[Path]]:
    """The book's ps files by bucket; held to the bucket sentinels where they exist."""
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
                raise RuntimeError(f"book bucket {b}: {len(have)} files on disk, its sentinel lists {len(want)}")
    return idx


# ── checkmates from the book ──────────────────────────────────────────────────

def _mate_scan(job: tuple) -> int:
    """A group of book buckets: per bucket, every '#' child with its ply parities, games and rows."""
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    groups, threads, mem, tmp = job
    con = duckdb.connect()
    con.execute(f"SET threads={threads}")
    con.execute(f"SET memory_limit='{mem}'")
    con.execute(f"SET temp_directory='{_p(tmp)}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    n = 0
    for _b, files, out in groups:
        out = Path(out)
        if out.exists():
            continue
        part = out.with_name(out.name + ".tmp")
        part.unlink(missing_ok=True)
        if files:
            con.execute(f"""COPY (
                SELECT child_hash, MIN(ply % 2)::TINYINT AS pmin, MAX(ply % 2)::TINYINT AS pmax,
                       SUM(total)::BIGINT AS games, COUNT(*)::BIGINT AS book_rows
                FROM read_parquet({_lit(files)}, hive_partitioning=false)
                WHERE move_san LIKE '%#' GROUP BY child_hash
            ) TO '{_p(part)}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
        else:
            pq.write_table(pa.table({"child_hash": pa.array([], pa.int64()), "pmin": pa.array([], pa.int8()),
                                     "pmax": pa.array([], pa.int8()), "games": pa.array([], pa.int64()),
                                     "book_rows": pa.array([], pa.int64())}), part)
        part.replace(out)
        n += 1
    con.close()
    return n


def build_terminal_mates(book: Path, work: Path, *, threads: int, mem: str, tmp: Path, log=print) -> dict:
    """Every mated position in the book: (hash, eval_cp, games, conflict), sorted by (bucket, hash)."""
    import pyarrow.parquet as pq
    idx = book_index(book)
    mdir = work / "mates"
    mdir.mkdir(parents=True, exist_ok=True)
    outs = {b: mdir / f"src{b:03d}.parquet" for b in range(BUCKETS)}
    todo = [b for b in range(BUCKETS) if not outs[b].exists()]
    t0 = time.time()
    jobs = [([(b, [str(p) for p in idx.get(b, [])], str(outs[b])) for b in todo[i:i + GROUP]],
             threads, mem, str(tmp)) for i in range(0, len(todo), GROUP)]
    log(f"mates: {BUCKETS - len(todo)} book buckets done, {len(todo)} to scan in {len(jobs)} processes")
    for k, job in enumerate(jobs, 1):
        _run_isolated(_mate_scan, job)
        if k % 8 == 0 or k == len(jobs):
            el = time.time() - t0
            log(f"  mates {k}/{len(jobs)} groups, {el:,.0f}s, ETA {(len(jobs) - k) * el / k / 60:,.1f} min")
    hs, p0, p1, gs, rs = [], [], [], [], []
    for b in range(BUCKETS):
        t = pq.ParquetFile(outs[b]).read()
        hs.append(t.column("child_hash").to_numpy())
        p0.append(t.column("pmin").to_numpy())
        p1.append(t.column("pmax").to_numpy())
        gs.append(t.column("games").to_numpy())
        rs.append(t.column("book_rows").to_numpy())
        if b % RELEASE_EVERY == 0:
            _release()
    h = np.concatenate(hs).astype(np.int64)
    pmin = np.concatenate(p0).astype(np.int8)
    pmax = np.concatenate(p1).astype(np.int8)
    games = np.concatenate(gs).astype(np.int64)
    rows = int(np.concatenate(rs).sum())
    del hs, p0, p1, gs, rs
    o = np.argsort(h, kind="stable")
    h, pmin, pmax, games = h[o], pmin[o], pmax[o], games[o]
    starts = np.flatnonzero(np.r_[True, h[1:] != h[:-1]]) if h.size else np.array([], np.int64)
    uh = h[starts]
    if h.size:
        pmin = np.minimum.reduceat(pmin, starts)
        pmax = np.maximum.reduceat(pmax, starts)
        games = np.add.reduceat(games, starts)
    conflict = pmin != pmax
    cp = np.where(pmin == 1, EVAL_CAP, -EVAL_CAP).astype(np.int16)   # odd ply: White mated Black
    bk = _bucket(uh)
    o = np.argsort(bk, kind="stable")                                # hash order kept within a bucket
    out = {"h": uh[o], "cp": cp[o], "games": games[o], "conflict": conflict[o],
           "offsets": np.searchsorted(bk[o], np.arange(BUCKETS + 1)), "book_rows": rows}
    log(f"mates: {rows:,} '#' book rows -> {uh.size:,} mated positions ({int(conflict.sum())} parity conflicts), "
        f"{time.time() - t0:,.0f}s")
    return out


def _sample_job(job: tuple) -> list:
    import duckdb
    files, n, seed, threads, mem, tmp = job
    con = duckdb.connect()
    con.execute(f"SET threads={threads}")
    con.execute(f"SET memory_limit='{mem}'")
    con.execute(f"SET temp_directory='{_p(tmp)}'")
    con.execute("SET enable_progress_bar=false")
    rows = con.execute(f"""SELECT * FROM (SELECT parent_epd, move_san, child_hash, ply
        FROM read_parquet({_lit(files)}, hive_partitioning=false) WHERE move_san LIKE '%#')
        USING SAMPLE reservoir({n} ROWS) REPEATABLE ({seed})""").fetchall()
    con.close()
    return rows


def check_terminal_sample(book: Path, n: int, seed: int, *, threads: int, mem: str, tmp: Path,
                          buckets: int = 8) -> tuple[int, list[str]]:
    """Replay sampled '#' book rows with python-chess: the move must mate, lead to child_hash, and the
    mated side must be the one the ply parity says."""
    import chess
    from zobrist import zobrist_int64
    idx = book_index(book)
    picks = sorted(random.Random(seed).sample(sorted(idx), min(buckets, len(idx))))
    rows: list = []
    for b in picks:
        rows += _run_isolated(_sample_job, ([str(p) for p in idx[b]], max(1, n // len(picks)), seed,
                                            threads, mem, str(tmp)))
    bad = []
    for epd, san, child, ply in rows:
        try:
            board = chess.Board(epd)
            board.push_san(san)
        except ValueError as e:
            bad.append(f"replay {epd!r} {san}: {e}")
            continue
        mated_white = board.turn == chess.WHITE
        if not board.is_checkmate():
            bad.append(f"not mate: {epd!r} {san}")
        elif zobrist_int64(board) != child:
            bad.append(f"hash: {epd!r} {san} -> {zobrist_int64(board)} != {child}")
        elif mated_white != (ply % 2 == 0):
            bad.append(f"parity: {epd!r} {san} ply {ply}")
    return len(rows), bad


# ── the build ─────────────────────────────────────────────────────────────────

def _npy_writer(path: Path, dtype, n: int):
    fh = open(path, "wb")
    np.lib.format.write_array_header_1_0(fh, {"descr": np.lib.format.dtype_to_descr(np.dtype(dtype)),
                                              "fortran_order": False, "shape": (n,)})
    return fh


def build_from_db_dir(db: Path, array_dir: Path, *, book: Path | None = None, terminal: bool = True,
                      work: Path | None = None, threads: int = 8, mem: str = "8GB",
                      tmp: Path = Path("D:/chess_duckdb_tmp"), terminal_sample: int = 20_000,
                      keep_work: bool = False, check_pool: Path | None = None, seed: int = 20261001,
                      log=print) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq
    t0 = time.time()
    db, array_dir = Path(db), Path(array_dir)
    if str(tmp).upper().startswith("F:") or str(array_dir).upper().startswith("F:"):
        raise SystemExit("FATAL: never F: (USB spinning disk)")
    fp = dir_fingerprint(db)                                       # refuses a DB without _DONE
    meta_db = json.loads((db / "_build.meta.json").read_text(encoding="utf-8"))
    book = Path(book or meta_db["inputs"]["book"])
    bfp = book_fingerprint(book)
    want_book = meta_db.get("inputs", {}).get("book_meta_sha256")
    if want_book and want_book != bfp["book_meta_sha256"]:
        raise ValueError(f"{book} is not the book {db} was built against (_book.meta.json sha256 differs)")
    work = Path(work or str(array_dir) + "_work")
    p1 = work / "p1"
    p1.mkdir(parents=True, exist_ok=True)
    array_dir.mkdir(parents=True, exist_ok=True)
    # A rebuild drops the old meta first: half-replaced arrays must never verify.
    (array_dir / "eval_arrays.meta.json").unlink(missing_ok=True)
    log(f"eval arrays from {db} ({fp['source_rows']:,} rows, _DONE {fp['done']}); book {book}; work {work}")
    coll = np.unique(np.asarray(pq.ParquetFile(book / "_collisions.parquet").read(columns=["parent_hash"])
                                .column("parent_hash").to_numpy(), dtype=np.int64))
    mates = build_terminal_mates(book, work, threads=threads, mem=mem, tmp=tmp, log=log) if terminal else None
    if terminal and terminal_sample:
        n, bad = check_terminal_sample(book, terminal_sample, seed, threads=threads, mem=mem, tmp=tmp)
        for s in bad[:10]:
            log(f"  BAD {s}")
        if bad or n == 0:
            raise RuntimeError(f"checkmate sample: {len(bad)} of {n} '#' rows fail the python-chess replay")
        log(f"mates: {n:,} sampled '#' rows replay to mate, child_hash and parity (python-chess)")

    # Pass 1: per eval bucket, a sorted, de-duplicated pair with the mates merged in.
    tp = time.time()
    for b in range(BUCKETS):
        marker = p1 / f"b{b:03d}.json"
        if marker.exists():
            continue
        t = pq.ParquetFile(db / f"bkt{b:03d}.parquet").read(columns=["position_hash", "eval_cp", "hash_ambiguous"])
        h = t.column("position_hash").to_numpy().astype(np.int64, copy=False)
        cp = t.column("eval_cp").to_numpy().astype(np.int16, copy=False)
        amb = np.asarray(t.column("hash_ambiguous").to_numpy(zero_copy_only=False), dtype=bool)
        del t
        if h.size and not bool(np.all(h[1:] >= h[:-1])):
            raise RuntimeError(f"bkt{b:03d}: position_hash not sorted")
        if h.size and not bool(np.all(_bucket(h) == b)):
            raise RuntimeError(f"bkt{b:03d}: a row hashes into another bucket")
        if h.size and int(np.abs(cp.astype(np.int32)).max()) > EVAL_CAP:
            raise RuntimeError(f"bkt{b:03d}: |eval_cp| > {EVAL_CAP} (or the MISSING sentinel)")
        eq = h[1:] == h[:-1] if h.size else np.zeros(0, bool)
        dup = np.zeros(h.size, bool)
        dup[1:] |= eq
        dup[:-1] |= eq
        is_coll = np.isin(h, coll)
        is_amb = np.isin(h, np.unique(h[amb])) if amb.any() else np.zeros(h.size, bool)
        excl = is_coll | dup | is_amb
        ei = np.flatnonzero(excl)
        excluded = [[int(h[i]), int(cp[i]),
                     "book_collision" if is_coll[i] else "db_duplicate" if dup[i] else "hash_ambiguous"]
                    for i in ei]
        kh, kc = h[~excl].copy(), cp[~excl].copy()
        st = {"bucket": b, "db_rows": int(h.size), "kept": int(kh.size), "mates_added": 0, "mates_in_db": 0,
              "mates_in_db_agree": 0, "db_mate_overridden": 0, "mate_hash_collision": 0,
              "mate_conflicts": 0, "mates_collision": 0, "overridden": []}
        if mates is not None:
            s, e = int(mates["offsets"][b]), int(mates["offsets"][b + 1])
            mh, mc, mconf = mates["h"][s:e], mates["cp"][s:e], mates["conflict"][s:e]
            in_db = np.isin(mh, kh)
            if in_db.any():
                pos = np.searchsorted(kh, mh[in_db])
                st["mates_in_db"] = int(in_db.sum())
                agree = kc[pos] == mc[in_db]
                st["mates_in_db_agree"] = int(agree.sum())
                if not agree.all():
                    # The DB has a value other than the mate for a position the book saw mated: check the
                    # DB row's EPD. A checkmate gets the mate value (2026-10-01: 98 checkmates carried a
                    # single classical fishnet row of 10-58 cp); anything else is a different position
                    # under the same hash, and the hash is excluded.
                    import chess
                    epd = pq.ParquetFile(db / f"bkt{b:03d}.parquet").read(columns=["epd"]).column("epd")
                    drop = []
                    for j in np.flatnonzero(~agree):
                        x, mate_cp = int(mh[in_db][j]), int(mc[in_db][j])
                        row = int(np.searchsorted(h, x))
                        try:
                            board = chess.Board(epd[row].as_py())
                            is_mate = board.is_checkmate() and ((board.turn == chess.WHITE) == (mate_cp < 0))
                        except ValueError:
                            is_mate = False
                        if is_mate:
                            kc[pos[j]] = mate_cp
                            st["db_mate_overridden"] += 1
                            st["overridden"].append(x)
                        else:
                            drop.append(pos[j])
                            excluded.append([x, int(kc[pos[j]]), "mate_hash_collision"])
                    st["mate_hash_collision"] = len(drop)
                    if drop:
                        keep = np.ones(kh.size, bool)
                        keep[drop] = False
                        kh, kc = kh[keep], kc[keep]
                        in_db = np.isin(mh, kh)
            on_excl = (np.isin(mh, h[excl]) | np.isin(mh, coll)  # an excluded hash stays MISSING
                       | np.isin(mh, np.asarray([x[0] for x in excluded if x[2] == "mate_hash_collision"],
                                                dtype=np.int64)))
            st["mates_collision"] = int(on_excl.sum())
            conf = mconf & ~in_db & ~on_excl
            st["mate_conflicts"] = int(conf.sum())
            excluded += [[int(x), None, "mate_conflict"] for x in mh[conf]]
            add = ~in_db & ~on_excl & ~mconf
            st["mates_added"] = int(add.sum())
            kh = np.concatenate([kh, mh[add]])
            kc = np.concatenate([kc, mc[add]])
            o = np.argsort(kh, kind="stable")
            kh, kc = kh[o], kc[o]
        if kh.size and not bool(np.all(kh[1:] > kh[:-1])):
            raise RuntimeError(f"bkt{b:03d}: kept hashes are not unique")
        _save_npy(p1 / f"h{b:03d}.npy", kh)
        _save_npy(p1 / f"c{b:03d}.npy", kc.astype(np.int16))
        st["n"] = int(kh.size)
        st["excluded"] = excluded
        mtmp = marker.with_name(marker.name + ".tmp")
        mtmp.write_text(json.dumps(st), encoding="utf-8")
        mtmp.replace(marker)
        if b % RELEASE_EVERY == 0:
            _release()
        if b % 64 == 63:
            el = time.time() - tp
            log(f"  pass 1: {b + 1}/{BUCKETS} buckets, {el:,.0f}s")
    stats = [json.loads((p1 / f"b{b:03d}.json").read_text(encoding="utf-8")) for b in range(BUCKETS)]
    n_total = sum(s["n"] for s in stats)
    log(f"pass 1: {sum(s['db_rows'] for s in stats):,} DB rows -> {n_total:,} entries ({time.time() - tp:,.0f}s)")

    # Pass 2: global order, one hash range at a time.
    tq = time.time()
    hp, cpp = _paths(array_dir)
    htmp, ctmp = hp.with_name(hp.name + ".tmp"), cpp.with_name(cpp.name + ".tmp")
    fh, fc = _npy_writer(htmp, np.int64, n_total), _npy_writer(ctmp, np.int16, n_total)
    mh_ = [np.load(p1 / f"h{b:03d}.npy", mmap_mode="r") for b in range(BUCKETS)]
    mc_ = [np.load(p1 / f"c{b:03d}.npy", mmap_mode="r") for b in range(BUCKETS)]
    step = 1 << (64 - int(np.log2(RANGES)))
    written, last = 0, None
    try:
        for r in range(RANGES):
            lo = -(1 << 63) + r * step
            hi = None if r == RANGES - 1 else lo + step
            hs, cs = [], []
            for hb, cb in zip(mh_, mc_):
                if hb.shape[0] == 0:
                    continue
                i0 = int(np.searchsorted(hb, lo, "left"))
                i1 = hb.shape[0] if hi is None else int(np.searchsorted(hb, hi, "left"))
                if i1 > i0:
                    hs.append(np.asarray(hb[i0:i1]))
                    cs.append(np.asarray(cb[i0:i1]))
            if not hs:
                continue
            H, C = np.concatenate(hs), np.concatenate(cs)
            o = np.argsort(H, kind="stable")
            H, C = H[o], C[o]
            if (H.size > 1 and not bool(np.all(H[1:] > H[:-1]))) or (last is not None and H[0] <= last):
                raise RuntimeError(f"range {r}: hashes not strictly increasing")
            last = int(H[-1])
            H.tofile(fh)
            C.tofile(fc)
            written += H.size
            if r % 32 == 31:
                log(f"  pass 2: {r + 1}/{RANGES} ranges, {written:,} entries, {time.time() - tq:,.0f}s")
    finally:
        fh.close()
        fc.close()
        del mh_, mc_
        gc.collect()
    if written != n_total:
        raise RuntimeError(f"pass 2 wrote {written:,} entries, pass 1 kept {n_total:,}")
    htmp.replace(hp)
    ctmp.replace(cpp)
    log(f"pass 2: {written:,} entries in global order ({time.time() - tq:,.0f}s)")

    # Sidecars, then validation, then the meta LAST (the arrays verify only once it exists).
    ex = [e for s in stats for e in s["excluded"]]
    pq.write_table(pa.table({"position_hash": pa.array([e[0] for e in ex], pa.int64()),
                             "eval_cp": pa.array([e[1] for e in ex], pa.int16()),
                             "reason": pa.array([e[2] for e in ex], pa.string())}),
                   array_dir / "excluded.parquet")
    if mates is not None:
        added = ~np.isin(mates["h"], coll) & ~mates["conflict"]
        pq.write_table(pa.table({"position_hash": pa.array(mates["h"]), "eval_cp": pa.array(mates["cp"]),
                                 "games": pa.array(mates["games"]), "parity_conflict": pa.array(mates["conflict"]),
                                 "eligible": pa.array(added)}),
                       array_dir / "terminal_mates.parquet", compression="zstd")
    overridden = [x for s in stats for x in s.get("overridden", [])]
    val = validate_arrays(array_dir, db, ex, mates, seed=seed, check_pool=check_pool, log=log,
                          overridden=overridden)
    counts = {k: sum(s.get(k, 0) for s in stats) for k in (
        "db_rows", "kept", "mates_added", "mates_in_db", "mates_in_db_agree", "db_mate_overridden",
        "mate_hash_collision", "mate_conflicts", "mates_collision")}
    counts["excluded"] = {r: sum(1 for e in ex if e[2] == r) for r in REASONS}
    counts["excluded_hashes"] = len({e[0] for e in ex})
    if mates is not None:
        counts["mate_book_rows"] = mates["book_rows"]
        counts["mated_positions"] = int(mates["h"].size)
    meta = dict(fp)
    meta.update({"n_rows": written, "book": bfp, "counts": counts, "validation": val,
                 "ranges": RANGES, "terminal": bool(terminal),
                 "built": time.strftime("%Y-%m-%dT%H:%M:%S"), "seconds": round(time.time() - t0, 1)})
    _write_meta(array_dir, meta)
    if not keep_work:
        shutil.rmtree(work, ignore_errors=True)
    log(f"done: {written:,} entries ({hp.stat().st_size / 1e9:.1f} + {cpp.stat().st_size / 1e9:.1f} GB), "
        f"{counts['excluded_hashes']} excluded hashes, {counts['mates_added']:,} mates added, "
        f"{time.time() - t0:,.0f}s")
    return meta


def validate_arrays(array_dir: Path, db: Path, excluded: list, mates: dict | None, *, seed: int,
                    sample: int = 1_000_000, buckets: int = 16, check_pool: Path | None = None, log=print,
                    overridden: list | None = None) -> dict:
    """The arrays against their source, not against themselves: sampled DB rows return their eval_cp,
    excluded hashes MISSING, eligible mates +-2000; optionally the coverage of a pooled-stats DAG."""
    import pyarrow.parquet as pq
    from eval_arrays import open_eval_arrays
    rng = np.random.default_rng(seed)
    h, e = open_eval_arrays(array_dir)
    ex_h = np.unique(np.asarray([x[0] for x in excluded], dtype=np.int64))
    ov_h = np.unique(np.asarray(overridden or [], dtype=np.int64))   # DB value replaced by a verified mate
    picks = sorted(rng.choice(BUCKETS, size=min(buckets, BUCKETS), replace=False).tolist())
    keys, want = [], []
    for b in picks:
        t = pq.ParquetFile(db / f"bkt{b:03d}.parquet").read(columns=["position_hash", "eval_cp"])
        kh = t.column("position_hash").to_numpy().astype(np.int64)
        kc = t.column("eval_cp").to_numpy().astype(np.int16)
        ok = ~np.isin(kh, ex_h) & ~np.isin(kh, ov_h)
        keys.append(kh[ok])
        want.append(kc[ok])
    keys, want = np.concatenate(keys), np.concatenate(want)
    if keys.size > sample:
        sel = rng.choice(keys.size, size=sample, replace=False)
        keys, want = keys[sel], want[sel]
    got = lookup_evals(keys, h, e)
    out = {"db_sampled": int(keys.size), "db_wrong": int((got != want).sum()), "db_mate_overridden": int(ov_h.size),
           "excluded_probed": int(ex_h.size),
           "excluded_answered": int((lookup_evals(ex_h, h, e) != MISSING).sum())}
    if mates is not None and mates["h"].size:
        elig = mates["h"][~mates["conflict"]]
        mc = mates["cp"][~mates["conflict"]]
        sel = rng.choice(elig.size, size=min(100_000, elig.size), replace=False)
        g = lookup_evals(elig[sel], h, e)
        notcoll = g != MISSING                               # collision hashes are MISSING by design
        out["mates_sampled"] = int(sel.size)
        out["mates_missing"] = int((~notcoll).sum())
        out["mates_non_terminal_value"] = int((np.abs(g[notcoll].astype(np.int32)) != EVAL_CAP).sum())
        out["mates_sign_differs_from_book"] = int((g[notcoll] != mc[sel][notcoll]).sum())
    if check_pool is not None:
        import polars as pl
        need = (pl.concat([pl.scan_parquet(check_pool).select(pl.col("parent_hash").alias("h")),
                           pl.scan_parquet(check_pool).select(pl.col("child_hash").alias("h")).drop_nulls()])
                .unique().collect()["h"].to_numpy().astype(np.int64))
        cov = lookup_evals(need, h, e) != MISSING
        out["pool"] = str(check_pool)
        out["pool_positions"] = int(need.size)
        out["pool_covered"] = int(cov.sum())
    # Every eligible mate is answered +-2000 with the book's sign (a disagreeing DB value was checked
    # and overridden or its hash excluded), so any other answer is a bug.
    bad = (out["db_wrong"] or out["excluded_answered"] or out.get("mates_non_terminal_value", 0)
           or out.get("mates_sign_differs_from_book", 0))
    log(f"validate: {out}")
    del h, e
    gc.collect()
    if bad:
        raise RuntimeError(f"validation failed: {out}")
    return out
