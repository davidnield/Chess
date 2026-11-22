"""ChessDB move-menu import, scan path (docs/move-menus-plan-2026-11-21.md; 2026-11-21).

Same output as import_menus.py (same OUT, schema, per-bucket _done records written LAST), built from ONE
sequential scan of the snapshot instead of a point lookup per book position. Measured: a lookup costs
~200 us of CPU, the export's EPD pass ~90% of its time (67 s keys vs 599 s EPDs, bucket 511); the scan
needs no EPDs and reads each snapshot entry once.

Phases, each resumable (an output exists only if complete):
  keys     per book bucket: SCAN/keys/bkt{b}.bin (sorted int64 lo, hi pairs) and bkt{b}.parquet (lo, hi,
           games, first_ply; renamed LAST): the importer's key pass, unchanged (>= MIN_GAMES over slices).
  scan     WSL cpp/cdb_scan over the snapshot -> SCAN/units/u{NNNN}.tsv (see its header): every entry in
           both orientations, our 128-bit key computed in C++ (keytest: 0 bad of 9,332,339 book keys),
           kept when in a Bloom filter of the keys.
  split    units -> SCAN/split/batch={j}/bkt={b}/*.parquet (DuckDB), UNIT_BATCH units per batch.
  join     per bucket: exact semi-join to the keys; python-chess decides the en passant rows the way the
           lookup's EPD probe did (flag 1: kept iff the restored ep capture is pseudo-legal and illegal;
           flag 2: kept iff the FEN's ep capture is legal); every sampled row (flag 4) has its key
           recomputed from its FEN and every move checked legal; any key with two rows left is an error.
           Side to move from the book's first ply (odd = White: 93,336,062 of 93,336,062 rows agree on
           buckets 0-9). Then import_menus.finish_bucket, the lookup path's own output code.
           A key still left with two rows is a position ChessDB holds under two encodings (bucket 40 had one):
           the book's EPD is looked up with cdb_lookup, exactly as the lookup path did, and the scan row equal
           to its answer is kept (anything else stops the bucket).
  compare  for buckets the lookup path finished: join into SCAN/compare, then both files diffed at
           tolerance 0 (EXCEPT ALL both ways).

Usage (pinned code for real runs; long runs launched detached):
  python scan_menus.py chain [--workers 4] [--join-workers 3]   everything below, in order, resumable;
                                   stops at the comparison gate unless every lookup-made bucket is identical
  python scan_menus.py keys [--workers 3] [--buckets ...]
  python scan_menus.py scan-cmd                    print the WSL command (launch it from a .bat)
  python scan_menus.py split [--workers 1]
  python scan_menus.py join [--workers 3] [--buckets ...]
  python scan_menus.py compare --buckets 0 1 ...
  python scan_menus.py keys-one|join-one|compare-one --bucket B
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import import_menus as im  # noqa: E402  (also puts the pinned python/ on sys.path)

SCAN = Path(os.environ.get("CHESSDB_SCAN_ROOT", "E:/chess/chessdb-scan-20261121"))   # override: tests only
KEYS, UNITS, SPLIT, CMP = SCAN / "keys", SCAN / "units", SCAN / "split", SCAN / "compare"
N_UNITS = 256          # requested; the snapshot's 279 SST files give fewer (UNITS/_units has the count)
UNIT_BATCH = 20
SCAN_THREADS = 12
TOOL = "/opt/chessdb-pilot/import/cdb_scan"


def wsl(p: Path) -> str:
    return im.wsl_path(p)


# â”€â”€ keys â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def keys_one(b: int) -> None:
    import numpy as np
    pq, bn = KEYS / f"bkt{b:03d}.parquet", KEYS / f"bkt{b:03d}.bin"
    if pq.exists() and bn.exists():
        return
    KEYS.mkdir(parents=True, exist_ok=True)
    files = sorted(im.BOOK.glob(f"ps/event=*/elo_band=*/bkt{b:03d}.parquet"))
    if not files:
        raise SystemExit(f"bucket {b}: no book files")
    con, _ = im.connect()
    lit = ", ".join("'" + f.as_posix() + "'" for f in files)
    t = time.time()
    con.execute(f"""CREATE TEMP TABLE k AS SELECT parent_hash AS lo, parent_hash_hi AS hi,
                    sum(total)::BIGINT AS games, min(ply)::SMALLINT AS first_ply
                    FROM read_parquet([{lit}], hive_partitioning=false) GROUP BY 1, 2
                    HAVING sum(total) >= {im.MIN_GAMES}""")
    r = con.execute("SELECT lo, hi FROM k ORDER BY lo, hi").fetchnumpy()
    arr = np.empty((len(r["lo"]), 2), dtype="<i8")
    arr[:, 0], arr[:, 1] = r["lo"], r["hi"]
    if len(arr) and not np.all((arr[1:, 0] > arr[:-1, 0]) | ((arr[1:, 0] == arr[:-1, 0]) & (arr[1:, 1] > arr[:-1, 1]))):
        raise SystemExit(f"bucket {b}: keys not unique")
    if len(arr) and np.any((arr[:, 0] & 511) != b):
        raise SystemExit(f"bucket {b}: a key outside its bucket")
    tb = bn.with_name(bn.name + ".tmp")
    arr.tofile(tb)
    tb.replace(bn)
    tp = pq.with_name(pq.name + ".tmp")
    con.execute(f"COPY (SELECT * FROM k ORDER BY lo, hi) TO '{tp.as_posix()}' (FORMAT parquet)")
    tp.replace(pq)
    con.close()
    im.log(f"keys {b}: {len(arr):,} positions in {time.time() - t:.0f}s")


# â”€â”€ scan â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def scan_cmd() -> str:
    UNITS.mkdir(parents=True, exist_ok=True)
    missing = [b for b in range(512) if not (KEYS / f"bkt{b:03d}.parquet").exists()]
    if missing:
        raise SystemExit(f"{len(missing)} key buckets missing (first {missing[:5]}): run keys first")
    return (f"bash {im.CPP}/mount.sh >/dev/null && {TOOL} scan {wsl(KEYS)} {wsl(UNITS)} "
            f"{SCAN_THREADS} {N_UNITS}")


def n_units() -> int:
    f = UNITS / "_units"
    if not f.exists():
        raise SystemExit("scan has not started (no units/_units)")
    return int(f.read_text().strip())


def scan_complete() -> bool:
    n = n_units()
    return all((UNITS / f"u{i:04d}.tsv").exists() for i in range(n))


def n_batches() -> int:
    return -(-n_units() // UNIT_BATCH)


# â”€â”€ split â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

COLS = "{'bkt':'INTEGER','lo':'BIGINT','hi':'BIGINT','ply':'INTEGER','moves':'VARCHAR','flags':'INTEGER','fen':'VARCHAR'}"


def split_batch(j: int) -> None:
    out = SPLIT / f"batch={j}"
    if (out / "_DONE").exists():
        return
    units = [UNITS / f"u{i:04d}.tsv" for i in range(j * UNIT_BATCH, min(n_units(), (j + 1) * UNIT_BATCH))]
    missing = [u.name for u in units if not u.exists()]
    if missing:
        raise SystemExit(f"batch {j}: units missing {missing[:5]}")
    tmp = SPLIT / f"batch={j}.tmp"
    if tmp.exists():
        import shutil
        shutil.rmtree(tmp)
    SPLIT.mkdir(parents=True, exist_ok=True)
    con, _ = im.connect("24GB")
    lit = ", ".join("'" + u.as_posix() + "'" for u in units)
    t = time.time()
    con.execute(f"""COPY (SELECT * FROM read_csv([{lit}], delim='\t', header=false, quote='',
                     columns={COLS}, null_padding=false, auto_detect=false))
                    TO '{tmp.as_posix()}' (FORMAT parquet, PARTITION_BY (bkt), ROW_GROUP_SIZE 262144)""")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{tmp.as_posix()}/*/*.parquet')").fetchone()[0]
    con.close()
    (tmp / "_DONE").write_text(json.dumps(dict(units=len(units), rows=n)), encoding="utf8")
    tmp.replace(out)
    im.log(f"split batch {j}: {len(units)} units, {n:,} rows in {time.time() - t:.0f}s")


# â”€â”€ join â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def decide(rows: list[tuple]) -> tuple[list[tuple], dict]:
    """rows: (lo, hi, flags, fen, moves) with flags != 0. Returns ([(lo, hi, flags, fen, keep)], counts);
    raises on a sampled row whose key or moves do not check out."""
    import chess
    from zobrist128 import zobrist128
    out, c = [], dict(flag1=0, flag1_kept=0, flag2=0, flag2_kept=0, sample=0, sample_moves=0)
    bad = []
    for lo, hi, flags, fen, moves in rows:
        board = chess.Board(fen)
        keep = True
        key_board = board
        if flags & 1:                                  # restored ep: which square gives the book key?
            c["flag1"] += 1
            match = None
            for f in range(8):
                sq = chess.square(f, 5 if board.turn == chess.WHITE else 2)
                b2 = board.copy(stack=False)
                b2.ep_square = sq
                if b2.has_pseudo_legal_en_passant() and zobrist128(b2) == (lo, hi):
                    match = b2
            if match is None:
                bad.append(("flag1-no-match", lo, hi, fen))
                continue
            keep = not match.has_legal_en_passant()
            key_board = match
            c["flag1_kept"] += keep
        if flags & 2:
            c["flag2"] += 1
            keep = board.has_legal_en_passant()
            c["flag2_kept"] += keep
        if flags & 4:
            c["sample"] += 1
            if zobrist128(key_board) != (lo, hi):
                bad.append(("key", lo, hi, fen))
            for item in (moves.split(";") if moves else []):
                c["sample_moves"] += 1
                try:
                    ok = board.is_legal(chess.Move.from_uci(item.split(":")[0]))
                except ValueError:
                    ok = False
                if not ok:
                    bad.append(("move", lo, hi, fen, item))
        out.append((lo, hi, flags, fen, keep))
    if bad:
        raise SystemExit(f"{len(bad)} bad scan rows, first: {bad[:3]}")
    return out, c


def resolve_by_lookup(b: int, keys: list[tuple[int, int]]) -> dict[tuple[int, int], tuple[int, str]]:
    """The lookup path's own answer (cdb_lookup on the book's EPD) for a few keys: {(lo, hi): (ply, moves)}."""
    import duckdb
    from explorer_storage import relation
    files = sorted(im.BOOK.glob(f"ps/event=*/elo_band=*/bkt{b:03d}.parquet"))
    con = duckdb.connect()
    lines = []
    for lo, hi in keys:                         # one equality query per key: the sorted book prunes row groups
        e0, e1, n = con.execute(f"SELECT min(parent_epd), max(parent_epd), count(*) FROM {relation(files)} "
                                f"WHERE parent_hash = {lo} AND parent_hash_hi = {hi}").fetchone()
        if not n or e0 != e1:
            raise SystemExit(f"bucket {b}: key ({lo}, {hi}) has {n} book rows, EPDs {e0!r} / {e1!r}")
        lines.append(f"{lo}\t{hi}\t{e0}\n")
    con.close()
    d = im.WORK / "dups"
    d.mkdir(parents=True, exist_ok=True)
    src, out = d / f"bkt{b:03d}.tsv", d / f"bkt{b:03d}.out.tsv"
    src.write_text("".join(lines), encoding="utf8", newline="\n")
    out.unlink(missing_ok=True)
    cmd = f"bash {im.CPP}/mount.sh >/dev/null && {im.LOOKUP} {im.wsl_path(src)} {im.wsl_path(out)} 1"
    r = subprocess.run(["wsl", "-d", "Ubuntu", "--", "bash", "-c", cmd], capture_output=True, text=True)
    if r.returncode != 0 or not out.exists():
        raise SystemExit(f"bucket {b}: cdb_lookup for duplicate keys failed rc={r.returncode}: {r.stderr[-1000:]}")
    res = {}
    for line in out.read_text(encoding="utf8").splitlines():
        lo, hi, ply, moves = (line.split("\t") + [""])[:4]
        res[(int(lo), int(hi))] = (int(ply), moves)
    if set(res) != set(keys):
        raise SystemExit(f"bucket {b}: cdb_lookup answered {len(res)} of {len(keys)} duplicate keys")
    im.log(f"bucket {b}: {len(keys)} keys with two ChessDB entries resolved by the lookup")
    return res


def join_one(b: int, root: Path | None = None) -> None:
    """root=None writes the real outputs (OUT); a root writes there (comparison)."""
    import pandas as pd
    p = im.paths(b)
    if root is not None:
        p = dict(p, positions=root / "positions" / f"bkt{b:03d}.parquet",
                 moves=root / "moves" / f"bkt{b:03d}.parquet", done=root / "_done" / f"bkt{b:03d}.json")
    if p["done"].exists():
        return
    batches = sorted(SPLIT.glob("batch=*"))
    if len(batches) != n_batches() or not all((d / "_DONE").exists() for d in batches):
        raise SystemExit("split not complete")
    kp = KEYS / f"bkt{b:03d}.parquet"
    files = [f.as_posix() for d in batches for f in (d / f"bkt={b}").glob("*.parquet")]
    con, _ = im.connect()
    con.execute(f"CREATE TEMP TABLE i AS SELECT lo, hi FROM read_parquet('{kp.as_posix()}')")
    n_i = con.execute("SELECT count(*) FROM i").fetchone()[0]
    lit = ", ".join(f"'{f}'" for f in files)
    con.execute(f"""CREATE TEMP TABLE s AS SELECT s.lo, s.hi, s.ply, coalesce(s.moves, '') AS moves, s.flags, s.fen
                    FROM read_parquet([{lit}]) s SEMI JOIN i USING (lo, hi)""")
    flagged = con.execute("SELECT lo, hi, flags, fen, moves FROM s WHERE flags <> 0").fetchall()
    decided, counts = decide(flagged)
    d = pd.DataFrame(decided, columns=["lo", "hi", "flags", "fen", "keep"])
    con.register("d", d)
    con.execute("""CREATE TEMP TABLE s2 AS
                   SELECT s.lo, s.hi, s.ply, s.moves FROM s LEFT JOIN d USING (lo, hi, flags, fen)
                   WHERE s.flags = 0 OR d.keep""")
    con.unregister("d")
    dups = con.execute("SELECT lo, hi FROM s2 GROUP BY 1, 2 HAVING count(*) > 1 ORDER BY 1, 2").fetchall()
    if dups:
        # ChessDB can hold one position under two encodings (seen in bucket 40, 2026-11-22: two entries, different
        # moves and distances, same position); the lookup probed the book EPD's canonical encoding and so hit
        # exactly one. Ask the lookup itself, and keep the scan row equal to its answer.
        for (lo, hi), (ply, moves) in resolve_by_lookup(b, dups).items():
            n = con.execute("SELECT count(*) FROM s2 WHERE lo = ? AND hi = ? AND ply = ? AND moves = ?",
                            [lo, hi, ply, moves]).fetchone()[0]
            if n != 1:
                raise SystemExit(f"bucket {b}: key ({lo}, {hi}) has two scan rows and {n} equal the lookup's answer")
            con.execute("DELETE FROM s2 WHERE lo = ? AND hi = ? AND NOT (ply = ? AND moves = ?)", [lo, hi, ply, moves])
    left = con.execute("SELECT count(*) FROM (SELECT lo, hi FROM s2 GROUP BY 1, 2 HAVING count(*) > 1)").fetchone()[0]
    if left:
        raise SystemExit(f"bucket {b}: {left} keys with two scan rows")
    con.execute("""CREATE TEMP TABLE o AS SELECT i.lo, i.hi, coalesce(s2.ply, -2) AS ply,
                   coalesce(s2.moves, '') AS moves FROM i LEFT JOIN s2 USING (lo, hi)""")
    chk = dict(sample=counts["sample"], sample_moves=counts["sample_moves"], bad_key=0, bad_move=0,
               duplicate_keys_resolved_by_lookup=len(dups),
               ep_variant_rows=counts["flag1"], ep_variant_kept=counts["flag1_kept"],
               ep_fen_rows=counts["flag2"], ep_fen_kept=counts["flag2_kept"])
    im.finish_bucket(con, b, p, n_i, chk, k_path=kp, stm_sql="k.first_ply % 2 = 1",
                     extra=dict(method="scan", scan_rows=len(flagged)))


def compare_one(b: int) -> dict:
    import duckdb
    ref = im.paths(b)
    if not ref["done"].exists():
        raise SystemExit(f"bucket {b}: no lookup-path output to compare with")
    join_one(b, root=CMP)
    con = duckdb.connect()
    res = dict(bucket=b)
    for kind in ("positions", "moves"):
        a, c = ref[kind].as_posix(), (CMP / kind / f"bkt{b:03d}.parquet").as_posix()
        na, nc = (con.execute(f"SELECT count(*) FROM read_parquet('{x}')").fetchone()[0] for x in (a, c))
        only_ref = con.execute(f"SELECT count(*) FROM (SELECT * FROM read_parquet('{a}') EXCEPT ALL "
                               f"SELECT * FROM read_parquet('{c}'))").fetchone()[0]
        only_scan = con.execute(f"SELECT count(*) FROM (SELECT * FROM read_parquet('{c}') EXCEPT ALL "
                                f"SELECT * FROM read_parquet('{a}'))").fetchone()[0]
        res[kind] = dict(lookup=na, scan=nc, only_lookup=only_ref, only_scan=only_scan)
    im.log(f"compare {b}: {res}")
    f = CMP / "_cmp" / f"bkt{b:03d}.json"
    f.parent.mkdir(parents=True, exist_ok=True)
    f.with_name(f.name + ".tmp").write_text(json.dumps(res), encoding="utf8")
    f.with_name(f.name + ".tmp").replace(f)
    return res


def lookup_buckets() -> list[int]:
    """Buckets the lookup path finished (their _done record has no 'method')."""
    out = []
    for f in sorted((im.OUT / "_done").glob("bkt*.json")):
        if "method" not in json.loads(f.read_text(encoding="utf8")):
            out.append(int(f.stem[3:]))
    return out


def chain(key_workers: int, join_workers: int) -> None:
    """keys -> scan -> split -> compare every lookup-made bucket (gate: zero differences) -> join."""
    im.log("chain: keys")
    fan_out("keys-one", [b for b in range(512) if not (KEYS / f"bkt{b:03d}.parquet").exists()], key_workers)
    if not ((UNITS / "_units").exists() and scan_complete()):
        im.log("chain: scan")
        cmd = scan_cmd() + f" >> {wsl(SCAN / 'scan.log')} 2>&1"
        r = subprocess.run(["wsl", "-d", "Ubuntu", "--", "bash", "-c", cmd])
        if r.returncode != 0 or not scan_complete():
            raise SystemExit(f"scan failed rc={r.returncode} (see {SCAN / 'scan.log'})")
    im.log(f"chain: split ({n_batches()} batches)")
    for j in range(n_batches()):
        if not (SPLIT / f"batch={j}" / "_DONE").exists():
            stage("split-one", j)
    ref = lookup_buckets()
    im.log(f"chain: compare {len(ref)} lookup-made buckets")
    fan_out("compare-one", [b for b in ref if not (CMP / "_cmp" / f"bkt{b:03d}.json").exists()], join_workers)
    bad = []
    for b in ref:
        r = json.loads((CMP / "_cmp" / f"bkt{b:03d}.json").read_text(encoding="utf8"))
        for kind in ("positions", "moves"):
            x = r[kind]
            if x["only_lookup"] or x["only_scan"] or x["lookup"] != x["scan"]:
                bad.append((b, kind, x))
    if bad or not ref:
        im.log(f"chain: STOPPED at the comparison gate: {len(bad)} differing files, first {bad[:3]}")
        raise SystemExit(1)
    im.log(f"chain: comparison gate passed ({len(ref)} buckets, zero differences); join")
    im.write_params()
    fan_out("join-one", [b for b in range(512) if not im.paths(b)["done"].exists()], join_workers)
    if all(im.paths(b)["done"].exists() for b in range(512)):
        from chessdb_publication import publish
        publish(im.OUT)
    im.log("chain finished")


# â”€â”€ driver â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def stage(name: str, b: int) -> None:
    r = subprocess.run([sys.executable, "-X", "utf8", __file__, name, "--bucket", str(b)])
    if r.returncode != 0:
        raise RuntimeError(f"{name} {b} failed rc={r.returncode}")


def fan_out(name: str, buckets: list[int], workers: int) -> None:
    # one task per worker in flight, and stop on the first failure (CLAUDE.md: no queued pools)
    todo = list(buckets)
    with cf.ThreadPoolExecutor(workers) as ex:
        live = {ex.submit(stage, name, todo.pop(0)) for _ in range(min(workers, len(todo)))}
        while live:
            done, live = cf.wait(live, return_when=cf.FIRST_COMPLETED)
            for f in done:
                f.result()                              # raises -> the with-block ends after live tasks
            while todo and len(live) < workers:
                live.add(ex.submit(stage, name, todo.pop(0)))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("phase", choices=["keys", "scan-cmd", "split", "join", "compare", "chain",
                                      "keys-one", "split-one", "join-one", "compare-one"])
    ap.add_argument("--join-workers", type=int, default=3)
    ap.add_argument("--bucket", type=int)
    ap.add_argument("--buckets", type=int, nargs="*")
    ap.add_argument("--workers", type=int, default=3)
    a = ap.parse_args()
    buckets = a.buckets if a.buckets is not None else list(range(512))
    if a.phase == "keys-one":
        keys_one(a.bucket)
    elif a.phase == "split-one":
        split_batch(a.bucket)                           # --bucket is the batch index here
    elif a.phase == "chain":
        chain(a.workers, a.join_workers)
    elif a.phase == "join-one":
        join_one(a.bucket)
    elif a.phase == "compare-one":
        print(json.dumps(compare_one(a.bucket)))
    elif a.phase == "keys":
        fan_out("keys-one", [b for b in buckets if not (KEYS / f"bkt{b:03d}.parquet").exists()], a.workers)
        im.log("keys finished")
    elif a.phase == "scan-cmd":
        print(scan_cmd())
    elif a.phase == "split":
        if not scan_complete():
            raise SystemExit("scan not complete")
        for j in range(n_batches()):
            split_batch(j)
    elif a.phase == "join":
        im.write_params()
        fan_out("join-one", [b for b in buckets if not im.paths(b)["done"].exists()], a.workers)
        im.log("join finished")
    elif a.phase == "compare":
        fan_out("compare-one", buckets, a.workers)


if __name__ == "__main__":
    main()
