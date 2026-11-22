"""ChessDB move-menu import (docs/move-menus-plan-2026-11-21.md).

For every position of the ply-51 explorer book with >= 2 games (summed over slices and plies), look
the position up in the ChessDB snapshot and keep its whole scored-move list.

Per book bucket b (0..511), each stage skip-gated on its output:
  export   book bucket -> WORK/pos/bkt{b}.tsv (lo, hi, epd; sorted by key) + bkt{b}.parquet
           (lo, hi, games, first_ply). Keys are grouped first, EPDs fetched for kept keys only.
           Refuses a key with two EPDs.
  lookup   WSL cdb_lookup (scratch/chessdb_import/cpp) -> WORK/out/bkt{b}.tsv (lo, hi, ply, moves)
  convert  -> OUT/positions/bkt{b}.parquet and OUT/moves/bkt{b}.parquet, after checks on a seeded
           sample: the key recomputed from the EPD equals (lo, hi) (row alignment), and every ChessDB
           move is legal in the position (orientation: ChessDB stores a position with its
           colour-mirror). A key that matches only once a pseudo-legal but illegal en passant square
           is restored counts as ep_lossy, not bad (the key hashes pseudo-legal ep, the EPD writes
           legal ep only; bucket 28 had the first, 11-21). OUT/_done/bkt{b}.json (counts, sha256s) is written LAST; then the WORK
           files for b are deleted.

Output read contract (OUT/README.md): positions: lo, hi (our 128-bit key's signed halves), games,
first_ply (book), white_to_move, found, cdb_ply (ChessDB's distance for this side; null if unknown or
not found), n_moves, best_score. moves: lo, hi, rank (1 = ChessDB's best), uci, score: side-to-move
view, ChessDB backprop scale (|score| >= 15000 is a mate or tablebase-win claim; 30000 - |score| is
the mate distance in plies where it is a mate; 0 includes tablebase draws and stalemates).

Usage (pinned code for imports; long runs launched detached):
  python import_menus.py run [--buckets 0 1 ...] [--export-workers 2] [--threads 48]
  python import_menus.py export-one|lookup-one|convert-one --bucket B
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os

import subprocess
import sys
import threading
import time
from pathlib import Path

PIN = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIN))

BOOK = Path("E:/chess/position-stats/brc-p51-train-201301-202607-20261115")
WORK = Path("D:/chess/chessdb-import/work")
OUT = Path("E:/chess/chessdb-menus-20260702")
_cpp_path = Path(__file__).resolve().parent / "cpp"
CPP = "/mnt/" + _cpp_path.as_posix()[0].lower() + _cpp_path.as_posix()[2:]
LOOKUP = "/opt/chessdb-pilot/import/cdb_lookup"
MIN_GAMES = 2
SNAPSHOT = dict(repo="chessdb/snapshots", revision="9de4573da712dbf0aaecadf62d164f7ba07e8145",
                dir="chess-20260702", entries=63996865341)
SAMPLE = 2000          # rows checked per bucket (key recompute + move legality)
LOG = Path("D:/chess/chessdb-import/import.log")


def log(msg: str) -> None:
    line = f"[{datetime.datetime.now():%m-%d %H:%M:%S}] {msg}"
    print(line, flush=True)
    with LOG.open("a", encoding="utf8") as f:
        f.write(line + "\n")


def wsl_path(p: Path) -> str:
    s = p.as_posix()
    return f"/mnt/{s[0].lower()}{s[2:]}"


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda: f.read(1 << 22), b""):
            h.update(b)
    return h.hexdigest()


def paths(b: int) -> dict:
    return dict(pos_tsv=WORK / "pos" / f"bkt{b:03d}.tsv", pos_pq=WORK / "pos" / f"bkt{b:03d}.parquet",
                out_tsv=WORK / "out" / f"bkt{b:03d}.tsv",
                positions=OUT / "positions" / f"bkt{b:03d}.parquet",
                moves=OUT / "moves" / f"bkt{b:03d}.parquet", done=OUT / "_done" / f"bkt{b:03d}.json")


def connect(mem: str = "12GB"):
    import duckdb
    con = duckdb.connect()
    tmp = Path(f"D:/chess_duckdb_tmp/cdb_import_{os.getpid()}")
    con.execute(f"SET memory_limit='{mem}'; SET threads=4; SET preserve_insertion_order=false; "
                f"SET temp_directory='{tmp.as_posix()}'")
    return con, tmp


# â”€â”€ stage 1: export â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def export_one(b: int) -> None:
    from explorer_storage import relation
    p = paths(b)
    if p["pos_tsv"].exists() and p["pos_pq"].exists():
        return
    files = sorted(BOOK.glob(f"ps/event=*/elo_band=*/bkt{b:03d}.parquet"))
    if not files:
        raise SystemExit(f"bucket {b}: no book files")
    con, tmp = connect()
    lit = ", ".join("'" + f.as_posix() + "'" for f in files)
    con.execute(f"""CREATE TEMP TABLE k AS SELECT parent_hash AS lo, parent_hash_hi AS hi,
                    sum(total)::BIGINT AS games, min(ply)::SMALLINT AS first_ply
                    FROM read_parquet([{lit}], hive_partitioning=false) GROUP BY 1, 2
                    HAVING sum(total) >= {MIN_GAMES}""")
    con.execute(f"""CREATE TEMP TABLE e AS SELECT parent_hash AS lo, parent_hash_hi AS hi,
                    min(parent_epd) AS epd, max(parent_epd) AS epd_max
                    FROM {relation(files)} WHERE (parent_hash, parent_hash_hi) IN (SELECT lo, hi FROM k)
                    GROUP BY 1, 2""")
    nk, ne, two = con.execute("SELECT (SELECT count(*) FROM k), count(*), count(*) FILTER (epd <> epd_max) "
                              "FROM e").fetchone()
    if nk != ne or two:
        raise SystemExit(f"bucket {b}: {nk} keys, {ne} with an EPD, {two} keys with two EPDs")
    p["pos_tsv"].parent.mkdir(parents=True, exist_ok=True)
    for name, sql, fmt in (
            ("pos_pq", "SELECT * FROM k ORDER BY lo, hi", "(FORMAT parquet)"),
            ("pos_tsv", "SELECT lo, hi, epd FROM e ORDER BY lo, hi", "(FORMAT csv, DELIMITER '\t', HEADER false, QUOTE '')")):
        t = p[name].with_name(p[name].name + ".tmp")
        con.execute(f"COPY ({sql}) TO '{t.as_posix()}' {fmt}")
        t.replace(p[name])
    con.close()
    log(f"export {b}: {nk:,} positions")


# â”€â”€ stage 2: lookup â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def lookup_one(b: int, threads: int) -> None:
    p = paths(b)
    if p["out_tsv"].exists():
        return
    p["out_tsv"].parent.mkdir(parents=True, exist_ok=True)
    cmd = (f"bash {CPP}/mount.sh >/dev/null && {LOOKUP} {wsl_path(p['pos_tsv'])} "
           f"{wsl_path(p['out_tsv'])} {threads}")
    t = time.time()
    r = subprocess.run(["wsl", "-d", "Ubuntu", "--", "bash", "-c", cmd], capture_output=True, text=True)
    if r.returncode != 0 or not p["out_tsv"].exists():
        raise SystemExit(f"bucket {b}: cdb_lookup failed rc={r.returncode}: {r.stderr[-2000:]}")
    log(f"lookup {b}: {r.stdout.strip()} in {time.time()-t:.0f}s")


# â”€â”€ stage 3: convert + check â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def illegal_ep_key(board, key: tuple, zobrist128) -> bool:
    """The key is the board's with an en passant square the EPD legitimately omits.

    Our key hashes the ep square when an ep capture is PSEUDO-legal (polyglot convention, shakmaty
    PseudoLegal); the book's EPD writes it only when the capture is LEGAL. So a position whose last move
    was a double push next to an enemy pawn that cannot legally take (it is pinned, say) has a key that
    differs from the key recomputed from its EPD. Accept exactly that case: some candidate ep square
    makes the keys equal, an ep capture onto it is pseudo-legal, and none is legal."""
    import chess
    if board.ep_square is not None:
        return False
    white = board.turn == chess.WHITE
    rank5, rank6, rank7 = (4, 5, 6) if white else (3, 2, 1)
    them = not board.turn
    for f in range(8):
        sq5, sq6, sq7 = chess.square(f, rank5), chess.square(f, rank6), chess.square(f, rank7)
        if board.piece_at(sq5) != chess.Piece(chess.PAWN, them) or board.piece_at(sq6) or board.piece_at(sq7):
            continue
        b2 = board.copy(stack=False)
        b2.ep_square = sq6
        if (b2.has_pseudo_legal_en_passant() and not b2.has_legal_en_passant()
                and zobrist128(b2) == key):
            return True
    return False


def check_sample(rows: list[tuple]) -> dict:
    import chess
    from zobrist128 import zobrist128
    bad_key = bad_move = moves = ep_lossy = 0
    for lo, hi, epd, mv in rows:
        board = chess.Board(epd)
        if zobrist128(board) != (lo, hi):
            if illegal_ep_key(board, (lo, hi), zobrist128):
                ep_lossy += 1
            else:
                bad_key += 1
        for item in (mv.split(";") if mv else []):
            moves += 1
            uci = item.split(":")[0]
            try:
                ok = board.is_legal(chess.Move.from_uci(uci))
            except ValueError:
                ok = False
            bad_move += not ok
    return dict(sample=len(rows), sample_moves=moves, bad_key=bad_key, bad_move=bad_move, ep_lossy=ep_lossy)


def convert_one(b: int) -> None:
    p = paths(b)
    if p["done"].exists():
        return
    con, tmp = connect()
    pos, out = p["pos_tsv"].as_posix(), p["out_tsv"].as_posix()
    con.execute(f"""CREATE TEMP TABLE i AS SELECT * FROM read_csv('{pos}', delim='\t', header=false, quote='',
                    columns={{'lo':'BIGINT','hi':'BIGINT','epd':'VARCHAR'}})""")
    con.execute(f"""CREATE TEMP TABLE o AS SELECT * FROM read_csv('{out}', delim='\t', header=false, quote='',
                    columns={{'lo':'BIGINT','hi':'BIGINT','ply':'INTEGER','moves':'VARCHAR'}})""")
    n_i, n_o, n_j = con.execute("""SELECT (SELECT count(*) FROM i), (SELECT count(*) FROM o),
                                   (SELECT count(*) FROM i JOIN o USING (lo, hi))""").fetchone()
    if not (n_i == n_o == n_j):
        raise SystemExit(f"bucket {b}: in {n_i}, out {n_o}, joined {n_j}")
    rows = con.execute(f"""SELECT i.lo, i.hi, i.epd, coalesce(o.moves, '') FROM i JOIN o USING (lo, hi)
                           ORDER BY hash(i.lo, i.hi, {20261121 + b}) LIMIT {SAMPLE}""").fetchall()
    chk = check_sample(rows)
    if chk["bad_key"] or chk["bad_move"]:
        raise SystemExit(f"bucket {b}: sample check failed {chk}")
    finish_bucket(con, b, p, n_i, chk, k_path=p["pos_pq"], stm_sql="split_part(i.epd, ' ', 2) = 'w'")
    for k in ("pos_tsv", "pos_pq", "out_tsv"):
        p[k].unlink(missing_ok=True)


def finish_bucket(con, b: int, p: dict, n_i: int, chk: dict, k_path: Path, stm_sql: str,
                  extra: dict | None = None) -> None:
    """Shared by both paths. Temp tables i (lo, hi[, epd]) and o (lo, hi, ply, moves: one row per key in i)
    and the export's key parquet k_path (lo, hi, games, first_ply) -> p['positions'], p['moves'], and
    p['done'] written LAST. stm_sql: the white-to-move expression over aliases i and k."""
    con.execute(f"""CREATE TEMP TABLE pos AS
        SELECT i.lo, i.hi, k.games, k.first_ply, {stm_sql} AS white_to_move,
               o.ply > -2 AS found, CASE WHEN o.ply >= 0 THEN o.ply::SMALLINT END AS cdb_ply,
               CASE WHEN coalesce(o.moves, '') = '' THEN 0 ELSE len(string_split(o.moves, ';')) END::SMALLINT AS n_moves,
               CASE WHEN coalesce(o.moves, '') = '' THEN NULL
                    ELSE split_part(split_part(o.moves, ';', 1), ':', 2)::SMALLINT END AS best_score
        FROM i JOIN o USING (lo, hi) JOIN read_parquet('{k_path.as_posix()}') k USING (lo, hi)""")
    n_pos = con.execute("SELECT count(*) FROM pos").fetchone()[0]
    if n_pos != n_i:
        raise SystemExit(f"bucket {b}: {n_pos} positions after joining the export's games, expected {n_i}")
    con.execute("""CREATE TEMP TABLE mv AS
        SELECT lo, hi, (generate_subscripts(m, 1))::SMALLINT AS rank, unnest(m) AS item
        FROM (SELECT lo, hi, string_split(moves, ';') AS m FROM o WHERE coalesce(moves, '') <> '')""")
    con.execute("""CREATE TEMP TABLE mv2 AS SELECT lo, hi, rank, split_part(item, ':', 1) AS uci,
                   split_part(item, ':', 2)::SMALLINT AS score FROM mv""")
    stats = con.execute("""SELECT count(*) FILTER (found), sum(n_moves), count(*) FILTER (cdb_ply IS NOT NULL)
                           FROM pos""").fetchone()
    n_mv = con.execute("SELECT count(*) FROM mv2").fetchone()[0]
    if n_mv != stats[1]:
        raise SystemExit(f"bucket {b}: {n_mv} move rows vs n_moves sum {stats[1]}")
    for key, table, order in (("positions", "pos", "lo, hi"), ("moves", "mv2", "lo, hi, rank")):
        p[key].parent.mkdir(parents=True, exist_ok=True)
        t = p[key].with_name(p[key].name + ".tmp")
        con.execute(f"COPY (SELECT * FROM {table} ORDER BY {order}) TO '{t.as_posix()}' "
                    f"(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1048576)")
        t.replace(p[key])
    con.close()
    done = dict(bucket=b, positions=n_pos, found=stats[0], moves=n_mv, with_ply=stats[2], **chk, **(extra or {}),
                positions_sha256=sha256(p["positions"]), moves_sha256=sha256(p["moves"]),
                finished=datetime.datetime.now().isoformat(timespec="seconds"))
    p["done"].parent.mkdir(parents=True, exist_ok=True)
    t = p["done"].with_name(p["done"].name + ".tmp")
    t.write_text(json.dumps(done, indent=1), encoding="utf8")
    t.replace(p["done"])
    log(f"convert {b}: {n_pos:,} positions, {stats[0]:,} found ({stats[0]/n_pos:.1%}), {n_mv:,} moves"
        + (f" [{extra.get('method')}]" if extra else ""))


# â”€â”€ driver â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def stage(name: str, b: int, extra: list[str] = ()) -> None:
    """Each stage in a fresh process (DuckDB decays across big queries in one process)."""
    r = subprocess.run([sys.executable, "-X", "utf8", __file__, name, "--bucket", str(b), *extra])
    if r.returncode != 0:
        raise RuntimeError(f"{name} {b} failed rc={r.returncode}")


def write_params() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    params = dict(book=str(BOOK), book_meta_sha256=sha256(BOOK / "_book.meta.json"), min_games=MIN_GAMES,
                  snapshot=SNAPSHOT, reader="cdbdirect e98117a (+ checksum-verifying get), TerarkDB 8710cf38",
                  tool=dict(cdb_lookup_sha256=sha256(Path(__file__).parent / "cpp" / "cdb_lookup.cpp"),
                            driver_sha256=sha256(Path(__file__))))
    f = OUT / "_params.json"
    if f.exists():
        old = json.loads(f.read_text(encoding="utf8"))
        for k in ("book_meta_sha256", "min_games", "snapshot"):
            if old[k] != params[k]:
                raise SystemExit(f"_params.json differs on {k}: refusing to mix runs")
        return
    f.write_text(json.dumps(params, indent=1), encoding="utf8")


def run(buckets: list[int], export_workers: int, threads: int, lead: int) -> None:
    write_params()
    todo = [b for b in buckets if not paths(b)["done"].exists()]
    log(f"run: {len(todo)} of {len(buckets)} buckets to do")
    exported: dict[int, threading.Event] = {b: threading.Event() for b in todo}
    looked: dict[int, threading.Event] = {b: threading.Event() for b in todo}
    errors: list[str] = []
    gate = threading.Semaphore(lead)            # exported but not yet looked up

    def exporter(mine: list[int]):
        for b in mine:
            if errors:
                return
            gate.acquire()
            try:
                stage("export-one", b)
            except Exception as e:
                errors.append(str(e)); exported[b].set(); return
            exported[b].set()

    def converter_wait_and_run(b: int):
        try:
            stage("convert-one", b)
        except Exception as e:
            errors.append(str(e))

    ex = [threading.Thread(target=exporter, args=(todo[i::export_workers],)) for i in range(export_workers)]
    for t in ex:
        t.start()
    conv: threading.Thread | None = None
    for b in todo:
        exported[b].wait()
        if errors:
            break
        try:
            stage("lookup-one", b, ["--threads", str(threads)])
        except Exception as e:
            errors.append(str(e)); break
        finally:
            gate.release()
        if conv is not None:
            conv.join()
        conv = threading.Thread(target=converter_wait_and_run, args=(b,))
        conv.start()
    if conv is not None:
        conv.join()
    for t in ex:
        t.join()
    if errors:
        log(f"STOPPED: {errors}")
        raise SystemExit(1)
    if all(paths(b)["done"].exists() for b in range(512)):
        from chessdb_publication import publish
        publish(OUT)
    log("run finished")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("phase", choices=["run", "export-one", "lookup-one", "convert-one"])
    ap.add_argument("--bucket", type=int)
    ap.add_argument("--buckets", type=int, nargs="*")
    ap.add_argument("--export-workers", type=int, default=2)
    ap.add_argument("--threads", type=int, default=48)
    ap.add_argument("--lead", type=int, default=4, help="max exported-but-not-looked-up buckets")
    a = ap.parse_args()
    LOG.parent.mkdir(parents=True, exist_ok=True)
    if a.phase == "run":
        run(a.buckets if a.buckets else list(range(512)), a.export_workers, a.threads, a.lead)
    elif a.phase == "export-one":
        export_one(a.bucket)
    elif a.phase == "lookup-one":
        lookup_one(a.bucket, a.threads)
    else:
        convert_one(a.bucket)


if __name__ == "__main__":
    main()
