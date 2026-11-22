"""Lichess cloud multi-PV rows into the move-menu table (docs/move-menus-plan-2026-11-21.md, step 3).

The eval DB keeps only the deepest cloud analysis's first PV; the cloud dataset holds ~7 PV rows per
position over ~2 analyses. For every book position with >= 2 games (the menus' scope: the scan path's
key files), keep every move that is the first move of some cloud PV, scored by the DEEPEST analysis
containing it (ties: more knodes, then the PV best for the side to move -- Lichess lists PVs best first, so
this is the eval DB's "first row in file order" when one analysis repeats a first move).

Phases, each resumable (an output exists only if complete). One dataset file at a time, so every FEN join
stays small (a file holds ~15M distinct FENs; the dataset ~200-300M):
  files    per dataset file i: distinct FENs -> WSL cpp/fen_keys (our 128-bit key; keytest: 0 bad of
           9,335,934 book keys; a sample also recomputed in python-chess) -> FEN map, en passant made exact
           with python-chess (a FEN whose ep capture is illegal is the same position as the FEN without
           it, so it maps to both keys: the book keys a position by pseudo-legal ep, a FEN may have either;
           castling differing from the cleaned rights, i.e. not standard chess, and unparseable FENs are
           dropped) -> the file's PV rows by bucket, WORK/rows/batch=i/bkt=b/*.parquet
           (lo, hi, uci, depth, knodes, cp, mate); _DONE (with counts) written last
  bucket   per bucket: keep book positions (>= 2 games), one row per (position, move) ->
           OUT/cloud_moves/bktNNN.parquet, then OUT/cloud_done/bktNNN.json LAST; needs the 512 key files

Output (OUT/cloud_moves): lo, hi, uci, cp_white, mate_white (the dataset's own White-view values; exactly
one of the two is set), depth, knodes (of the analysis chosen), n_analyses (cloud analyses with this move
as a PV's first move).

Usage: python cloud_menus.py all|files|bucket [--workers 3]   |   file-one|bucket-one --bucket N
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime
import json
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import import_menus as im  # noqa: E402

CLOUD = Path("H:/chess/evals/cloud_2026-10-07")
WORK = Path("E:/chess/chessdb-scan-20261121/cloud_work")
KEYS = Path("E:/chess/chessdb-scan-20261121/keys")
OUT = Path(os.environ.get("CLOUD_MENUS_OUT", str(im.OUT)))   # override: tests only
TOOL = "/opt/chessdb-pilot/import/fen_keys"


def files() -> list[str]:
    fs = sorted(CLOUD.glob("data_*.parquet"))
    if len(fs) != 20 or not (CLOUD / "_DOWNLOAD.DONE").exists():
        raise SystemExit(f"cloud dataset incomplete: {len(fs)} files")
    return [f.as_posix() for f in fs]


def lit(fs: list[str]) -> str:
    return "[" + ", ".join(f"'{f}'" for f in fs) + "]"


def ep_variants(fen: str, flag: int) -> list[tuple[int, int, int]]:
    """Extra (lo, hi, variant) keys for a FEN that is the same position under another ep square.
    flag 1: no ep in the FEN; each restorable ep square whose capture is pseudo-legal but illegal.
    flag 2: the FEN's ep capture is pseudo-legal; if illegal, the position without it too."""
    import chess
    from zobrist128 import zobrist128
    b = chess.Board(fen)
    out = []
    if flag == 1:
        rank = 5 if b.turn == chess.WHITE else 2
        for f in range(8):
            b2 = b.copy(stack=False)
            b2.ep_square = chess.square(f, rank)
            if b2.has_pseudo_legal_en_passant() and not b2.has_legal_en_passant():
                out.append((*zobrist128(b2), 1))
    elif flag == 2 and not b.has_legal_en_passant():
        b2 = b.copy(stack=False)
        b2.ep_square = None
        out.append((*zobrist128(b2), 2))
    return out


def file_one(i: int) -> None:
    """One dataset file: its distinct FENs -> keys (C++) -> FEN map (ep exact) -> its rows by bucket."""
    import chess
    import pandas as pd
    from zobrist128 import zobrist128
    d = WORK / f"f{i:02d}"
    rows_out = WORK / "rows" / f"batch={i}"
    if (rows_out / "_DONE").exists():
        return
    d.mkdir(parents=True, exist_ok=True)
    src = files()[i]
    con, _ = im.connect("12GB")
    t = time.time()
    fens = d / "fens.tsv"
    if not fens.exists():
        tmp = fens.with_name(fens.name + ".tmp")
        con.execute(f"""COPY (SELECT DISTINCT fen FROM read_parquet('{src}'))
                        TO '{tmp.as_posix()}' (FORMAT csv, DELIMITER '\t', HEADER false, QUOTE '')""")
        tmp.replace(fens)
    fk = d / "fenkeys.tsv"
    if not fk.exists():
        cmd = f"{TOOL} fenkeys {im.wsl_path(fens)} {im.wsl_path(fk)}"
        r = subprocess.run(["wsl", "-d", "Ubuntu", "--", "bash", "-c", cmd], capture_output=True, text=True)
        if r.returncode != 0 or not fk.exists():
            raise SystemExit(f"file {i}: fen_keys failed rc={r.returncode}: {r.stderr[-2000:]}")
    con.execute(f"""CREATE TEMP TABLE fk AS SELECT * FROM read_csv('{fk.as_posix()}', delim='\t', header=false,
                    quote='', columns={{'fen':'VARCHAR','lo':'BIGINT','hi':'BIGINT','flag':'INTEGER'}}, auto_detect=false)""")
    n_fk, n_fens = con.execute(f"""SELECT (SELECT count(*) FROM fk), (SELECT count(*) FROM read_csv('{fens.as_posix()}',
                                   delim='\t', header=false, quote='', columns={{'fen':'VARCHAR'}}, auto_detect=false))""").fetchone()
    if n_fk != n_fens:
        raise SystemExit(f"file {i}: {n_fens} FENs but {n_fk} key rows")
    flags = {str(k): v for k, v in con.execute("SELECT flag, count(*) FROM fk GROUP BY 1 ORDER BY 1").fetchall()}
    flagged = con.execute("SELECT fen, flag FROM fk WHERE flag IN (1, 2)").fetchall()
    extra = [(fen, lo, hi, v) for fen, flag in flagged for lo, hi, v in ep_variants(fen, flag)]
    sample = con.execute("SELECT fen, lo, hi FROM fk WHERE flag IN (0, 3) USING SAMPLE 5000 ROWS (reservoir, 7)").fetchall()
    bad = [(f, lo, hi) for f, lo, hi in sample if zobrist128(chess.Board(f)) != (lo, hi)]
    if bad:
        raise SystemExit(f"file {i}: {len(bad)} of {len(sample)} sampled keys differ from python-chess, first {bad[:2]}")
    con.register("x", pd.DataFrame(extra, columns=["fen", "lo", "hi", "variant"]) if extra else
                 pd.DataFrame({"fen": pd.Series([], dtype=str), "lo": pd.Series([], dtype="int64"),
                               "hi": pd.Series([], dtype="int64"), "variant": pd.Series([], dtype="int64")}))
    con.execute("""CREATE TEMP TABLE m AS SELECT fen, lo, hi FROM fk WHERE flag IN (0, 1, 2, 3)
                   UNION ALL SELECT fen, lo, hi FROM x""")
    con.unregister("x")
    tmp = WORK / "rows" / f"batch={i}.tmp"
    if tmp.exists():
        import shutil
        shutil.rmtree(tmp)
    tmp.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""COPY (SELECT (m.lo & 511)::INTEGER AS bkt, m.lo, m.hi, split_part(c.line, ' ', 1) AS uci,
                                 c.depth, c.knodes, c.cp, c.mate
                          FROM read_parquet('{src}') c JOIN m USING (fen) WHERE coalesce(c.line, '') <> '')
                    TO '{tmp.as_posix()}' (FORMAT parquet, PARTITION_BY (bkt), ROW_GROUP_SIZE 262144)""")
    n_rows = con.execute(f"SELECT count(*) FROM read_parquet('{tmp.as_posix()}/*/*.parquet')").fetchone()[0]
    n_src = con.execute(f"SELECT count(*) FROM read_parquet('{src}')").fetchone()[0]
    con.close()
    meta = dict(file=Path(src).name, source_rows=n_src, fens=n_fens, flags=flags, ep_flagged=len(flagged),
                ep_variant_keys=len(extra), python_sample=len(sample), rows_out=n_rows)
    (tmp / "_DONE").write_text(json.dumps(meta, indent=1), encoding="utf8")
    tmp.replace(rows_out)
    for f in (fens, fk):
        f.unlink(missing_ok=True)
    im.log(f"cloud file {i}: {meta} in {time.time() - t:.0f}s")


def bucket_one(b: int) -> None:
    done = OUT / "cloud_done" / f"bkt{b:03d}.json"
    if done.exists():
        return
    kp = KEYS / f"bkt{b:03d}.parquet"
    if not kp.exists():
        raise SystemExit(f"bucket {b}: book keys missing ({kp})")
    batches = sorted((WORK / "rows").glob("batch=*"))
    if len([x for x in batches if (x / "_DONE").exists() and not x.name.endswith(".tmp")]) != 20 \
            and not os.environ.get("CLOUD_ALLOW_PARTIAL"):            # tests only
        raise SystemExit("cloud rows incomplete: run the files phase")
    src = [f.as_posix() for x in batches if not x.name.endswith(".tmp") for f in (x / f"bkt={b}").glob("*.parquet")]
    con, _ = im.connect("8GB")
    out = OUT / "cloud_moves" / f"bkt{b:03d}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    rows_sql = (f"SELECT lo, hi, uci, depth, knodes, cp, mate FROM read_parquet({lit(src)})" if src else
                "SELECT NULL::BIGINT lo, NULL::BIGINT hi, NULL::VARCHAR uci, NULL::UTINYINT depth, "
                "NULL::INTEGER knodes, NULL::SMALLINT cp, NULL::TINYINT mate WHERE false")
    con.execute(f"""COPY (
        WITH r AS (SELECT DISTINCT r.lo, r.hi, r.uci, r.depth, r.knodes, r.cp, r.mate, b.first_ply % 2 = 1 AS wtm
                   FROM ({rows_sql}) r JOIN read_parquet('{kp.as_posix()}') b USING (lo, hi)),
             v AS (SELECT *, (CASE WHEN wtm THEN 1 ELSE -1 END) *
                             (CASE WHEN mate > 0 THEN 100000 - mate WHEN mate < 0 THEN -100000 - mate
                                   WHEN mate = 0 THEN -100000 ELSE cp END) AS mover_value FROM r),
             ranked AS (SELECT *, row_number() OVER w AS rn, count(*) OVER (PARTITION BY lo, hi, uci) AS n
                        FROM v WINDOW w AS (PARTITION BY lo, hi, uci
                                            ORDER BY depth DESC, knodes DESC, mover_value DESC))
        SELECT lo, hi, uci, cp AS cp_white, mate AS mate_white, depth, knodes, n::INTEGER AS n_analyses
        FROM ranked WHERE rn = 1 ORDER BY lo, hi, uci)
        TO '{tmp.as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
    n, npos, nmulti = con.execute(f"""SELECT count(*), count(DISTINCT (lo, hi)),
                                      count(*) FILTER ((cp_white IS NULL) = (mate_white IS NULL))
                                      FROM read_parquet('{tmp.as_posix()}')""").fetchone()
    con.close()
    if nmulti:
        raise SystemExit(f"bucket {b}: {nmulti} rows with neither or both of cp and mate")
    tmp.replace(out)
    done.parent.mkdir(parents=True, exist_ok=True)
    t = done.with_name(done.name + ".tmp")
    t.write_text(json.dumps(dict(bucket=b, moves=n, positions=npos, sha256=im.sha256(out),
                                 finished=datetime.datetime.now().isoformat(timespec="seconds")), indent=1),
                 encoding="utf8")
    t.replace(done)


def stage(name: str, b: int) -> None:
    r = subprocess.run([sys.executable, "-X", "utf8", __file__, name, "--bucket", str(b)])
    if r.returncode != 0:
        raise RuntimeError(f"{name} {b} failed rc={r.returncode}")


def fan_out(name: str, items: list[int], workers: int) -> None:
    todo = list(items)
    with cf.ThreadPoolExecutor(workers) as ex:
        live = {ex.submit(stage, name, todo.pop(0)) for _ in range(min(workers, len(todo)))}
        while live:
            done, live = cf.wait(live, return_when=cf.FIRST_COMPLETED)
            for f in done:
                f.result()
            while todo and len(live) < workers:
                live.add(ex.submit(stage, name, todo.pop(0)))


def phase_buckets(workers: int) -> None:
    fan_out("bucket-one", [b for b in range(512) if not (OUT / "cloud_done" / f"bkt{b:03d}.json").exists()], workers)
    if all((OUT / "cloud_done" / f"bkt{b:03d}.json").exists() for b in range(512)):
        (OUT / "_CLOUD_DONE").write_text(datetime.datetime.now().isoformat(timespec="seconds"), encoding="utf8")
    im.log("cloud buckets finished")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("phase", choices=["all", "files", "bucket", "file-one", "bucket-one"])
    ap.add_argument("--bucket", type=int)
    ap.add_argument("--workers", type=int, default=3)
    a = ap.parse_args()
    if a.phase == "bucket-one":
        bucket_one(a.bucket)
        return
    if a.phase == "file-one":
        file_one(a.bucket)                              # --bucket is the file index here
        return
    if a.phase in ("all", "files"):
        fan_out("file-one", [i for i in range(20) if not (WORK / "rows" / f"batch={i}" / "_DONE").exists()],
                a.workers)
        im.log("cloud files finished")
    if a.phase in ("all", "bucket"):
        phase_buckets(a.workers)


if __name__ == "__main__":
    main()
