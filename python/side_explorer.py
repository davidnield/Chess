"""An explorer month for a SIDE-specific rating filter, in the layout
explorer_policy_value.py reads (month=Y_M/bkt=0/part-0000.parquet + _term/).

explorer-extract filters on the game's mean Elo, but the weak-us question needs
"our side 1000-1799, the opponent >= 1800" -- replay_holdout's
--our-elo 1000:1800 --opp-min-elo 1800 population -- so it needs one month dir per
colour of book. Games are replayed in Python (python-chess, the replay's own
incremental hasher) to --max-ply plies: every move is an edge (parent, SAN, child,
result); a game that ends within the cap is ENDED at its last position, one that
goes on is HORIZON at the cap, and a game that fails to parse keeps the edges it
walked and no term row -- the explorer's own conventions. Bots and Abandoned /
Rules infraction games are dropped, as the explorer month does.

Bootstrap contract (follow-up review R2, 2026-10-31). --folds F splits the games into F
folds, fold = blake2b(game_id, person=b"fold") % F, and keeps `fold` in the ps and term
keys; --sample keeps a game iff blake2b(game_id, person=b"sample") < frac. Both are pure
functions of the game id, so a game is in or out of the sample, and in the same fold, in
EVERY table built this way -- our-side and opponent tables share each game's bootstrap
weight. `_explorer_params.json` records the settings (folds, the hash rules, sample,
months, population filters, code revision); convertibility_eval refuses a bootstrap over
tables that lack it, declare fewer than 2 folds, or disagree on folds or rules, and a dir
is locked to its settings (a resume with different ones is refused).

    PYTHONPATH=python .venv/Scripts/python.exe python/side_explorer.py \
        --our-colour white --out E:/chess/holdout_explorer/weak_white_2026_08_09
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import chess
import polars as pl
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_pooled_stats import discover_source_files  # noqa: E402
from stage1_extract_positions import iter_san_moves  # noqa: E402
from zobrist import IncrementalZobrist, zobrist_int64  # noqa: E402

EXCLUDED_TERMS = {"Abandoned", "Rules infraction"}
START = zobrist_int64(chess.Board())


FOLD_RULE = "blake2b(game_id, digest_size=8, person=b'fold') little-endian % F"
SAMPLE_RULE = "blake2b(game_id, digest_size=8, person=b'sample') little-endian / 2**64 < frac"


def _h64(game_id, person: bytes) -> int:
    return int.from_bytes(hashlib.blake2b(str(game_id).encode(), digest_size=8,
                                          person=person).digest(), "little")


def fold_of(game_id, folds: int) -> int:
    """A game's fold: a keyed hash of its id (the same game -> the same fold in every table).
    Folds are nested when F is a power of two: fold_of(g, F) % G == fold_of(g, G) for G | F,
    so one table serves every coarser fold count (convertibility_eval --use-folds G)."""
    if folds <= 1:
        return 0
    return _h64(game_id, b"fold") % folds


def in_sample(game_id, frac: float) -> bool:
    """The game-keyed sample: the same game is kept or dropped in every table."""
    return frac >= 1.0 or _h64(game_id, b"sample") < frac * 2.0 ** 64


def one_file(args):
    path, our_white, lo, hi, opp_min, max_ply, frac, folds = args
    cols = ["movetext", "white_elo", "black_elo", "white_score", "white_title",
            "black_title", "termination", "game_id"]
    edges: dict[tuple, list] = {}
    term: dict[tuple, list] = {}
    kept = failed = 0
    board = chess.Board()
    hasher = IncrementalZobrist(board)
    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(batch_size=100_000, columns=cols):
        df = pl.from_arrow(batch)
        ours, theirs = ("white_elo", "black_elo") if our_white else ("black_elo", "white_elo")
        df = df.filter(pl.col(ours).is_between(lo, hi, closed="left")
                       & (pl.col(theirs) >= opp_min)
                       & pl.col("white_score").is_not_null()
                       & (pl.col("white_title").fill_null("") != "BOT")
                       & (pl.col("black_title").fill_null("") != "BOT")
                       & ~pl.col("termination").fill_null("").is_in(list(EXCLUDED_TERMS)))
        for mt, ws, gid in df.select("movetext", "white_score", "game_id").iter_rows():
            if not in_sample(gid, frac):
                continue
            kept += 1
            f = fold_of(gid, folds)
            r = (1, 0, 0) if ws == 1.0 else (0, 1, 0) if ws == 0.5 else (0, 0, 1)
            board.reset()
            hasher.reset(board)
            h = START
            ply = 0
            ok, more = True, False
            for san in iter_san_moves(mt or ""):
                if ply >= max_ply:
                    more = True
                    break
                try:
                    mv = board.parse_san(san)
                except ValueError:
                    ok = False
                    break
                hasher.push_move(board, mv)
                c = hasher.current(board)
                k = (h, san, c, f)
                e = edges.get(k)
                if e is None:
                    edges[k] = [r[0], r[1], r[2], 1]
                else:
                    e[0] += r[0]
                    e[1] += r[1]
                    e[2] += r[2]
                    e[3] += 1
                h = c
                ply += 1
            if not ok:
                failed += 1
                continue
            k = (h, 1 if more else 0, f)
            t = term.get(k)
            if t is None:
                term[k] = [r[0], r[1], r[2], 1]
            else:
                t[0] += r[0]
                t[1] += r[1]
                t[2] += r[2]
                t[3] += 1
    ps = pl.DataFrame([(p, s, c, f, w, d, b, n) for (p, s, c, f), (w, d, b, n) in edges.items()],
                      schema={"parent_hash": pl.Int64, "move_san": pl.Utf8,
                              "child_hash": pl.Int64, "fold": pl.Int16, "white_wins": pl.Int64,
                              "draws": pl.Int64, "black_wins": pl.Int64, "total": pl.Int64},
                      orient="row")
    tm = pl.DataFrame([(p, k, 0, f, w, d, b, n) for (p, k, f), (w, d, b, n) in term.items()],
                      schema={"position_hash": pl.Int64, "kind": pl.Int32,
                              "reason": pl.Int32, "fold": pl.Int16, "white_wins": pl.Int64,
                              "draws": pl.Int64, "black_wins": pl.Int64, "total": pl.Int64},
                      orient="row")
    return ps, tm, kept, failed


def file_to_part(job) -> None:
    """One source file -> <part>.ps.parquet + <part>.tm.parquet, then <part>.json (kept,
    failed) LAST: the json is the part's sentinel, so a rerun skips finished files."""
    args, part = job
    ps, tm, kept, failed = one_file(args)
    for df, suf in ((ps, ".ps.parquet"), (tm, ".tm.parquet")):
        tmp = Path(part + suf + ".tmp")
        df.write_parquet(tmp)
        tmp.replace(part + suf)
    Path(part + ".json").write_text(json.dumps([kept, failed]), encoding="utf-8")


def merge_parts(parts: Path, ps_out: Path, tm_out: Path, tmp_dir: str, mem: str):
    """Sum the month's per-file parts into its ps and term files with DuckDB (2026-10-31).
    Holding the frames in the parent and merging them with polars peaked near 95 GB for a
    64-fold movers table; DuckDB spills instead. Returns (ps rows, term rows, term total)."""
    import duckdb
    if str(tmp_dir).upper().startswith("F:"):
        raise SystemExit("FATAL: DuckDB spill on F: (USB HDD) is forbidden")
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{mem}'; SET threads=8; SET preserve_insertion_order=false; "
                f"SET temp_directory='{Path(tmp_dir).as_posix()}'")
    g = parts.as_posix()
    for src, dst, key, types in (
            ("ps", ps_out, "parent_hash, move_san, child_hash, fold",
             "parent_hash::BIGINT parent_hash, move_san::VARCHAR move_san, "
             "child_hash::BIGINT child_hash, fold::SMALLINT fold"),
            ("tm", tm_out, "position_hash, kind, reason, fold",
             "position_hash::BIGINT position_hash, kind::INTEGER kind, "
             "reason::INTEGER reason, fold::SMALLINT fold")):
        tmp = dst.with_suffix(".parquet.tmp")
        con.execute(f"""COPY (SELECT {types}, SUM(white_wins)::BIGINT white_wins,
            SUM(draws)::BIGINT draws, SUM(black_wins)::BIGINT black_wins, SUM(total)::BIGINT total
            FROM read_parquet('{g}/*.{src}.parquet') GROUP BY {key})
            TO '{tmp.as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
        tmp.replace(dst)
    n_ps = con.execute(f"SELECT count(*) FROM read_parquet('{ps_out.as_posix()}')").fetchone()[0]
    n_tm, tot = con.execute(f"SELECT count(*), SUM(total) FROM read_parquet('{tm_out.as_posix()}')").fetchone()
    return n_ps, n_tm, int(tot or 0)


def params_of(a) -> dict:
    """What a table dir is locked to (and what convertibility_eval checks across tables)."""
    return {"folds": a.folds, "fold_rule": FOLD_RULE, "sample": a.sample,
            "sample_rule": SAMPLE_RULE, "months": a.months, "events": a.events,
            "our_colour": a.our_colour, "our_elo": a.our_elo, "opp_min_elo": a.opp_min_elo,
            "max_ply": a.max_ply}


def lock_params(out: Path, a) -> None:
    """Write _explorer_params.json on first use; refuse a resume with other settings."""
    want = params_of(a)
    pf = out / "_explorer_params.json"
    if pf.exists():
        have = json.loads(pf.read_text(encoding="utf-8"))
        diff = {k: (have.get(k), v) for k, v in want.items() if have.get(k) != v}
        if diff:
            raise SystemExit(f"FATAL: {out} was built with other settings: {diff}")
        return
    if any(out.glob("_month=*.DONE")):
        raise SystemExit(f"FATAL: {out} has months but no _explorer_params.json (built before "
                         f"2026-10-31): rebuild it in a fresh dir")
    try:
        rev = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True,
                             cwd=Path(__file__).parent).stdout.strip()
    except OSError:
        rev = None
    out.mkdir(parents=True, exist_ok=True)
    pf.write_text(json.dumps({**want, "code_rev": rev}, indent=1), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", default="2026_8,2026_9")
    ap.add_argument("--events", default="Blitz,Rapid,Classical")
    ap.add_argument("--our-colour", required=True, choices=["white", "black"])
    ap.add_argument("--our-elo", default="1000:1800")
    ap.add_argument("--opp-min-elo", type=int, default=1800)
    ap.add_argument("--max-ply", type=int, default=30)
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--sample", type=float, default=1.0, help="fraction of kept games to replay")
    ap.add_argument("--folds", type=int, default=1,
                    help="split games into F folds by blake2b(game_id) %% F and keep `fold` in the "
                         "key (2026-10-31): convertibility_eval's bootstrap reweights whole folds, so "
                         "a resampled game moves all its edges together. 1 = no split (fold 0)")
    ap.add_argument("--tmp-dir", default="D:/chess_duckdb_tmp", help="DuckDB spill dir (never F:)")
    ap.add_argument("--mem", default="24GB", help="DuckDB memory_limit for the month's merge")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    lo, hi = (int(x) for x in a.our_elo.split(":"))
    out = Path(a.out)
    events = a.events.split(",")
    lock_params(out, a)
    for m in a.months.split(","):
        y, mo = (int(x) for x in m.split("_"))
        done = out / f"_month={m}.DONE"
        if done.exists():
            print(f"{m}: done, skipping")
            continue
        t0 = time.time()
        files = [f for f, yy, mm, _ev in discover_source_files(y, y, None, events) if mm == mo]
        parts = out / "_parts" / f"month={m}"
        parts.mkdir(parents=True, exist_ok=True)
        jobs = [((str(f), a.our_colour == "white", lo, hi, a.opp_min_elo, a.max_ply, a.sample,
                  a.folds), str(parts / f"p{i:04d}")) for i, f in enumerate(files)]
        todo = [j for j in jobs if not Path(j[1] + ".json").exists()]
        if todo:
            with Pool(a.workers) as pool:
                for _ in pool.imap_unordered(file_to_part, todo):
                    pass
        kept = failed = 0
        for _, pp in jobs:
            k_, f_ = json.loads(Path(pp + ".json").read_text(encoding="utf-8"))
            kept += k_
            failed += f_
        (out / f"month={m}" / "bkt=0").mkdir(parents=True, exist_ok=True)
        (out / "_term").mkdir(parents=True, exist_ok=True)
        n_ps, n_tm, term_total = merge_parts(parts, out / f"month={m}" / "bkt=0" / "part-0000.parquet",
                                             out / "_term" / f"year={y}_month={mo}.term.parquet",
                                             a.tmp_dir, a.mem)
        assert term_total == kept - failed, "term total != kept - failed"
        done.write_text("ok\n")
        shutil.rmtree(parts)
        print(f"{m}: {len(files)} files, {kept:,} games ({failed:,} failed), "
              f"{n_ps:,} edges, {n_tm:,} term rows ({time.time() - t0:,.0f}s)",
              flush=True)


if __name__ == "__main__":
    main()
