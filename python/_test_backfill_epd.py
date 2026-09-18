"""backfill_epd fills parent_epd with the EPD the extract would have written.

The extract stores parent_epd only through ply 16, so the backfill re-derives the
rest by replaying forward from the EPDs it did keep. Everything about that is
quiet when wrong: a board rebuilt from an EPD loses state the hash remembers, a
key's ply comes through any_value() and can point at the wrong level, and a
mis-derived EPD is still a well-formed string. So the months below are built from
real move lists with the true EPD of every position recorded first, the EPD is
then nulled past a cutoff to mimic EPD_MAX_PLY, and the backfilled output is
compared against that truth row by row.

  * correctness — every output parent_epd equals the EPD computed directly along
    the game, at every ply, and no row comes out NULL;
  * transposition — one position reached at two different plies, and a tempo-loss
    line that returns to an earlier position, resolve to one EPD;
  * castling — rights lost and the king returned to its square is NOT the earlier
    position, and the EPDs differ where the rights do;
  * en passant, legal — the capture replays and its child hash verifies;
  * en passant, ILLEGAL (pinned) — the parent's EPD has no ep square while its
    hash carries the adjacency key, which is why a parent's hash is never checked
    against its EPD; its children still verify;
  * promotion and under-promotion, and SAN carrying + and #;
  * fixpoint — an edge filed under a ply its parent is not known at is still
    resolved by the sweep;
  * a planted collision (one child_hash overwritten) is reported AND fails the
    month;
  * an unreachable position fails the month rather than being written NULL;
  * a conflict (one hash, two EPDs) is reported and is NOT fatal;
  * resume — a stale _tmp_month=* is discarded, and a finished month is skipped;
  * layout — bucket dirs and their contents are identical to _partition_by_bucket.

Run: .venv/Scripts/python.exe python/_test_backfill_epd.py
"""
from __future__ import annotations

import contextlib
import io
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import chess
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import backfill_epd as bf
from build_pooled_stats import N_MERGE_BUCKETS, _partition_by_bucket
from zobrist import zobrist_int64

CUTOFF = 4              # stands in for EPD_MAX_PLY = 16
BUCKETS = 8
THREADS, WORKERS, MEM = 2, 2, "1GB"
YEAR, MONTH = 2025, 3
PS_SCHEMA = {"parent_hash": pl.Int64, "move_san": pl.Utf8, "event": pl.Utf8,
             "elo_band": pl.Int64, "parent_epd": pl.Utf8, "child_hash": pl.Int64,
             "child_eval": pl.Int32, "ply": pl.Int32, "white_wins": pl.Int64,
             "draws": pl.Int64, "black_wins": pl.Int64, "total": pl.Int64}
KEY = ["parent_hash", "move_san", "event", "elo_band"]

_checks: list[tuple[bool, str]] = []

# More tasks than any plausible per-worker recycle limit, run in a subprocess so
# a regression FAILS on a timeout instead of hanging the suite. See _run_pool:
# max_tasks_per_child deadlocks on 3.11 + Windows spawn, and it did so in the
# 2024-06 pilot at exactly 11 workers x 32 tasks.
POOL_STRESS_N = 200
POOL_STRESS_WORKERS = 3


def _double(t: tuple) -> int:
    return t[0] * 2


def check(ok: bool, label: str) -> bool:
    _checks.append((bool(ok), label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    return bool(ok)


# ── building a month out of real games ────────────────────────────────────────

GAMES = [
    # scholar's mate: SAN with + and #, and a game that ends early
    (None, "e4 e5 Bc4 Nc6 Qh5 Nf6 Qxf7#"),
    # the same position at ply 8 by two move orders
    (None, "Nf3 d5 d4 Nf6 c4 e6 Nc3 Be7"),
    (None, "d4 Nf6 Nf3 d5 c4 e6 Nc3 Be7"),
    # tempo loss: the START position recurs at depth 4
    (None, "Nf3 Nf6 Ng1 Ng8 e4 e5 Nf3 Nc6"),
    # castling rights lost, then both kings walk back
    (None, "e4 e5 Ke2 Ke7 Ke1 Ke8 Nf3 Nf6"),
    # a legal en-passant capture
    (None, "e4 Nf6 e5 d5 exd6 cxd6 Nf3 g6"),
    # promotion and under-promotion from a crafted position
    ("8/P6k/8/8/8/8/6PK/8 w - - 0 1", "a8=Q+ Kg7 Qa1+ Kh7"),
    ("8/P6k/8/8/8/8/6PK/8 w - - 0 1", "a8=N Kg7 g4 Kf7"),
    # ILLEGAL (pinned) en passant: after d4 the black e4 pawn is adjacent to d3,
    # so polyglot mixes in the ep file, but exd3 would expose the a4 king to Qh4
    # along the rank, so board.epd() prints no ep square.
    ("8/8/8/8/k3p2Q/8/3P4/3K4 w - - 0 1", "d4 Ka5 Qe7 Kb5"),
]


def build_rows(games) -> tuple[list[dict], dict[int, str]]:
    """Replay each game, recording the TRUE EPD of every position as we go.

    This is the ground truth the backfill has to reproduce: the extract computed
    parent_epd the same way, with board.epd() on the position before the move.
    """
    rows: list[dict] = []
    truth: dict[int, str] = {}
    for i, (fen, movetext) in enumerate(games):
        board = chess.Board(fen) if fen else chess.Board()
        for ply, san in enumerate(movetext.split(), 1):
            ph, epd = zobrist_int64(board), board.epd()
            truth[ph] = epd
            board.push(board.parse_san(san))
            rows.append({"parent_hash": ph, "move_san": san,
                         "event": "Blitz" if i % 2 == 0 else "Rapid",
                         "elo_band": 1600 + 200 * (i % 3),
                         "parent_epd": epd if ply <= CUTOFF else None,
                         "child_hash": zobrist_int64(board), "child_eval": None,
                         "ply": ply, "white_wins": 1, "draws": 0,
                         "black_wins": 0, "total": 1})
        truth[zobrist_int64(board)] = board.epd()
    return rows, truth


def write_month(mdir: Path, rows: list[dict], year: int = YEAR,
                month: int = MONTH) -> Path:
    """Aggregate exactly as consolidate_monthly's ps spec does, so the tool sees
    a real monthly — including any_value() on parent_epd and ply."""
    mdir.mkdir(parents=True, exist_ok=True)
    df = (pl.DataFrame(rows, schema=PS_SCHEMA)
          .group_by(KEY)
          .agg(pl.col("parent_epd").first(), pl.col("child_hash").first(),
               pl.col("child_eval").first(), pl.col("ply").first(),
               pl.col("white_wins").sum(), pl.col("draws").sum(),
               pl.col("black_wins").sum(), pl.col("total").sum())
          .select(list(PS_SCHEMA)))
    p = mdir / f"year={year}_month={month}.ps.parquet"
    df.write_parquet(p, compression="zstd")
    return p


def run(monthly: Path, out: Path, work: Path, buckets: int = BUCKETS,
        year: int = YEAR, month: int = MONTH):
    """backfill_month with its chatter captured; returns (manifest, log, error)."""
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            man = bf.backfill_month(monthly, year, month, out, work, buckets,
                                    WORKERS, THREADS, MEM, work / "_duck",
                                    fresh=False)
        return man, buf.getvalue(), None
    except Exception as exc:                                       # noqa: BLE001
        return None, buf.getvalue(), exc


def read_out(out: Path, year: int = YEAR, month: int = MONTH) -> pl.DataFrame:
    files = sorted((out / f"month={year}_{month}").glob("bkt=*/*.parquet"))
    return pl.concat([pl.read_parquet(f) for f in files]) if files \
        else pl.DataFrame(schema=PS_SCHEMA)


def main() -> None:
    if "--pool-stress" in sys.argv:
        got = bf._run_pool(_double, [(i,) for i in range(POOL_STRESS_N)],
                           POOL_STRESS_WORKERS)
        print(sum(got))
        return

    print("\nthe worker pool survives more tasks than a recycle limit would allow")
    proc = subprocess.run([sys.executable, __file__, "--pool-stress"],
                          capture_output=True, text=True, timeout=300)
    want = sum(i * 2 for i in range(POOL_STRESS_N))
    check(proc.returncode == 0 and proc.stdout.strip() == str(want),
          f"{POOL_STRESS_N} tasks over {POOL_STRESS_WORKERS} workers all return "
          f"(max_tasks_per_child would deadlock here)")

    tmp = Path(tempfile.mkdtemp(prefix="test_backfill_"))
    try:
        rows, truth = build_rows(GAMES)

        # ── the main month ────────────────────────────────────────────────────
        print("\nbackfill of a month built from real games")
        mdir, out, work = tmp / "m1", tmp / "o1", tmp / "w1"
        monthly = write_month(mdir, rows)
        n_in = pl.read_parquet(monthly).height
        n_null_in = pl.read_parquet(monthly)["parent_epd"].null_count()
        # a stale in-flight month dir from a killed run must not survive
        stale = out / f"_tmp_month={YEAR}_{MONTH}"
        (stale / "bkt=0").mkdir(parents=True)
        (stale / "bkt=0" / "junk.parquet").write_bytes(b"not a parquet")
        man, log, err = run(monthly, out, work)
        check(err is None, f"the month completes ({err})")
        got = read_out(out)
        check(got.height == n_in,
              f"row count conserved: {got.height:,} out == {n_in:,} in")
        check(got["parent_epd"].null_count() == 0,
              f"no NULL parent_epd remains (was {n_null_in:,} of {n_in:,})")
        wrong = [(h, e) for h, e in zip(got["parent_hash"], got["parent_epd"])
                 if truth[h] != e]
        check(not wrong,
              f"every parent_epd equals the EPD computed along the game "
              f"({len(wrong)} wrong)")
        src = pl.read_parquet(monthly)
        check(all(int(src[c].sum()) == int(got[c].sum())
                  for c in ("total", "white_wins", "draws", "black_wins")),
              "all four summed columns conserved exactly")
        check(src.sort(KEY).drop("parent_epd").equals(got.sort(KEY).drop("parent_epd")),
              "every column except parent_epd is byte-identical to the input")
        check(not stale.exists(), "a stale _tmp_month=* from a killed run is discarded")
        check((out / f"_month={YEAR}_{MONTH}.DONE").exists() and not work.exists()
              or not (work / f"month={YEAR}_{MONTH}").exists(),
              "the sentinel is written and the month's work dir is cleaned up")
        check(man and man["unresolved"] == 0 and man["mismatches"] == 0
              and man["rows"] == n_in and man["files"] > 0,
              "the manifest records rows, files and a clean gate")
        check((out / "_manifest" / f"month={YEAR}_{MONTH}.parquet").exists(),
              "a manifest row is written for the month")

        # ── the specific chess cases, read out of that same month ─────────────
        print("\nthe cases that make EPD re-derivation subtle")
        epd_of = dict(zip(got["parent_hash"], got["parent_epd"]))
        deep = got.filter(pl.col("ply") > CUTOFF)
        check(deep.height > 0 and all(truth[h] == e for h, e
                                      in zip(deep["parent_hash"], deep["parent_epd"])),
              f"{deep.height} rows past the EPD cutoff are all correct")

        # Six plies in, not eight: the eighth is a leaf, and only a position that
        # is somebody's parent carries a parent_epd to check.
        b_a = chess.Board()
        for san in "Nf3 d5 d4 Nf6 c4 e6".split():
            b_a.push(b_a.parse_san(san))
        b_b = chess.Board()
        for san in "d4 Nf6 Nf3 d5 c4 e6".split():
            b_b.push(b_b.parse_san(san))
        h_t = zobrist_int64(b_a)
        check(h_t == zobrist_int64(b_b) and epd_of.get(h_t) == b_a.epd(),
              "a position reached by two move orders has one hash and one EPD")

        start = chess.Board()
        check(epd_of.get(zobrist_int64(start)) == start.epd(),
              "the tempo-loss line's return to the start position resolves to it")

        b_c = chess.Board()
        for san in "e4 e5 Ke2 Ke7 Ke1 Ke8".split():
            b_c.push(b_c.parse_san(san))
        b_d = chess.Board()
        for san in "e4 e5".split():
            b_d.push(b_d.parse_san(san))
        check(epd_of.get(zobrist_int64(b_c)) == b_c.epd()
              and b_c.epd() != b_d.epd() and "-" in b_c.epd().split()[2],
              "kings returning home is NOT the earlier position: castling rights differ")

        b_e = chess.Board()
        for san in "e4 Nf6 e5 d5".split():
            b_e.push(b_e.parse_san(san))
        h_ep = zobrist_int64(b_e)
        check(epd_of.get(h_ep) == b_e.epd() and b_e.epd().split()[3] == "d6",
              "a legal en-passant target survives into the backfilled EPD")

        b_p = chess.Board("8/8/8/8/k3p2Q/8/3P4/3K4 w - - 0 1")
        b_p.push(b_p.parse_san("d4"))
        pinned_epd, pinned_hash = b_p.epd(), zobrist_int64(b_p)
        rebuilt = chess.Board(pinned_epd + " 0 1")
        check(pinned_epd.split()[3] == "-"
              and zobrist_int64(rebuilt) != pinned_hash,
              "pinned ep: the EPD drops the square the hash keeps "
              "(so a parent's hash is never checked against its EPD)")
        check(epd_of.get(pinned_hash) == pinned_epd,
              "the pinned-ep position still gets its own correct EPD")
        kids = got.filter(pl.col("parent_hash") == pinned_hash)
        check(kids.height > 0 and all(
            epd_of.get(c, truth[c]) == truth[c] for c in kids["child_hash"]),
              "its children verify anyway: only an ep capture could differ, "
              "and that capture is illegal")

        b_q = chess.Board("8/P6k/8/8/8/8/6PK/8 w - - 0 1")
        b_q.push(b_q.parse_san("a8=Q+"))
        b_n = chess.Board("8/P6k/8/8/8/8/6PK/8 w - - 0 1")
        b_n.push(b_n.parse_san("a8=N"))
        check(epd_of.get(zobrist_int64(b_q)) == b_q.epd()
              and truth[zobrist_int64(b_n)] == b_n.epd(),
              "promotion and under-promotion both replay correctly (SAN with +)")

        # ── the fixpoint ──────────────────────────────────────────────────────
        print("\nthe fixpoint, for keys whose any_value(ply) is not their depth")
        mdir2, out2, work2 = tmp / "m2", tmp / "o2", tmp / "w2"
        df = pl.read_parquet(monthly)
        # Name the edge rather than picking one: it has to be an edge whose child
        # is itself a parent AND is reachable no other way. A leaf child never
        # needs an EPD, and the tempo-loss line's depth-6 position is the depth-2
        # position of another game, so either choice would resolve without a sweep
        # and the test would pass for the wrong reason.
        b_mv = chess.Board()
        for san in "Nf3 d5 d4 Nf6 c4".split():
            b_mv.push(b_mv.parse_san(san))
        mis_parent, mis_move = zobrist_int64(b_mv), "e6"
        picked = (pl.col("parent_hash") == mis_parent) & (pl.col("move_san") == mis_move)
        check(df.filter(picked).height > 0 and df.filter(picked)["ply"][0] == 6,
              "the fixpoint fixture is the ply-6 edge into the transposition")
        moved = df.with_columns(
            pl.when(picked).then(pl.lit(2, dtype=pl.Int32))
            .otherwise(pl.col("ply")).alias("ply"))
        mdir2.mkdir(parents=True)
        p2 = mdir2 / f"year={YEAR}_month={MONTH}.ps.parquet"
        moved.write_parquet(p2, compression="zstd")
        man2, log2, err2 = run(p2, out2, work2)
        got2 = read_out(out2)
        check(err2 is None and got2["parent_epd"].null_count() == 0,
              "an edge filed under ply 2 whose parent is deeper still resolves")
        check("sweep 1:" in log2,
              "the sweep is what resolves it, and says so in the log")
        check(all(truth[h] == e for h, e in zip(got2["parent_hash"],
                                                got2["parent_epd"])),
              "and every EPD in that month is still correct")

        # A CHAIN of mis-filed plies needs one round per link: sweep 1 can only
        # resolve the depth-6 position, and the depth-7 edge below it becomes
        # replayable only once that lands. Rounds after the first read just the
        # previous round's discoveries, so this is the path that a single-sweep
        # fixture would leave completely untested -- and 2024-06 needs ~8 rounds.
        mdir2b, out2b, work2b = tmp / "m2b", tmp / "o2b", tmp / "w2b"
        b_ch = chess.Board()
        for san in "Nf3 d5 d4 Nf6 c4 e6".split():
            b_ch.push(b_ch.parse_san(san))
        picked2 = ((pl.col("parent_hash") == zobrist_int64(b_ch))
                   & (pl.col("move_san") == "Nc3"))
        check(df.filter(picked2).height > 0 and df.filter(picked2)["ply"][0] == 7,
              "the chain fixture is the ply-7 edge below that one")
        chained = df.with_columns(
            pl.when(picked | picked2).then(pl.lit(2, dtype=pl.Int32))
            .otherwise(pl.col("ply")).alias("ply"))
        mdir2b.mkdir(parents=True)
        p2b = mdir2b / f"year={YEAR}_month={MONTH}.ps.parquet"
        chained.write_parquet(p2b, compression="zstd")
        man2b, log2b, err2b = run(p2b, out2b, work2b)
        got2b = read_out(out2b)
        check(err2b is None and got2b["parent_epd"].null_count() == 0
              and all(truth[h] == e for h, e in zip(got2b["parent_hash"],
                                                    got2b["parent_epd"])),
              "a two-link chain of mis-filed plies resolves, every EPD correct")
        check("sweep 2:" in log2b,
              "it genuinely takes two rounds (one link unwound per round)")
        check("pending:" in log2b,
              "and the edges reaching the missing set are collected once, not per round")

        # ── the gates ─────────────────────────────────────────────────────────
        print("\nthe gates")
        mdir3, out3, work3 = tmp / "m3", tmp / "o3", tmp / "w3"
        bad = df.with_columns(
            pl.when(picked).then(pl.lit(123456789, dtype=pl.Int64))
            .otherwise(pl.col("child_hash")).alias("child_hash"))
        mdir3.mkdir(parents=True)
        p3 = mdir3 / f"year={YEAR}_month={MONTH}.ps.parquet"
        bad.write_parquet(p3, compression="zstd")
        man3, _, err3 = run(p3, out3, work3)
        check(err3 is not None and "mismatch" in str(err3),
              f"a planted wrong child_hash fails the month ({type(err3).__name__})")
        rep = sorted((work3 / f"month={YEAR}_{MONTH}" / "mismatch").glob("*.parquet"))
        found = pl.concat([pl.read_parquet(f) for f in rep]) if rep else None
        check(found is not None and found.height >= 1
              and found["reason"][0] == "hash",
              "and the mismatch is written out with the position that failed")
        check(not (out3 / f"month={YEAR}_{MONTH}").exists()
              and not (out3 / f"_month={YEAR}_{MONTH}.DONE").exists(),
              "no month directory and no sentinel are left behind by a failure")

        mdir4, out4, work4 = tmp / "m4", tmp / "o4", tmp / "w4"
        orphan = pl.DataFrame([{"parent_hash": 987654321, "move_san": "e4",
                                "event": "Blitz", "elo_band": 1600,
                                "parent_epd": None, "child_hash": 987654322,
                                "child_eval": None, "ply": 20, "white_wins": 1,
                                "draws": 0, "black_wins": 0, "total": 1}],
                              schema=PS_SCHEMA)
        mdir4.mkdir(parents=True)
        p4 = mdir4 / f"year={YEAR}_month={MONTH}.ps.parquet"
        pl.concat([df, orphan]).write_parquet(p4, compression="zstd")
        _, _, err4 = run(p4, out4, work4)
        check(err4 is not None and "no EPD" in str(err4),
              "a position no edge reaches fails the month instead of writing NULL")

        # ── conflicts are reported, not fatal ─────────────────────────────────
        print("\nconflicts: one hash, two EPDs")
        mdir5, out5, work5 = tmp / "m5", tmp / "o5", tmp / "w5"
        shallow = df.filter(pl.col("parent_epd").is_not_null()).sort(KEY)
        target = shallow["parent_hash"][0]
        twin = df.with_columns(
            pl.when((pl.col("parent_hash") == target)
                    & (pl.col("move_san") == shallow["move_san"][0]))
            .then(pl.lit(chess.Board().epd()))
            .otherwise(pl.col("parent_epd")).alias("parent_epd"))
        mdir5.mkdir(parents=True)
        p5 = mdir5 / f"year={YEAR}_month={MONTH}.ps.parquet"
        twin.write_parquet(p5, compression="zstd")
        man5, _, err5 = run(p5, out5, work5)
        conf = sorted((out5 / "_conflicts" / f"month={YEAR}_{MONTH}").glob("*.parquet"))
        cdf = pl.concat([pl.read_parquet(f) for f in conf]) if conf else None
        check(err5 is None, "two EPDs for one hash do NOT fail the month")
        check(cdf is not None and target in list(cdf["hash"]),
              "the conflicting hash is reported for the collision audit")

        # ── resume, and the merge's layout ────────────────────────────────────
        print("\nresume and layout")
        man6, log6, err6 = run(monthly, out, work)
        check(err6 is None and man6 == {} and "already done" in log6,
              "a month with its sentinel is skipped on a re-run")

        mdir7, out7, work7 = tmp / "m7", tmp / "o7", tmp / "w7"
        shutil.copytree(mdir, mdir7)
        _, _, err7 = run(mdir7 / f"year={YEAR}_month={MONTH}.ps.parquet", out7,
                         work7, buckets=N_MERGE_BUCKETS)
        ref = tmp / "ref"
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            _partition_by_bucket(mdir7, "ps", ref, THREADS, MEM, work7 / "_duck")
        ours = {p.name for p in (out7 / f"month={YEAR}_{MONTH}").glob("bkt=*")}
        theirs = {p.name for p in (ref / f"month={YEAR}_{MONTH}").glob("bkt=*")}
        check(err7 is None and ours == theirs and ours,
              f"the same {len(ours)} bucket dirs as _partition_by_bucket, "
              f"named identically")
        mine = read_out(out7).sort(KEY)
        refd = pl.concat([pl.read_parquet(f) for f in
                          (ref / f"month={YEAR}_{MONTH}").glob("bkt=*/*.parquet")]).sort(KEY)
        check(list(mine.columns) == list(refd.columns) == list(PS_SCHEMA),
              "and the same columns, in the monthly's own order")
        check(mine.drop("parent_epd").equals(refd.drop("parent_epd")),
              "and the same rows in the same buckets, differing only in parent_epd")
        per_bucket_ok = True
        for d in (out7 / f"month={YEAR}_{MONTH}").glob("bkt=*"):
            i = int(d.name.split("=")[1])
            hs = pl.concat([pl.read_parquet(f) for f in d.glob("*.parquet")])["parent_hash"]
            per_bucket_ok &= all(((h % N_MERGE_BUCKETS) + N_MERGE_BUCKETS)
                                 % N_MERGE_BUCKETS == i for h in hs)
        check(per_bucket_ok,
              "every row sits in the bucket _bucket_expr(parent_hash) names")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} "
          f"({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
