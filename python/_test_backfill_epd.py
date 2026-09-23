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
  * a planted wrong child_hash is reported AND, under --max-quarantine-edges 0,
    fails the month exactly as before B2;
  * an unreachable position fails the month rather than being written NULL;
  * quarantine (B2) — a forged genuine-collision shape (a deep position's
    outgoing rows re-keyed onto a resolved position's hash) is quarantined, not
    fatal: the book is exactly the truth minus that edge and its subtree,
    book + quarantine == input, the reasons are edge/unreachable, a wrong side to
    move is labelled hash+parity, the threshold and orphans stay fatal, a work
    dir halted under the old policy resumes without redoing a level, and the
    quarantine sits outside the merge's month=*/bkt=* glob;
  * a conflict (one hash, two EPDs) is reported and is NOT fatal;
  * resume — a stale _tmp_month=* is discarded, and a finished month is skipped;
  * layout — bucket dirs and their contents are identical to _partition_by_bucket.

Run: .venv/Scripts/python.exe python/_test_backfill_epd.py
"""
from __future__ import annotations

import contextlib
import io
import json
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

# The collision fixtures (B2), kept out of GAMES so the main month is untouched.
# G_P walks two kings past the EPD cutoff; its deep positions are the P' whose
# outgoing rows get re-keyed. G_E's start position is the resolved E: White king
# on c6, so both forged moves (Kb5 at ply 5, Kd6 at ply 6) are legal from it and
# replay to a child hash that is NOT the stored one. G_E itself plays neither
# move, so the forged (hash(E), san) key belongs to the forgery alone.
G_P = ("4k3/8/8/8/8/3K4/8/8 w - - 0 1", "Kc4 Kd7 Kb4 Kc7 Kb5 Kd6 Kc4 Ke5 Kc3 Ke4")
G_E = ("8/8/2K5/8/8/8/8/k7 w - - 0 1", "Kc7 Kb2 Kc8 Kc2")


def line_hashes(fen: str, movetext: str) -> list[int]:
    """Hash of the position after 0, 1, 2 ... plies of one game."""
    board = chess.Board(fen)
    out = [zobrist_int64(board)]
    for san in movetext.split():
        board.push(board.parse_san(san))
        out.append(zobrist_int64(board))
    return out


def forge(df: pl.DataFrame, p_hash: int, e_hash: int) -> pl.DataFrame:
    """Re-key ONLY p_hash's outgoing rows onto e_hash: the signature a genuine
    64-bit collision leaves. P' keeps its incoming edge and becomes a leaf."""
    return df.with_columns(
        pl.when(pl.col("parent_hash") == p_hash).then(pl.lit(e_hash, pl.Int64))
        .otherwise(pl.col("parent_hash")).alias("parent_hash"))


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
        year: int = YEAR, month: int = MONTH, **kw):
    """backfill_month with its chatter captured; returns (manifest, log, error).
    `kw` carries the quarantine knobs (max_q_edges, max_q_rows)."""
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            man = bf.backfill_month(monthly, year, month, out, work, buckets,
                                    WORKERS, THREADS, MEM, work / "_duck",
                                    fresh=False, **kw)
        return man, buf.getvalue(), None
    except Exception as exc:                                       # noqa: BLE001
        return None, buf.getvalue(), exc


def read_out(out: Path, year: int = YEAR, month: int = MONTH) -> pl.DataFrame:
    files = sorted((out / f"month={year}_{month}").glob("bkt=*/*.parquet"))
    return pl.concat([pl.read_parquet(f) for f in files]) if files \
        else pl.DataFrame(schema=PS_SCHEMA)


def read_q(out: Path, year: int = YEAR, month: int = MONTH) -> pl.DataFrame:
    files = sorted((out / "_quarantine" / f"month={year}_{month}").rglob("*.parquet"))
    return pl.concat([pl.read_parquet(f) for f in files]) if files \
        else pl.DataFrame(schema={**PS_SCHEMA, "reason": pl.Utf8})


def read_reports(d: Path) -> pl.DataFrame | None:
    files = sorted(d.glob("*.parquet"))
    return pl.concat([pl.read_parquet(f) for f in files]) if files else None


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
        # Ply 3, not 2: a transposed occurrence always has the same side to move,
        # so any_value(ply) keeps its parity, and a White move filed under an even
        # ply would be quarantined by the write stage's parity rule.
        chained = df.with_columns(
            pl.when(picked).then(pl.lit(2, dtype=pl.Int32))
            .when(picked2).then(pl.lit(3, dtype=pl.Int32))
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
        man3, _, err3 = run(p3, out3, work3, max_q_edges=0)
        check(err3 is not None and "mismatch" in str(err3),
              f"a planted wrong child_hash fails the month under "
              f"--max-quarantine-edges 0 ({type(err3).__name__})")
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
        check(err4 is not None and "no replayable edge" in str(err4),
              "a position no edge reaches fails the month instead of writing NULL")

        # ── B2: quarantine a genuine-collision shape instead of halting ───────
        print("\nquarantine: a forged 64-bit collision")
        rows_c, truth_c = build_rows(GAMES + [G_P, G_E])
        base = pl.read_parquet(write_month(tmp / "mc", rows_c))
        hp = line_hashes(*G_P)             # hp[i] = G_P after i plies
        h_e = line_hashes(*G_E)[0]         # E: resolved from its ply-1 seed
        ply_of = {h: i for i, h in enumerate(hp)}

        def month_of(name: str, df: pl.DataFrame) -> Path:
            d = tmp / name
            d.mkdir(parents=True)
            p = d / f"year={YEAR}_month={MONTH}.ps.parquet"
            df.write_parquet(p, compression="zstd")
            return p

        # 1 ── P' = G_P after 4 plies (White to move), its ply-5 Kb5 re-keyed
        #      onto E (also White to move): a plain hash mismatch.
        forged1 = forge(base, hp[4], h_e)
        pq1 = month_of("mq1", forged1)
        outq1, workq1 = tmp / "oq1", tmp / "wq1"
        manq1, logq1, errq1 = run(pq1, outq1, workq1)
        check(errq1 is None, f"a forged collision is quarantined, not fatal ({errq1})")
        rep1 = read_reports(outq1 / "_mismatch" / f"month={YEAR}_{MONTH}")
        check(rep1 is not None and rep1.height == 1
              and (rep1["parent_hash"][0], rep1["move_san"][0],
                   rep1["child_hash"][0], rep1["reason"][0])
              == (h_e, "Kb5", hp[5], "hash"),
              "the report lists exactly P''s re-keyed edge, reason 'hash'")
        # The truth: every row except P''s edge and its whole subtree -- rows
        # whose parent is G_P after 4..9 plies -- with the directly computed EPD.
        drop = pl.col("parent_hash").is_in(hp[4:10])
        want_book = (base.filter(~drop)
                     .with_columns(pl.col("parent_hash")
                                   .map_elements(truth_c.get, return_dtype=pl.Utf8)
                                   .alias("parent_epd"))
                     .sort(KEY))
        book1, q1 = read_out(outq1).sort(KEY), read_q(outq1).sort(KEY)
        check(book1.equals(want_book),
              f"book == truth minus the forged edge and its descendants, as a "
              f"row multiset ({book1.height} rows, {base.height - book1.height} out)")
        sums = ("total", "white_wins", "draws", "black_wins")
        check(book1.height + q1.height == forged1.height
              and all(int(book1[c].sum()) + int(q1[c].sum()) == int(forged1[c].sum())
                      for c in sums),
              "book + quarantine == input, on rows and all four sums")
        reasons1 = dict(q1.group_by("reason").len().iter_rows())
        check(reasons1 == {"edge": 1, "unreachable": 5},
              f"the quarantine is 1 edge row + 5 unreachable rows ({reasons1})")
        want_q = forged1.filter(pl.col("parent_hash").is_in([h_e, *hp[5:10]])
                                & ~((pl.col("parent_hash") == h_e)
                                    & (pl.col("move_san") != "Kb5"))).sort(KEY)
        check(q1.drop("parent_epd", "reason").equals(want_q.drop("parent_epd"))
              and q1.filter(pl.col("reason") == "unreachable")["parent_epd"]
              .null_count() == 5
              and q1.filter(pl.col("reason") == "edge")["parent_epd"].to_list()
              == [truth_c[h_e]],
              "quarantined rows are the input rows; unreachable keep a NULL EPD, "
              "the edge keeps the EPD it was resolved to")
        check(manq1 and manq1["quarantine_edges"] == 1
              and manq1["quarantine_rows"] == q1.height == 6
              and manq1["unreachable_positions"] == 5
              and manq1["mismatches"] == 1
              and json.loads(manq1["quarantine_by_reason"]) == reasons1
              and manq1["rows"] == book1.height
              and all(manq1[c] == int(book1[c].sum())
                      and manq1[f"quarantine_{c}"] == int(q1[c].sum()) for c in sums),
              "the manifest counts the book and the quarantine separately, exactly")
        check("quarantined 1 edges, 6 rows" in logq1 and "_quarantine" in logq1,
              "one log line names the quarantine and where it went")

        # 6 ── the merge reads month=*/bkt=*/*.parquet; the quarantine is not in it
        merge_view = set(outq1.glob("month=*/bkt=*/*.parquet"))
        q_files = set((outq1 / "_quarantine").rglob("*.parquet"))
        check(q_files and not (merge_view & q_files)
              and not any("_q" in p.parts or "reason" in pl.read_parquet_schema(p)
                          for p in merge_view),
              "nothing under _quarantine matches the merge's month=*/bkt=* glob")

        # 2 ── P' = G_P after 5 plies (Black to move), its ply-6 Kd6 re-keyed
        #      onto E (White to move): the report labels the wrong side to move.
        pq2 = month_of("mq2", forge(base, hp[5], h_e))
        outq2, workq2 = tmp / "oq2", tmp / "wq2"
        manq2, _, errq2 = run(pq2, outq2, workq2)
        rep2 = read_reports(outq2 / "_mismatch" / f"month={YEAR}_{MONTH}")
        check(errq2 is None and rep2 is not None
              and rep2["reason"].to_list() == ["hash+parity"]
              and manq2["unreachable_positions"] == 4,
              f"E with the opposite side to move: reason 'hash+parity' ({errq2})")

        # 3 ── the threshold: 0 is the old policy, fatal at the forged level
        outq3, workq3 = tmp / "oq3", tmp / "wq3"
        _, _, errq3 = run(pq1, outq3, workq3, max_q_edges=0)
        check(errq3 is not None and "ply 5: 1 hash mismatches" in str(errq3),
              f"--max-quarantine-edges 0 raises the old error at ply 5 ({errq3})")
        check(not (outq3 / f"month={YEAR}_{MONTH}").exists()
              and not (outq3 / f"_month={YEAR}_{MONTH}.DONE").exists()
              and not (outq3 / "_quarantine").exists()
              and not (outq3 / "_manifest").exists(),
              "and nothing is promoted: no month, quarantine, manifest or sentinel")

        # 5 ── resume across the policy change: that halted work dir, re-run
        #      with the defaults, must pick up at ply 5 and redo nothing earlier.
        wm = workq3 / f"month={YEAR}_{MONTH}"
        params_before = (wm / "_params.json").read_text()
        early = ["edges", "seeds"] + [f"lvl={k}" for k in range(1, 5)]
        stamps = {n: bf._marker(wm, n).stat().st_mtime_ns for n in early}
        marked: list[str] = []
        seen: dict = {}
        real_mark = bf._mark

        def spy_mark(work_dir: Path, name: str) -> None:
            if not marked:          # before anything new: the dir as resumed
                seen["params"] = (work_dir / "_params.json").read_text()
                seen["stamps"] = {n: bf._marker(work_dir, n).stat().st_mtime_ns
                                  for n in early}
            marked.append(name)
            real_mark(work_dir, name)

        bf._mark = spy_mark
        try:
            manq5, logq5, errq5 = run(pq1, outq3, workq3)
        finally:
            bf._mark = real_mark
        check(errq5 is None and manq5 and manq5["quarantine_edges"] == 1,
              f"a month halted under the old policy completes on re-run ({errq5})")
        check(seen.get("params") == params_before
              and seen.get("stamps") == stamps
              and marked and marked[0] == "lvl=5"
              and not set(early) & set(marked),
              "it resumed at ply 5: _params.json unchanged, the earlier stage "
              "markers kept their timestamps, and no earlier stage re-ran")
        check(read_out(outq3).sort(KEY).equals(book1)
              and read_q(outq3).sort(KEY).equals(q1),
              "and its book and quarantine are identical to a fresh run's")

        # 4 ── an orphan is still fatal with the default thresholds, even in a
        #      month whose collision would otherwise quarantine cleanly
        pq4 = month_of("mq4", pl.concat([forged1, orphan]))
        _, _, errq4 = run(pq4, tmp / "oq4", tmp / "wq4")
        check(errq4 is not None and "no replayable edge" in str(errq4),
              "an orphan beside a quarantined collision still fails the month")

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
