"""The fused winpos path reproduces build_crush_winpos.winpos_sql exactly.

winpos_sql stays the definition of the event. This asserts the inline version
agrees with it, rather than assuming the reimplementation is faithful — the
guarantees it encodes (no double counting, ever-achieved rather than
frontier-eval, downstream-only, the strict > at the edge's own ply) are subtle
enough that a plausible-looking rewrite can violate one silently and still
produce a well-formed histogram.

Two levels:
  1. The SAME synthetic fixtures _test_winpos.py uses, imported rather than
     copied, so both implementations are judged against one set of hand-designed
     edge cases (G1..G7).
  2. Random games with random eval assignments, which is what catches the cases
     nobody thought to hand-write.

Also pins the MISSING trap: an uncovered position is -32768, which satisfies
`eval <= -thresh` and would fabricate a black crossing everywhere the eval DB
has a hole. In SQL that case simply fails to join.

Run: .venv/Scripts/python.exe python/_test_winpos_fused.py
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import duckdb
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from build_crush_winpos import SENTINEL, winpos_sql
from eval_arrays import MISSING
from winpos_fused import game_events, winpos_batch
import _test_winpos as oracle_fixtures

THRESH = 300
_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def run_oracle(pm: pl.DataFrame, crush: pl.DataFrame, win_w, win_b,
               keys) -> dict:
    """winpos_sql over in-memory fixtures staged to parquet.

    winpos_sql reads its inputs by path, so they have to hit disk. They are
    per-invocation scratch, NOT curated fixtures: staging them under
    _test_fixtures/ put data-dependent parquet into git, and the real-data
    level's newest-year selector silently rewrote them whenever a backfill
    landed a new year. A temp dir keeps the working tree clean.
    """
    with tempfile.TemporaryDirectory(prefix="winpos_fused_oracle_") as td:
        pm_pq, cr_pq = Path(td) / "pm.parquet", Path(td) / "crush.parquet"
        pm.write_parquet(pm_pq)
        crush.write_parquet(cr_pq)
        con = duckdb.connect()
        con.execute("CREATE TEMP TABLE keys (parent_hash BIGINT, move_san VARCHAR)")
        if keys:
            con.executemany("INSERT INTO keys VALUES (?, ?)", list(keys))
        con.execute("CREATE TEMP TABLE win_w (position_hash BIGINT)")
        if win_w:
            con.executemany("INSERT INTO win_w VALUES (?)", [(h,) for h in win_w])
        con.execute("CREATE TEMP TABLE win_b (position_hash BIGINT)")
        if win_b:
            con.executemany("INSERT INTO win_b VALUES (?)", [(h,) for h in win_b])
        rows = con.execute(winpos_sql(str(pm_pq), str(cr_pq))).fetchall()
        con.close()
    return {(ph, san, mb): (n, w, b) for ph, san, mb, n, w, b in rows}


def run_fused(pm: pl.DataFrame, crush: pl.DataFrame, win_w, win_b,
              keys) -> dict:
    """Same inputs through the inline path, then the caller-side aggregation
    and the merge-time semi-join to `keys` that the fused design defers."""
    pm = pm.sort(["game_id", "ply"])
    evals = [(THRESH if h in win_w else (-THRESH if h in win_b else int(MISSING)))
             for h in pm["parent_hash"].to_list()]
    gids = pm["game_id"].to_list()
    spans, facts = [], []
    cf = {r["game_id"]: (r["white_win_normal"], r["black_win_normal"], r["move_count"])
          for r in crush.iter_rows(named=True)}
    s = 0
    for i in range(1, len(gids) + 1):
        if i == len(gids) or gids[i] != gids[s]:
            spans.append((s, i))
            facts.append(cf[gids[s]])
            s = i
    raw = winpos_batch(pm["parent_hash"].to_list(), pm["move_san"].to_list(),
                       pm["ply"].to_list(), evals, spans, facts, THRESH)
    df = pl.DataFrame(raw).group_by("parent_hash", "move_san", "move_bucket").agg(
        pl.col("n").sum(), pl.col("white_wins").sum(), pl.col("black_wins").sum())
    kset = set(keys)
    return {(ph, sa, mb): (n, w, b)
            for ph, sa, mb, n, w, b in df.iter_rows()
            if (ph, sa) in kset}


def compare(label: str, a: dict, b: dict) -> None:
    if a == b:
        check(True, f"{label}: {len(a)} rows identical")
        return
    only_o = {k: v for k, v in a.items() if a.get(k) != b.get(k)}
    check(False, f"{label}: {len(only_o)} differing keys")
    for k in list(only_o)[:6]:
        print(f"        {k}  oracle={a.get(k)}  fused={b.get(k)}")


REAL_GAMES = 4000


def real_check() -> None:
    """Extract-fused winpos vs the shipped second-pass replay + winpos_sql."""
    import shutil

    import chess
    import numpy as np
    import pyarrow.parquet as pq

    from build_pooled_stats import SOURCE_ROOT, extract_file
    from build_crush_winpos_phase2 import _walk_game_winpos
    from eval_arrays import open_eval_arrays, lookup_evals
    from zobrist import IncrementalZobrist

    src = None
    for year in sorted(SOURCE_ROOT.glob("year=*"), reverse=True):
        for mon in sorted(year.glob("month=*")):
            for ev in sorted(mon.glob("event=*")):
                fs = sorted(ev.glob("*.parquet"))
                if fs:
                    src = fs[0]
                    break
            if src:
                break
        if src:
            break
    if src is None or not (Path("E:/chess/eval_arrays/eval_hash.npy")).exists():
        print("  SKIP real-data level (source parquet or eval arrays unavailable)")
        return
    # The selector takes the newest year on disk, so which games this level
    # actually covers drifts as backfills land. Print it: without this a failure
    # is not reproducible from the output alone.
    print(f"  real-data source: {src.relative_to(SOURCE_ROOT)} "
          f"(first {REAL_GAMES:,} games)")

    tmp = Path(tempfile.mkdtemp(prefix="winpos_fused_"))
    try:
        wp = tmp / f"w{THRESH}.parquet"
        extract_file(src, tmp / "a.ps.parquet", tmp / "a.crush.parquet",
                     min_elo=1800, max_ply=30, tiers=None,
                     limit_games=REAL_GAMES, term_out=tmp / "a.term.parquet",
                     winpos_out={THRESH: wp})
        fused = {(ph, sa, mb): (n, w, b) for ph, sa, mb, n, w, b
                 in pl.read_parquet(wp).iter_rows()}

        # Independent path: the shipped phase-2 replay.
        pm_buf = {"game_id": [], "ply": [], "parent_hash": [], "move_san": [],
                  "elo_band": []}
        cr = {"game_id": [], "white_win_normal": [], "black_win_normal": [],
              "move_count": []}
        hasher = IncrementalZobrist(chess.Board())
        n = 0
        cols = ["movetext", "white_score", "termination", "move_count",
                "mean_elo", "elo_band", "game_id"]
        for batch in pq.ParquetFile(src).iter_batches(batch_size=5000, columns=cols):
            for rec in batch.to_pylist():
                if n >= REAL_GAMES:
                    break
                n += 1
                me, ws = rec["mean_elo"], rec["white_score"]
                if me is None or me < 1800 or ws is None:
                    continue
                normal = rec["termination"] == "Normal"
                _walk_game_winpos(pm_buf, rec["game_id"], rec["movetext"],
                                  rec["elo_band"], 30, hasher)
                cr["game_id"].append(rec["game_id"])
                cr["white_win_normal"].append(1 if (normal and ws == 1.0) else 0)
                cr["black_win_normal"].append(1 if (normal and ws == 0.0) else 0)
                cr["move_count"].append(rec["move_count"])
            if n >= REAL_GAMES:
                break

        pm = pl.DataFrame(pm_buf)
        crush = pl.DataFrame(cr, schema={
            "game_id": pl.Utf8, "white_win_normal": pl.Int32,
            "black_win_normal": pl.Int32, "move_count": pl.Int32})
        h, e = open_eval_arrays()
        uniq = np.unique(np.asarray(pm["parent_hash"].to_list(), dtype=np.int64))
        cp = lookup_evals(uniq, h, e)
        win_w = set(uniq[(cp >= THRESH)].tolist())
        win_b = set(uniq[(cp <= -THRESH) & (cp != int(MISSING))].tolist())
        keys = set(zip(pm["parent_hash"].to_list(), pm["move_san"].to_list()))
        oracle = run_oracle(pm, crush, win_w, win_b, keys)

        compare(f"REAL {src.parent.parent.name}/{src.parent.name} "
                f"({pm.height:,} edges, {len(fused):,} fused rows)", oracle, fused)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> None:
    print("=" * 70)
    print("FUSED WINPOS == winpos_sql")
    print("=" * 70)

    # ---- 1. the oracle's own hand-designed edge cases (G1..G7) --------------
    pm_pq, cr_pq = oracle_fixtures.make_fixtures()
    pm = pl.read_parquet(pm_pq)
    crush = pl.read_parquet(cr_pq)
    keys = [(100, "e4"), (100, "d4"), (100, "g3"), (103, "c5"),
            (109, "Nf3"), (104, "d5"), (106, "Bg5")]
    win_w = {201, 202, 203, 106, 209}
    win_b: set = set()
    compare("G1-G7 synthetic",
            run_oracle(pm, crush, win_w, win_b, keys),
            run_fused(pm, crush, win_w, win_b, keys))

    # ---- 2. randomised games ----------------------------------------------
    import random
    rng = random.Random(20260805)
    rows, cf_rows, kk = [], [], set()
    ww, wb = set(), set()
    for g in range(1, 121):
        nply = rng.randint(1, 14)
        start = rng.randint(1, 6)
        plies = sorted(rng.sample(range(start, start + 24), nply))
        for p in plies:
            h = rng.randint(1, 60)
            san = rng.choice(["e4", "d4", "Nf3", "c5", "Bg5", "Qh5"])
            rows.append((g, p, h, san))
            kk.add((h, san))
            r = rng.random()
            if r < 0.18:
                ww.add(h)
            elif r < 0.34:
                wb.add(h)
        cf_rows.append((g, rng.randint(0, 1), rng.randint(0, 1), rng.randint(1, 95)))
    ww -= wb                                    # a position cannot be both
    pm2 = pl.DataFrame(
        {"game_id": [r[0] for r in rows], "ply": [r[1] for r in rows],
         "parent_hash": [r[2] for r in rows], "move_san": [r[3] for r in rows]},
        schema={"game_id": pl.Int64, "ply": pl.Int32,
                "parent_hash": pl.Int64, "move_san": pl.Utf8},
    ).with_columns(pl.lit(1900, dtype=pl.Int32).alias("elo_band"))
    crush2 = pl.DataFrame(
        {"game_id": [r[0] for r in cf_rows],
         "white_win_normal": [r[1] for r in cf_rows],
         "black_win_normal": [r[2] for r in cf_rows],
         "move_count": [r[3] for r in cf_rows]},
        schema={"game_id": pl.Int64, "white_win_normal": pl.Int32,
                "black_win_normal": pl.Int32, "move_count": pl.Int32})
    compare(f"randomised ({len(rows)} edges, {len(cf_rows)} games)",
            run_oracle(pm2, crush2, ww, wb, kk),
            run_fused(pm2, crush2, ww, wb, kk))

    # ---- 3. the MISSING trap ----------------------------------------------
    # -32768 <= -300 is true. If MISSING is not masked, every uncovered position
    # manufactures a black crossing; in SQL it just fails to join.
    ev_w, ev_b = game_events([1, 2, 3], [int(MISSING)] * 3, THRESH, False, False, 40)
    check(all(v == SENTINEL for v in ev_w) and all(v == SENTINEL for v in ev_b),
          "all-MISSING evals produce no events (the -32768 <= -thresh trap)")

    ev_w, ev_b = game_events([1, 2, 3], [int(MISSING), -400, int(MISSING)],
                             THRESH, False, False, 40)
    check(ev_b[0] == 1 and ev_w == [SENTINEL] * 3,
          "a real black crossing among MISSING values still fires")

    # ---- 4. strict inequality at the edge's own ply ------------------------
    ev_w, _ = game_events([4, 6], [400, 400], THRESH, False, False, 50)
    check(ev_w[0] == 3 and ev_w[1] == SENTINEL,
          "crossing at the edge's OWN ply is excluded (downstream-only)")

    # ---- 5. REAL data, end to end -----------------------------------------
    # The three levels above test the algorithm. This tests the INTEGRATION: the
    # per-game spans into the shared replay buffer, and that the population the
    # batched eval lookup sees is the same one pm.parent_hash represents. It runs
    # the shipped second-pass replay (_walk_game_winpos) independently and feeds
    # winpos_sql, so a spans bug shows up as a mismatch rather than as agreement
    # with itself. NOT independent on the eval values themselves — both sides read
    # the same arrays; that lookup is covered by _test_eval_arrays.py.
    real_check()

    print()
    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
