"""Synthetic test for build_crush_winpos.winpos_sql — verifies the task-#27
guarantees against the EXACT production query (imported, not copied).

Cases:
  G1 cross-then-resign counts ONCE (guarantee 1): cross ply 8 (mv 4) + resign mv 18
     -> one white event at bucket 4, nothing at 18.
  G2 give-away still counts (guarantee 2): cross ply 8, game drawn -> bucket 4.
  G3 downstream-only (guarantee 3): cross at ply 4; edge at ply 2 gets bucket 2,
     edge at ply 9 (cross is upstream) gets NO event.
  G4 uncovered game (guarantee 4): no crossings, black resigns... black WINS by
     Normal at mv 12 -> black event bucket 11 (edge at ply 3, 1 move done).
  G5 both sides, one game: White crosses mv 3 but loses by resignation mv 30 ->
     white bucket 3 AND black bucket 30, n counted once.
  G6 strict inequality: crossings at ply 6 (== edge ply, excluded) and ply 9 ->
     event mv 4, bucket 4-2=2 (a non-strict filter would give bucket 1).
  G7 bucket clip: resign at mv 90 -> bucket 60.

Run: .venv/Scripts/python.exe python/_test_winpos.py
"""
from __future__ import annotations
import sys
from pathlib import Path

import duckdb
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from build_crush_winpos import winpos_sql

SCRATCH = Path(__file__).parent.parent / "_winpos_test_fixtures"
SCRATCH.mkdir(exist_ok=True)


def make_fixtures() -> tuple[Path, Path]:
    pm_rows = [
        # (game_id, ply, parent_hash, move_san)   elo_band/event added below
        (1, 1, 100, "e4"), (1, 8, 201, "xx"),          # G1
        (2, 1, 100, "g3"), (2, 8, 201, "xx"),          # G2
        (3, 2, 103, "c5"), (3, 4, 202, "xx"), (3, 9, 109, "Nf3"),  # G3
        (4, 3, 104, "d5"),                              # G4
        (5, 1, 100, "d4"), (5, 6, 203, "xx"),          # G5
        (6, 6, 106, "Bg5"), (6, 9, 209, "xx"),         # G6 (106 itself is winning)
        (7, 1, 100, "e4"),                              # G7
    ]
    pm = pl.DataFrame(
        {"game_id": [r[0] for r in pm_rows], "ply": [r[1] for r in pm_rows],
         "parent_hash": [r[2] for r in pm_rows], "move_san": [r[3] for r in pm_rows]},
        schema={"game_id": pl.Int64, "ply": pl.Int32,
                "parent_hash": pl.Int64, "move_san": pl.Utf8},
    ).with_columns(pl.lit(1900, dtype=pl.Int32).alias("elo_band"))
    crush = pl.DataFrame(
        {"game_id":          [1, 2, 3, 4, 5, 6, 7],
         "white_win_normal": [1, 0, 0, 0, 0, 0, 1],
         "black_win_normal": [0, 0, 0, 1, 1, 0, 0],
         "move_count":       [18, 40, 50, 12, 30, 60, 90]},
        schema={"game_id": pl.Int64, "white_win_normal": pl.Int32,
                "black_win_normal": pl.Int32, "move_count": pl.Int32},
    )
    pm_pq, crush_pq = SCRATCH / "pm.parquet", SCRATCH / "crush.parquet"
    pm.write_parquet(pm_pq)
    crush.write_parquet(crush_pq)
    return pm_pq, crush_pq


def main() -> None:
    pm_pq, crush_pq = make_fixtures()
    con = duckdb.connect()
    keys = [(100, "e4"), (100, "d4"), (100, "g3"), (103, "c5"),
            (109, "Nf3"), (104, "d5"), (106, "Bg5")]
    con.execute("CREATE TEMP TABLE keys (parent_hash BIGINT, move_san VARCHAR)")
    con.executemany("INSERT INTO keys VALUES (?, ?)", keys)
    con.execute("CREATE TEMP TABLE win_w (position_hash BIGINT)")
    con.executemany("INSERT INTO win_w VALUES (?)",
                    [(201,), (202,), (203,), (106,), (209,)])
    con.execute("CREATE TEMP TABLE win_b (position_hash BIGINT)")   # empty

    rows = con.execute(winpos_sql(str(pm_pq), str(crush_pq))).fetchall()
    got = {(ph, san, mb): (n, w, b) for ph, san, mb, n, w, b in rows}

    expected = {
        (100, "e4", 0):  (2, 0, 0),   # n: G1 + G7
        (100, "e4", 4):  (0, 1, 0),   # G1: min(cross mv 4, resign mv 18) -> ONCE
        (100, "e4", 60): (0, 1, 0),   # G7: resign mv 90 clipped to 60
        (100, "g3", 0):  (1, 0, 0),
        (100, "g3", 4):  (0, 1, 0),   # G2: crossed then drawn -> still counts
        (103, "c5", 0):  (1, 0, 0),
        (103, "c5", 2):  (0, 1, 0),   # G3 early edge: cross ply4 -> mv2, bucket 2
        (109, "Nf3", 0): (1, 0, 0),   # G3 late edge: cross is UPSTREAM -> no event
        (104, "d5", 0):  (1, 0, 0),
        (104, "d5", 11): (0, 0, 1),   # G4: black Normal win mv12, 1 move done
        (100, "d4", 0):  (1, 0, 0),
        (100, "d4", 3):  (0, 1, 0),   # G5 white: crossed mv3 (later lost!)
        (100, "d4", 30): (0, 0, 1),   # G5 black: resignation mv30 — both sides, n once
        (106, "Bg5", 0): (1, 0, 0),
        (106, "Bg5", 2): (0, 1, 0),   # G6: ply-6 crossing excluded (== edge ply),
                                      #     ply-9 crossing -> mv4, bucket 4-2=2
    }

    ok = True
    for k, v in sorted(expected.items()):
        g = got.get(k)
        status = "PASS" if g == v else "FAIL"
        if g != v:
            ok = False
        print(f"  {status}  {k}: expected {v}, got {g}")
    extra = set(got) - set(expected)
    if extra:
        ok = False
        for k in sorted(extra):
            print(f"  FAIL  unexpected row {k}: {got[k]}")
    # No-double-count invariant: every (key) has sum(white)+sum(black) <= games.
    for ph, san in keys:
        n_tot = sum(v[0] for k, v in got.items() if k[:2] == (ph, san))
        w_tot = sum(v[1] for k, v in got.items() if k[:2] == (ph, san))
        b_tot = sum(v[2] for k, v in got.items() if k[:2] == (ph, san))
        if w_tot > n_tot or b_tot > n_tot:
            ok = False
            print(f"  FAIL  double count at ({ph},{san}): n={n_tot} w={w_tot} b={b_tot}")

    print(f"\n{'ALL PASS' if ok else 'FAILURES PRESENT'} "
          f"({len(expected)} expected rows, {len(got)} produced)")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
