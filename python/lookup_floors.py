"""Reshape an uncut own-move lookup without rebuilding it: raise the own-move floor, or put the
dropped moves back where a closed book is.

`floor`: keep our moves with at least F games. A full lookup (uncut_own_moves.py) holds every
observed own move, and a full-menu graph costs the builder 3-4 GB per million edges; own moves
played 1-4 times are 43-44% of lookup rows (2026-11-11), so a floor-5 lookup lets the 0.60/1.00
gates fit in memory. The dropped rows' games and evaluation coverage go into the position's
other_* bucket with exactly rebucket_pool.py's arithmetic (held equal by _test_lookup_floors.py),
so every position's continuing population is unchanged. Opponent rows are untouched.

`addback`: from the full lookup, restore our below-floor moves at every our-turn position a
closed run's final book prescribes, and every position its verification passes searched; at those
positions the rows and the sidecar row become the full lookup's exactly. A few thousand positions,
so the graph stays the floored size.

Both write graph.parquet, aux.parquet, aux.parquet.meta.json, recipe.json and ready.json (last),
in the layout uncut_own_moves.load() reads; the recipe records the source lookup(s), so the
recipe hash every book meta carries identifies the transform.

    python python/lookup_floors.py floor --source <full lookup> --colour black --min-games 5 --out <dir>
    python python/lookup_floors.py addback --full <full lookup> --floor <floored lookup> \\
        --run <closed run_rare_book.py dir> [--run ...] --colour black --out <dir>
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import polars as pl
import pyarrow as pa

import uncut_own_moves as lookup
from eval_arrays import MISSING, lookup_evals, open_eval_arrays
from stage3_backwards_induction import LICHESS_CP_SCALE

SUMS = ("total", "white_wins", "draws", "black_wins")
OTHER_SUMS = tuple("other_" + c for c in SUMS)


def ready(folder: Path) -> dict:
    """A finished lookup's ready.json, after checking its manifest and files are unchanged."""
    r = lookup.read_json(folder / "ready.json")
    if r["recipe_sha256"] != lookup.digest(folder / "recipe.json"):
        raise ValueError(f"{folder}: manifest changed")
    for name, expected in r["files"].items():
        if lookup.stamp(folder / name)[1:] != expected:
            raise ValueError(f"{folder}: file changed: {name}")
    return r


def finish(out: Path, contract: dict, recipe: dict, source_meta: Path) -> dict:
    (out / "aux.parquet.meta.json").write_bytes(source_meta.read_bytes())
    lookup.atomic_json(out / "recipe.json", {"contract": contract, **recipe})
    done = {"contract": contract, "recipe_sha256": lookup.digest(out / "recipe.json"),
            "files": {n: lookup.stamp(out / n)[1:]
                      for n in ("graph.parquet", "aux.parquet", "aux.parquet.meta.json")}}
    lookup.atomic_json(out / "ready.json", done)
    shutil.rmtree(out / "spill", ignore_errors=True)
    return done


def connect(out: Path, threads: int, memory: str):
    con = duckdb.connect()
    con.execute(f"SET threads={int(threads)}; SET memory_limit={lookup.literal(memory)}")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory={lookup.literal(str(out / 'spill'))}")
    return con


def register_eval(con) -> None:
    """pos_eval(hash) -> White's expected score from the eval arrays, NULL if unknown (as
    rebucket_pool.py defines the other bucket's coverage)."""
    mm = open_eval_arrays()

    def evaluate(keys):
        valid = np.array(keys.is_valid())
        vals = lookup_evals(np.array(keys.fill_null(0), dtype=np.int64), *mm)
        ws = 1 / (1 + np.exp(-LICHESS_CP_SCALE * vals.astype(float)))
        return pa.array(ws, mask=(~valid) | (vals == MISSING))
    con.create_function("pos_eval", evaluate, ["BIGINT"], "DOUBLE", type="arrow", null_handling="special")


def floor(source: Path, colour: str, min_games: int, out: Path, threads=4, memory="24GB") -> dict:
    src = ready(source)
    if src["contract"]["colour"] != colour:
        raise ValueError("source lookup is for the other colour")
    out.mkdir(parents=True, exist_ok=True)
    if (out / "ready.json").exists():
        return ready(out)
    ours = "w" if colour == "white" else "b"
    g, x = lookup.parquet(source / "graph.parquet"), lookup.parquet(source / "aux.parquet")
    drop = f"split_part(parent_epd,' ',2)='{ours}' AND total<{int(min_games)}"
    with connect(out, threads, memory) as con:
        register_eval(con)
        con.execute(f"""CREATE TEMP TABLE removed AS WITH e AS MATERIALIZED
            (SELECT *, pos_eval(child_hash) ev FROM {g} WHERE {drop})
            SELECT parent_hash h, sum(total)::BIGINT n, sum(white_wins)::BIGINT w,
            sum(draws)::BIGINT d, sum(black_wins)::BIGINT b, count(*)::INTEGER cnt,
            sum(CASE WHEN ev IS NOT NULL THEN total ELSE 0 END)::BIGINT cov,
            sum(total*ev) weighted, min(ev) lo, max(ev) hi FROM e GROUP BY 1""")
        orphans = con.execute(f"SELECT count(*) FROM removed r ANTI JOIN {x} a ON a.position_hash=r.h").fetchone()[0]
        if orphans:
            raise ValueError(f"{orphans} parents of dropped moves have no sidecar row")
        # the source is sorted by (parent_hash, move_san); the filtered copy keeps that order
        lookup.copy(con, f"SELECT * FROM {g} WHERE NOT ({drop}) ORDER BY parent_hash, move_san",
                    out / "graph.parquet")
        # the other bucket absorbs the dropped rows: rebucket_pool.py's formulas, column for column
        lookup.copy(con, f"""WITH x AS (SELECT a.*, r.*,
            round(a.other_total*coalesce(a.other_eval_cov,0))::BIGINT old_cov
            FROM {x} a LEFT JOIN removed r ON a.position_hash=r.h)
            SELECT * EXCLUDE (h,n,w,d,b,cnt,cov,weighted,lo,hi,old_cov) REPLACE (
            other_total+coalesce(n,0) AS other_total, other_white_wins+coalesce(w,0) AS other_white_wins,
            other_draws+coalesce(d,0) AS other_draws, other_black_wins+coalesce(b,0) AS other_black_wins,
            other_edges+coalesce(cnt,0) AS other_edges,
            (coalesce(other_eval_mean,0)*old_cov+coalesce(weighted,0))/nullif(old_cov+coalesce(cov,0),0) AS other_eval_mean,
            least(other_eval_min,lo) AS other_eval_min, greatest(other_eval_max,hi) AS other_eval_max,
            coalesce((old_cov+coalesce(cov,0))/nullif(other_total+coalesce(n,0),0),0) AS other_eval_cov)
            FROM x ORDER BY position_hash""", out / "aux.parquet")
        ng, nx = lookup.parquet(out / "graph.parquet"), lookup.parquet(out / "aux.parquet")
        # games are conserved position by position: named rows + other, before and after
        mass = """SELECT h, sum(t)::BIGINT t, sum(w)::BIGINT w FROM (
            SELECT parent_hash h, total t, white_wins w FROM {g}
            UNION ALL SELECT position_hash, other_total, other_white_wins FROM {x}) GROUP BY h"""
        bad = con.execute(f"""SELECT count(*) FROM ({mass.format(g=g, x=x)}) p
            FULL JOIN ({mass.format(g=ng, x=nx)}) q USING (h)
            WHERE p.t IS DISTINCT FROM q.t OR p.w IS DISTINCT FROM q.w""").fetchone()[0]
        theirs = f"split_part(parent_epd,' ',2)!='{ours}'"
        q = f"SELECT * FROM {{g}} WHERE {theirs}"
        opp = con.execute(f"""SELECT (SELECT count(*) FROM (({q.format(g=g)}) EXCEPT ALL ({q.format(g=ng)}))),
            (SELECT count(*) FROM (({q.format(g=ng)}) EXCEPT ALL ({q.format(g=g)})))""").fetchone()
        rows, kept, gone = con.execute(f"""SELECT (SELECT count(*) FROM {g}), (SELECT count(*) FROM {ng}),
            (SELECT coalesce(sum(cnt),0) FROM removed)""").fetchone()
        if bad or any(opp) or kept + gone != rows:
            raise ValueError(f"floor checks failed: {bad} positions not conserved, opponent rows "
                             f"changed {opp}, rows {kept}+{gone}!={rows}")
    return finish(out, src["contract"], {
        "derived_from": {"lookup": str(source.resolve()), "recipe_sha256": src["recipe_sha256"]},
        "own_min_games": int(min_games), "producer": lookup.digest(__file__),
        "rows": {"source": rows, "kept": kept, "dropped": gone}}, source / "aux.parquet.meta.json")


def booked_and_searched(runs, colour) -> set:
    out = set()
    for run in runs:
        final = json.loads((Path(run) / "final.json").read_text())
        book = pl.read_parquet(final["book"])
        out |= set(book.filter(pl.col("side_to_move") == colour)["position_hash"].to_list())
        for ov in sorted(Path(run).glob("round-*/overlay.parquet")):
            out |= set(pl.read_parquet(ov)["position_hash"].to_list())
    return out


def addback(full: Path, floored: Path, runs, colour: str, out: Path, threads=4, memory="16GB") -> dict:
    whole, low = ready(full), ready(floored)
    rec = lookup.read_json(floored / "recipe.json")
    if rec.get("derived_from", {}).get("recipe_sha256") != whole["recipe_sha256"]:
        raise ValueError("the floored lookup was not derived from this full lookup")
    if whole["contract"] != low["contract"] or whole["contract"]["colour"] != colour:
        raise ValueError("lookups disagree on pool, sidecar or colour")
    min_games = int(rec["own_min_games"])
    out.mkdir(parents=True, exist_ok=True)
    if (out / "ready.json").exists():
        return ready(out)
    pos = sorted(booked_and_searched(runs, colour))
    pl.DataFrame({"position_hash": pos}, schema={"position_hash": pl.Int64}).write_parquet(out / "positions.parquet")
    ours = "w" if colour == "white" else "b"
    fg, fx = lookup.parquet(full / "graph.parquet"), lookup.parquet(full / "aux.parquet")
    lg, lx = lookup.parquet(floored / "graph.parquet"), lookup.parquet(floored / "aux.parquet")
    with connect(out, threads, memory) as con:
        con.execute(f"CREATE TEMP TABLE pos AS SELECT DISTINCT position_hash h FROM {lookup.parquet(out / 'positions.parquet')}")
        con.execute(f"""CREATE TEMP TABLE restored AS SELECT g.* FROM {fg} g SEMI JOIN pos p ON p.h=g.parent_hash
            WHERE split_part(g.parent_epd,' ',2)='{ours}' AND g.total<{min_games}""")
        lookup.copy(con, f"SELECT * FROM {lg} UNION ALL SELECT * FROM restored ORDER BY parent_hash, move_san",
                    out / "graph.parquet")
        # at a chosen position the sidecar row is the full lookup's (only positions with restored rows change)
        changed = "(SELECT DISTINCT parent_hash h FROM restored)"
        lookup.copy(con, f"""SELECT s.* FROM {lx} s ANTI JOIN {changed} c ON c.h=s.position_hash
            UNION ALL SELECT f.* FROM {fx} f SEMI JOIN {changed} c ON c.h=f.position_hash
            ORDER BY position_hash""", out / "aux.parquet")
        ng, nx = lookup.parquet(out / "graph.parquet"), lookup.parquet(out / "aux.parquet")
        q = f"SELECT * FROM {{g}} SEMI JOIN pos p ON p.h=parent_hash WHERE split_part(parent_epd,' ',2)='{ours}'"
        d = con.execute(f"""SELECT (SELECT count(*) FROM (({q.format(g=ng)}) EXCEPT ALL ({q.format(g=fg)}))),
            (SELECT count(*) FROM (({q.format(g=fg)}) EXCEPT ALL ({q.format(g=ng)})))""").fetchone()
        oth = ", ".join(OTHER_SUMS)
        o = con.execute(f"""SELECT count(*) FROM (
            SELECT position_hash, {oth} FROM {nx} SEMI JOIN pos p ON p.h=position_hash
            EXCEPT SELECT position_hash, {oth} FROM {fx} SEMI JOIN pos p ON p.h=position_hash)""").fetchone()[0]
        n_new, n_low, n_back, a_new, a_low = con.execute(f"""SELECT (SELECT count(*) FROM {ng}),
            (SELECT count(*) FROM {lg}), (SELECT count(*) FROM restored),
            (SELECT count(*) FROM {nx}), (SELECT count(*) FROM {lx})""").fetchone()
        if any(d) or o or n_new != n_low + n_back or a_new != a_low:
            raise ValueError(f"add-back checks failed: rows {d}, other {o}, counts {n_new} {n_low} {n_back}")
    return finish(out, whole["contract"], {
        "derived_from": {"lookup": str(floored.resolve()), "recipe_sha256": low["recipe_sha256"],
                         "full_lookup": str(full.resolve()), "full_recipe_sha256": whole["recipe_sha256"]},
        "own_min_games": min_games,
        "restored_below_floor_at": {"positions": len(pos),
                                    "positions_sha256": lookup.digest(out / "positions.parquet"),
                                    "runs": [str(Path(r).resolve()) for r in runs], "rows": n_back},
        "producer": lookup.digest(__file__)}, floored / "aux.parquet.meta.json")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    f = sub.add_parser("floor")
    f.add_argument("--source", type=Path, required=True)
    f.add_argument("--colour", required=True, choices=["white", "black"])
    f.add_argument("--min-games", type=int, required=True)
    f.add_argument("--out", type=Path, required=True)
    b = sub.add_parser("addback")
    b.add_argument("--full", type=Path, required=True)
    b.add_argument("--floor", type=Path, required=True)
    b.add_argument("--run", type=Path, action="append", required=True)
    b.add_argument("--colour", required=True, choices=["white", "black"])
    b.add_argument("--out", type=Path, required=True)
    for p in (f, b):
        p.add_argument("--threads", type=int, default=4)
        p.add_argument("--mem", default="16GB")
    a = ap.parse_args()
    t0 = time.monotonic()
    if a.cmd == "floor":
        done = floor(a.source, a.colour, a.min_games, a.out, a.threads, a.mem)
    else:
        done = addback(a.full, a.floor, a.run, a.colour, a.out, a.threads, a.mem)
    print(f"ready {done['recipe_sha256'][:16]} ({time.monotonic() - t0:.0f}s)", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
