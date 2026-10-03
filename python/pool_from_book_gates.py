"""
pool_from_book_gates.py — the acceptance gates for pool_from_book.py (spec section 6).

Each gate writes its verdict into <work>/_gates.json (merged by key), which
`pool_from_book.py --phase meta` copies into the outputs' .meta.json.

  term-vs-book   Gate 1. The builder's own flow derivation (derive_flows), run on
                 ALL 54 book slices for a few output buckets, against the book's
                 pooled term table: kind-0 rows at end_ply = p, 1 <= p <= cap,
                 summed over reason, at (position, end_ply) grain. derived - book
                 must be >= 0 in every component (parse failures only add); the
                 total residual is reported. Needs its own arrivals partition
                 (every slice, restricted to the gate's child buckets): ~1 h.
  reimpl         Gate 2. A second implementation for the real population, written
                 from the spec in Polars + numpy with no bucket machinery: one
                 DuckDB scan for the arrivals, then per bucket edges, survivors,
                 other_* (its own vectorised eval search), term as
                 SUM A(x, p<=cap) - SUM D(x, 2..cap+1), horizon. Compared with the
                 FINISHED outputs: pool rows exactly, aux integers exactly, doubles
                 within 1e-12.
  conservation   Gate 3. From the finished files against fresh book scans:
                 pool total + other_total = population mass at ply <= cap excluding
                 collision parents; per non-root pool parent, SUM A = edges + other +
                 term + horizon component-wise; the root's ply-1 games.
  mass-split     Not a gate: the report's kept / other / outside / term / horizon
                 mass per ply, for several --floors in one pass.
  evals          Gate 4. 10,000 random below-floor edges of pool parents; for their
                 parents other_eval_* recomputed with annotate.facts.EvalDB per key.
                 Plus cov in [0, 1] and min <= mean <= max over the whole sidecar.

Usage (same population flags as the build; --work is the build's work dir):
  python pool_from_book_gates.py term-vs-book --work W --buckets 0 128 156 189 200 383 511 R
  python pool_from_book_gates.py reimpl --work W --buckets ...
  python pool_from_book_gates.py conservation --work W
  python pool_from_book_gates.py evals --work W
"""
from __future__ import annotations

import argparse
import json
import math
import random
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from types import SimpleNamespace

import numpy as np

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

ALL_EVENTS = ["Blitz", "Bullet", "Classical", "Correspondence", "Rapid", "UltraBullet"]
ALL_BANDS = [0, 1000, 1200, 1400, 1600, 1800, 2000, 2200, 2500]
SUMS = ("white_wins", "draws", "black_wins", "total")


def _p(path) -> str:
    return str(path).replace("\\", "/")


def record(work: Path, key: str, result: dict) -> None:
    p = work / "_gates.json"
    g = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    g[key] = result
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(g, indent=2), encoding="utf-8")
    tmp.replace(p)


def load_params(work: Path) -> dict:
    return json.loads((work / "_params.json").read_text(encoding="utf-8"))


def out_files(work: Path, out_dir: Path) -> tuple[Path, Path]:
    tag = load_params(work)["tag"]
    return (out_dir / f"position_stats_pooled_{tag}.parquet",
            out_dir / f"position_stats_aux_pooled_{tag}.parquet")


def _duck(a):
    import pool_from_book as P
    return P._duck(a.threads, a.mem, Path(a.tmp_dir))


# ── gate 1: the derivation against the book's term table ──────────────────────

def _term_bucket(task: tuple) -> dict:
    import pool_from_book as P
    i, book, max_ply, adir, threads, mem, tmp = task
    t0 = time.time()
    files = P.slice_files(Path(book), ALL_EVENTS, ALL_BANDS, i)
    con = P._duck(threads, mem, Path(tmp))
    try:
        P.derive_flows(con, files, _p(Path(adir) / "g*" / f"cb={i}" / "*.parquet"), max_ply)
        neg_en = con.execute("SELECT COUNT(*) FROM en WHERE " +
                             " OR ".join(f"{c} < 0" for c in SUMS)).fetchone()[0]
        s = ", ".join(f"SUM({c})::BIGINT AS {c}" for c in SUMS)
        con.execute(f"""
            CREATE TEMP TABLE bt AS
            SELECT position_hash AS x, end_ply AS p, {s}
            FROM read_parquet('{_p(Path(book) / 'term' / f'bkt{i:03d}.parquet')}')
            WHERE kind = 0 AND end_ply BETWEEN 1 AND {max_ply}
            GROUP BY position_hash, end_ply
        """)
        diff = ", ".join(f"COALESCE(en.{c}, 0) - COALESCE(bt.{c}, 0) AS {c}" for c in SUMS)
        con.execute(f"""
            CREATE TEMP TABLE df AS
            SELECT COALESCE(en.x, bt.x) AS x, COALESCE(en.p, bt.p) AS p, {diff}
            FROM (SELECT * FROM en WHERE p BETWEEN 1 AND {max_ply}) en
            FULL OUTER JOIN bt ON bt.x = en.x AND bt.p = en.p
        """)
        neg = " OR ".join(f"{c} < 0" for c in SUMS)
        r = con.execute(f"""
            SELECT COUNT(*) FILTER (WHERE {neg}),
                   COUNT(*) FILTER (WHERE total <> 0 OR white_wins <> 0 OR draws <> 0
                                     OR black_wins <> 0),
                   {', '.join(f'SUM({c})' for c in SUMS)}, MAX(total)
            FROM df""").fetchone()
        book_term = con.execute("SELECT SUM(total) FROM bt").fetchone()[0] or 0
        derived = con.execute(f"SELECT SUM(total) FROM en WHERE p BETWEEN 1 AND {max_ply}"
                              ).fetchone()[0] or 0
        sample = con.execute(f"SELECT * FROM df WHERE {neg} LIMIT 5").fetchall() if r[0] else []
        end0 = con.execute(f"""SELECT SUM(total) FROM read_parquet(
            '{_p(Path(book) / 'term' / f'bkt{i:03d}.parquet')}') WHERE kind = 0 AND end_ply = 0
            """).fetchone()[0] or 0
    finally:
        con.close()
    return {"bucket": i, "files": len(files), "neg_ended_cells": int(neg_en),
            "neg_diff_cells": int(r[0]), "nonzero_diff_cells": int(r[1]),
            "residual": {c: int(v or 0) for c, v in zip(SUMS, r[2:6])},
            "max_cell_residual": int(r[6] or 0), "book_term_total": int(book_term),
            "derived_term_total": int(derived), "book_end_ply0_total": int(end0),
            "neg_sample": [list(map(int, x)) for x in sample],
            "secs": round(time.time() - t0, 1)}


def gate_term_vs_book(a) -> bool:
    import pool_from_book as P
    work = Path(a.work)
    prm = load_params(work)
    max_ply = int(prm["max_ply"])
    gdir = work / "gate1"
    adir = gdir / "arrivals"
    ns = SimpleNamespace(book=Path(prm["book"]), events=ALL_EVENTS, elo_bands=ALL_BANDS,
                         max_ply=max_ply, threads=a.threads, mem=a.mem,
                         tmp_dir=a.tmp_dir, workers=a.workers)
    t0 = time.time()
    P.phase_arrivals(ns, child_buckets=a.buckets, adir=adir)
    t_arr = time.time() - t0
    rdir = gdir / "results"
    rdir.mkdir(parents=True, exist_ok=True)
    tasks = [(i, prm["book"], max_ply, str(adir), a.threads, a.mem, str(a.tmp_dir))
             for i in a.buckets if not (rdir / f"b{i:03d}.json").exists()]
    for r in P._pool_run(_term_bucket, tasks, a.workers,
                         lambda r: f"bucket {r['bucket']}: neg {r['neg_diff_cells']}, "
                                   f"residual {r['residual']['total']:,} of "
                                   f"{r['book_term_total']:,} ({r['secs']:.0f}s)"):
        (rdir / f"b{r['bucket']:03d}.json").write_text(json.dumps(r, indent=2))
    res = [json.loads((rdir / f"b{i:03d}.json").read_text()) for i in a.buckets]
    neg = sum(r["neg_diff_cells"] + r["neg_ended_cells"] for r in res)
    resid = {c: sum(r["residual"][c] for r in res) for c in SUMS}
    bt = sum(r["book_term_total"] for r in res)
    ok = neg == 0
    out = {"pass": ok, "buckets": a.buckets, "slices": "all 54", "max_ply": max_ply,
           "negative_cells": neg, "residual": resid, "book_term_total": bt,
           "residual_share": resid["total"] / bt if bt else None,
           "nonzero_cells": sum(r["nonzero_diff_cells"] for r in res),
           "max_cell_residual": max(r["max_cell_residual"] for r in res),
           "book_end_ply0_total_in_buckets": sum(r["book_end_ply0_total"] for r in res),
           "arrivals_s": round(t_arr, 1), "per_bucket": res}
    record(work, "term_vs_book", out)
    print(f"\nGATE 1 term-vs-book: {'PASS' if ok else 'FAIL'}  negative cells {neg}, "
          f"residual {resid['total']:,} of {bt:,} book term games "
          f"({out['residual_share']:.3e}), nonzero cells {out['nonzero_cells']:,}")
    return ok


# ── gate 2: an independent implementation ──────────────────────────────────────

def _slice_globs(book: Path, events, bands) -> list[str]:
    return [_p(book / "ps" / f"event={e}" / f"elo_band={b}" / "*.parquet")
            for e in events for b in bands
            if (book / "ps" / f"event={e}" / f"elo_band={b}").exists()]


def _rem(x: int) -> int:
    return x % 512


def gate_reimpl(a) -> bool:
    import polars as pl
    import pyarrow.parquet as pq
    from eval_arrays import MISSING, open_eval_arrays
    from stage3_backwards_induction import LICHESS_CP_SCALE
    work = Path(a.work)
    prm = load_params(work)
    book, cap, floor = Path(prm["book"]), int(prm["max_ply"]), int(prm["min_games"])
    events, bands = prm["events"], prm["elo_bands"]
    pool_f, aux_f = out_files(work, Path(a.out_dir))
    coll = set(pl.read_parquet(book / "_collisions.parquet")["parent_hash"].to_list())
    t0 = time.time()

    # Arrivals for the gate's buckets: ONE plain scan of the population.
    con = _duck(a)
    globs = ", ".join(f"'{g}'" for g in _slice_globs(book, events, bands))
    bl = ", ".join(map(str, a.buckets))
    arr = pl.from_arrow(con.execute(f"""
        SELECT child_hash, SUM(white_wins) w, SUM(draws) d, SUM(black_wins) b, SUM(total) t
        FROM read_parquet([{globs}], hive_partitioning=false)
        WHERE ply <= {cap} AND ((child_hash % 512) + 512) % 512 IN ({bl})
        GROUP BY child_hash""").arrow())
    con.close()
    print(f"  arrivals scan: {arr.height:,} children ({time.time()-t0:.0f}s)", flush=True)

    mm_h, mm_c = open_eval_arrays(Path(prm["eval_arrays"]))
    pool_all = pl.scan_parquet(pool_f)
    aux_all = pl.scan_parquet(aux_f)
    bad = []
    stats = []
    for i in a.buckets:
        tb = time.time()
        frames = []
        for e in events:
            for b in bands:
                f = book / "ps" / f"event={e}" / f"elo_band={b}" / f"bkt{i:03d}.parquet"
                if f.exists():
                    # Polars' own reader rejects the book's parquet-rs files
                    # ("Invalid thrift: bad data", polars 1.40); pyarrow's
                    # ParquetFile reads them (README: partitioning stays off).
                    frames.append(pl.from_arrow(pq.ParquetFile(f).read(
                        columns=["parent_hash", "parent_epd", "move_san", "child_hash",
                                 "ply", *SUMS])))
        rows = pl.concat(frames)
        le = rows.filter(pl.col("ply") <= cap)
        edges = (le.filter(~pl.col("parent_hash").is_in(list(coll)))
                 .group_by("parent_hash", "move_san")
                 .agg(pl.col("parent_epd").first(), pl.col("parent_epd").n_unique().alias("ne"),
                      pl.col("child_hash").first(), pl.col("child_hash").n_unique().alias("nc"),
                      pl.col("ply").min(), *[pl.col(c).sum() for c in SUMS]))
        assert edges["ne"].max() == 1 and edges["nc"].max() == 1, "edge not single-valued"
        surv = (edges.filter(pl.col("total") >= floor)
                .with_columns(pl.lit("Pooled").alias("event"),
                              pl.lit(0, dtype=pl.Int64).alias("elo_band"),
                              ((pl.col("white_wins") + 0.5 * pl.col("draws"))
                               / pl.col("total")).alias("white_score_avg"))
                .select("parent_hash", "move_san", "parent_epd", "child_hash",
                        pl.col("ply").cast(pl.Int32), *SUMS, "event", "elo_band",
                        "white_score_avg")
                .sort("parent_hash", "move_san"))
        mine_pool = (pool_all.filter(((pl.col("parent_hash") % 512) + 512) % 512 == i)
                     .sort("parent_hash", "move_san").collect())
        pool_eq = surv.equals(mine_pool)

        # other_*: own vectorised search into the arrays (not lookup_evals).
        below = edges.filter(pl.col("total") < floor)
        keys = below["child_hash"].to_numpy()
        uk = np.unique(keys)
        pos = np.searchsorted(mm_h, uk)
        pos = np.minimum(pos, mm_h.shape[0] - 1)
        found = np.asarray(mm_h[pos]) == uk
        cpv = np.where(found, np.asarray(mm_c[pos]).astype(np.float64), np.nan)
        cpv[np.asarray(mm_c[pos]) == MISSING] = np.nan
        cp_of = np.full(keys.shape[0], np.nan)
        cp_of[:] = cpv[np.searchsorted(uk, keys)]
        es = 1.0 / (1.0 + np.exp(-LICHESS_CP_SCALE * cp_of))
        below = below.with_columns(pl.Series("es", es).fill_nan(None))
        oth = below.group_by("parent_hash").agg(
            pl.col("total").sum().alias("other_total"),
            pl.col("white_wins").sum().alias("other_white_wins"),
            pl.col("draws").sum().alias("other_draws"),
            pl.col("black_wins").sum().alias("other_black_wins"),
            pl.len().cast(pl.Int32).alias("other_edges"),
            (pl.col("total") * pl.col("es")).sum().alias("_num"),
            pl.col("total").filter(pl.col("es").is_not_null()).sum().alias("_cov"),
            pl.col("es").min().alias("other_eval_min"),
            pl.col("es").max().alias("other_eval_max"))
        oth = oth.with_columns(
            pl.when(pl.col("_cov") > 0).then(pl.col("_num") / pl.col("_cov"))
            .otherwise(None).alias("other_eval_mean"),
            (pl.col("_cov") / pl.col("other_total")).alias("other_eval_cov")
        ).drop("_num", "_cov")

        # term(x) = SUM A(x, p<=cap) - SUM D(x, q in 2..cap+1); horizon = D(x, cap+1)
        dep = rows.filter(pl.col("ply").is_between(2, cap + 1)).group_by("parent_hash").agg(
            *[pl.col(c).sum().alias("d_" + c) for c in SUMS])
        hor = rows.filter(pl.col("ply") == cap + 1).group_by("parent_hash").agg(
            *[pl.col(c).sum().alias("horizon_" + c) for c in SUMS])
        ab = arr.filter(((pl.col("child_hash") % 512) + 512) % 512 == i).rename(
            {"child_hash": "parent_hash", "w": "a_white_wins", "d": "a_draws",
             "b": "a_black_wins", "t": "a_total"})
        keys_df = surv.select("parent_hash").unique()
        aux = (keys_df.join(ab, on="parent_hash", how="left")
               .join(dep, on="parent_hash", how="left")
               .join(hor, on="parent_hash", how="left")
               .join(oth, on="parent_hash", how="left").fill_null(strategy="zero"))
        # fill_null zeroed the eval columns too: restore the NULLs from oth.
        aux = aux.drop("other_eval_mean", "other_eval_min", "other_eval_max").join(
            oth.select("parent_hash", "other_eval_mean", "other_eval_min", "other_eval_max"),
            on="parent_hash", how="left")
        aux = aux.with_columns(
            *[(pl.col("a_" + c) - pl.col("d_" + c)).alias("term_other_" + c) for c in SUMS],
            *[pl.lit(0, dtype=pl.Int64).alias(f"{g}_{c}") for g in ("term_normal", "term_flag")
              for c in SUMS])
        cols = pl.scan_parquet(aux_f).collect_schema().names()
        aux = (aux.rename({"parent_hash": "position_hash"})
               .with_columns(pl.col("other_edges").cast(pl.Int32),
                             pl.col("other_eval_cov").cast(pl.Float64))
               .select(cols).sort("position_hash"))
        mine_aux = (aux_all.filter(((pl.col("position_hash") % 512) + 512) % 512 == i)
                    .sort("position_hash").collect())
        aux_ok = aux.height == mine_aux.height and \
            (aux["position_hash"] == mine_aux["position_hash"]).all()
        worst = 0.0
        if aux_ok:
            for c in cols:
                x, y = aux[c], mine_aux[c]
                if x.dtype.is_float():
                    if not (x.is_null() == y.is_null()).all():
                        aux_ok = False
                        bad.append(f"b{i} {c}: null pattern differs")
                        continue
                    d = (x - y).abs().max()
                    worst = max(worst, d or 0.0)
                    if d is not None and d > 1e-12:
                        aux_ok = False
                        bad.append(f"b{i} {c}: max |diff| {d}")
                elif not (x == y).all():
                    aux_ok = False
                    bad.append(f"b{i} {c}: integers differ")
        if not pool_eq:
            bad.append(f"b{i}: pool rows differ ({surv.height} vs {mine_pool.height})")
        stats.append({"bucket": i, "pool_rows": surv.height, "aux_rows": aux.height,
                      "pool_equal": pool_eq, "aux_equal": aux_ok, "max_double_diff": worst,
                      "secs": round(time.time() - tb, 1)})
        print(f"  bucket {i}: pool {surv.height:,} {'==' if pool_eq else '!='}, aux "
              f"{aux.height:,} {'==' if aux_ok else '!='} (max double diff {worst:.2e}) "
              f"({time.time()-tb:.0f}s)", flush=True)
    ok = not bad
    record(work, "reimpl", {"pass": ok, "buckets": a.buckets, "problems": bad,
                            "per_bucket": stats})
    print(f"\nGATE 2 reimpl: {'PASS' if ok else 'FAIL'} {bad[:10]}")
    return ok


# ── gate 3: conservation ──────────────────────────────────────────────────────

def gate_conservation(a) -> bool:
    import chess
    from stage1_extract_positions import zobrist_int64
    work = Path(a.work)
    prm = load_params(work)
    book, cap = Path(prm["book"]), int(prm["max_ply"])
    pool_f, aux_f = out_files(work, Path(a.out_dir))
    root = int(zobrist_int64(chess.Board()))
    con = _duck(a)
    globs = ", ".join(f"'{g}'" for g in _slice_globs(book, prm["events"], prm["elo_bands"]))
    pop = f"read_parquet([{globs}], hive_partitioning=false)"
    coll = f"(SELECT DISTINCT parent_hash FROM read_parquet('{_p(book / '_collisions.parquet')}'))"
    t0 = time.time()
    pool_t = con.execute(f"SELECT SUM(total) FROM read_parquet('{_p(pool_f)}')").fetchone()[0]
    oth_t = con.execute(f"SELECT SUM(other_total) FROM read_parquet('{_p(aux_f)}')").fetchone()[0]
    pop_t, pop_coll = con.execute(f"""
        SELECT SUM(total) FILTER (WHERE parent_hash NOT IN {coll}),
               SUM(total) FILTER (WHERE parent_hash IN {coll})
        FROM {pop} WHERE ply <= {cap}""").fetchone()
    mass_ok = int(pool_t) + int(oth_t) == int(pop_t)
    print(f"  mass: pool {pool_t:,} + other {oth_t:,} = {pool_t + oth_t:,} vs population "
          f"{pop_t:,} (collision parents {pop_coll:,}) -> {'OK' if mass_ok else 'MISMATCH'} "
          f"({time.time()-t0:.0f}s)", flush=True)

    # Per pool parent: SUM_{p<=cap} A = edges + other + term + horizon.
    t1 = time.time()
    s = ", ".join(f"SUM({c})::BIGINT AS {c}" for c in SUMS)
    con.execute(f"""CREATE TEMP TABLE pp AS SELECT DISTINCT parent_hash AS x
                    FROM read_parquet('{_p(pool_f)}')""")
    con.execute(f"""
        CREATE TEMP TABLE arr AS
        SELECT child_hash AS x, {s} FROM {pop}
        WHERE ply <= {cap} AND child_hash IN (SELECT x FROM pp)
        GROUP BY child_hash""")
    con.execute(f"""
        CREATE TEMP TABLE pe AS SELECT parent_hash AS x, {s}
        FROM read_parquet('{_p(pool_f)}') GROUP BY parent_hash""")
    rhs = lambda c: (f"COALESCE(pe.{c}, 0) + ax.other_{c} + ax.term_other_{c} "
                     f"+ ax.term_normal_{c} + ax.term_flag_{c} + ax.horizon_{c}")
    mism, n_par, a_tot = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE {' OR '.join(
                   f'COALESCE(arr.{c}, 0) <> {rhs(c)}' for c in SUMS)}),
               COUNT(*), SUM(arr.total)
        FROM read_parquet('{_p(aux_f)}') ax
        LEFT JOIN pe ON pe.x = ax.position_hash
        LEFT JOIN arr ON arr.x = ax.position_hash
        WHERE ax.position_hash <> {root}""").fetchone()
    sample = con.execute(f"""
        SELECT ax.position_hash, arr.total, {rhs('total')}
        FROM read_parquet('{_p(aux_f)}') ax
        LEFT JOIN pe ON pe.x = ax.position_hash LEFT JOIN arr ON arr.x = ax.position_hash
        WHERE ax.position_hash <> {root} AND COALESCE(arr.total, 0) <> {rhs('total')}
        LIMIT 5""").fetchall() if mism else []
    print(f"  per-parent: {mism:,} of {n_par:,} non-root pool parents mismatch "
          f"({time.time()-t1:.0f}s) {sample}", flush=True)

    # The root: ply-1 games.
    ply1_book = con.execute(f"SELECT SUM(total) FROM {pop} WHERE parent_hash = {root} "
                            f"AND ply = 1").fetchone()[0]
    sl = con.execute(f"""SELECT SUM(ply1_games) FROM read_parquet('{_p(book / '_slices.parquet')}')
        WHERE event IN ({', '.join(f"'{e}'" for e in prm['events'])})
          AND elo_band IN ({', '.join(map(str, prm['elo_bands']))})""").fetchone()[0]
    root_dep = con.execute(f"SELECT SUM(total) FROM {pop} WHERE parent_hash = {root} "
                           f"AND ply <= {cap}").fetchone()[0]
    root_pool = con.execute(f"""SELECT
        (SELECT SUM(total) FROM read_parquet('{_p(pool_f)}') WHERE parent_hash = {root}),
        (SELECT other_total FROM read_parquet('{_p(aux_f)}') WHERE position_hash = {root})
        """).fetchone()
    root_pool_ply1 = con.execute(f"""SELECT SUM(total) FROM read_parquet('{_p(pool_f)}')
        WHERE parent_hash = {root} AND ply = 1""").fetchone()[0]
    con.close()
    root_ok = int(ply1_book) == int(sl) and int(root_pool[0]) + int(root_pool[1]) == int(root_dep)
    print(f"  root: book ply-1 games {ply1_book:,} vs _slices ply1_games {sl:,}; pool root "
          f"edges {root_pool[0]:,} + other {root_pool[1]:,} = {root_pool[0]+root_pool[1]:,} vs "
          f"book departures at ply <= {cap} {root_dep:,} (returns {root_dep - ply1_book:,})",
          flush=True)
    ok = mass_ok and mism == 0 and root_ok
    record(work, "conservation", {
        "pass": ok, "pool_total": int(pool_t), "other_total": int(oth_t),
        "population_le_cap_excl_collisions": int(pop_t),
        "collision_parent_mass": int(pop_coll or 0), "mass_identity": mass_ok,
        "non_root_parents": int(n_par), "per_parent_mismatches": int(mism),
        "arrivals_into_pool_parents": int(a_tot or 0),
        "root_ply1_games_book": int(ply1_book), "root_ply1_games_slices": int(sl),
        "root_pool_edges_total": int(root_pool[0]), "root_pool_other_total": int(root_pool[1]),
        "root_pool_ply1_edges_total": int(root_pool_ply1 or 0),
        "root_departures_le_cap": int(root_dep), "root_ok": root_ok})
    print(f"\nGATE 3 conservation: {'PASS' if ok else 'FAIL'}")
    return ok


# ── gate 4: evals ──────────────────────────────────────────────────────────────

def gate_evals(a) -> bool:
    import polars as pl
    from annotate.facts import EvalDB
    from stage3_backwards_induction import LICHESS_CP_SCALE
    work = Path(a.work)
    prm = load_params(work)
    book, cap, floor = Path(prm["book"]), int(prm["max_ply"]), int(prm["min_games"])
    pool_f, aux_f = out_files(work, Path(a.out_dir))
    aux = pl.read_parquet(aux_f)

    # Whole-sidecar invariants.
    m = aux.filter(pl.col("other_eval_mean").is_not_null())
    inv = {
        "cov_out_of_range": aux.filter((pl.col("other_eval_cov") < 0)
                                       | (pl.col("other_eval_cov") > 1)).height,
        "min_gt_mean": m.filter(pl.col("other_eval_min") > pl.col("other_eval_mean") + 1e-15).height,
        "mean_gt_max": m.filter(pl.col("other_eval_mean") > pl.col("other_eval_max") + 1e-15).height,
        "null_mean_but_cov": aux.filter(pl.col("other_eval_mean").is_null()
                                        & (pl.col("other_eval_cov") != 0)).height,
        "mean_but_zero_cov": m.filter(pl.col("other_eval_cov") == 0).height,
    }
    print(f"  invariants over {aux.height:,} rows: {inv}", flush=True)

    # 10,000 random below-floor edges of pool parents, from a few random buckets.
    rng = random.Random(a.seed)
    bks = sorted(rng.sample(range(512), a.sample_buckets))
    coll = set(pl.read_parquet(book / "_collisions.parquet")["parent_hash"].to_list())
    pp = set(aux["position_hash"].to_list())
    con = _duck(a)
    below = []
    for i in bks:
        files = [_p(book / "ps" / f"event={e}" / f"elo_band={b}" / f"bkt{i:03d}.parquet")
                 for e in prm["events"] for b in prm["elo_bands"]
                 if (book / "ps" / f"event={e}" / f"elo_band={b}" / f"bkt{i:03d}.parquet").exists()]
        lst = ", ".join(f"'{f}'" for f in files)
        below.append(pl.from_arrow(con.execute(f"""
            SELECT parent_hash, move_san, MIN(child_hash) AS child_hash, SUM(total) AS total
            FROM read_parquet([{lst}], hive_partitioning=false) WHERE ply <= {cap}
            GROUP BY parent_hash, move_san HAVING SUM(total) < {floor}""").arrow()))
    con.close()
    below = pl.concat(below).filter(pl.col("parent_hash").is_in(list(pp))
                                    & ~pl.col("parent_hash").is_in(list(coll)))
    samp = below.sample(n=min(a.n, below.height), seed=a.seed)
    parents = samp["parent_hash"].unique()
    edges = below.filter(pl.col("parent_hash").is_in(parents))
    print(f"  sampled {samp.height:,} edges from buckets {bks}; {parents.len():,} parents, "
          f"{edges.height:,} below-floor edges to evaluate", flush=True)
    db = EvalDB(prm["eval_arrays"])
    t0 = time.time()
    exp = {}
    for (h,), g in edges.group_by(["parent_hash"]):
        num = cov = tot = 0.0
        mn, mx = math.inf, -math.inf
        for c, t in zip(g["child_hash"].to_list(), g["total"].to_list()):
            tot += t
            cp = db.get_cp(int(c))
            if cp is None or cp == -32768:
                continue
            es = 1.0 / (1.0 + math.exp(-LICHESS_CP_SCALE * cp))
            num += t * es
            cov += t
            mn, mx = min(mn, es), max(mx, es)
        exp[int(h)] = (num / cov if cov else None, mn if cov else None,
                       mx if cov else None, cov / tot, int(tot), g.height)
    got = aux.filter(pl.col("position_hash").is_in(parents)).to_dicts()
    bad = []
    worst = 0.0
    for r in got:
        e = exp[r["position_hash"]]
        if (r["other_total"], r["other_edges"]) != (e[4], e[5]):
            bad.append((r["position_hash"], "mass/edges", r["other_total"], e[4]))
            continue
        for k, v in zip(("other_eval_mean", "other_eval_min", "other_eval_max",
                         "other_eval_cov"), e[:4]):
            if (r[k] is None) != (v is None):
                bad.append((r["position_hash"], k, r[k], v))
            elif v is not None:
                worst = max(worst, abs(r[k] - v))
                if abs(r[k] - v) > 1e-12:
                    bad.append((r["position_hash"], k, r[k], v))
    if len(got) != parents.len():
        bad.append(("parents found", len(got), parents.len()))
    ok = not bad and not any(inv.values())
    record(work, "evals", {"pass": ok, "invariants": inv, "sample_edges": samp.height,
                           "sample_buckets": bks, "parents_checked": len(got),
                           "edges_evaluated": edges.height, "max_abs_diff": worst,
                           "problems": [list(map(str, b)) for b in bad[:20]],
                           "lookup_s": round(time.time() - t0, 1)})
    print(f"\nGATE 4 evals: {'PASS' if ok else 'FAIL'}  {len(got):,} parents checked, "
          f"max |diff| {worst:.2e}, problems {bad[:5]}")
    return ok


# ── report helper: the mass split by ply, for several floors at once ───────────

def _split_bucket(task: tuple) -> dict:
    import pool_from_book as P
    i, book, events, bands, cap, floors, adir, coll, threads, mem, tmp = task
    files = P.slice_files(Path(book), events, bands, i)
    con = P._duck(threads, mem, Path(tmp))
    out = {}
    try:
        con.execute("CREATE TEMP TABLE coll (h BIGINT)")
        if coll:
            con.executemany("INSERT INTO coll VALUES (?)", [(int(h),) for h in coll])
        con.execute(f"""CREATE TEMP TABLE r AS SELECT parent_hash, move_san, ply, total
            FROM {P._read(files, 'parent_hash, move_san, ply, total', f'ply <= {cap}')}""")
        con.execute("""CREATE TEMP TABLE e AS SELECT parent_hash, move_san, SUM(total) AS t,
            parent_hash IN (SELECT h FROM coll) AS c FROM r GROUP BY parent_hash, move_san""")
        P.derive_flows(con, files, _p(Path(adir) / "g*" / f"cb={i}" / "*.parquet"), cap)
        for F in floors:
            con.execute(f"""CREATE OR REPLACE TEMP TABLE pp AS SELECT DISTINCT parent_hash AS x
                FROM e WHERE NOT c AND t >= {F}""")
            mv = con.execute(f"""
                SELECT r.ply,
                  SUM(r.total) FILTER (WHERE NOT e.c AND e.t >= {F}),
                  SUM(r.total) FILTER (WHERE NOT e.c AND e.t < {F} AND pp.x IS NOT NULL),
                  SUM(r.total) FILTER (WHERE NOT e.c AND pp.x IS NULL),
                  SUM(r.total) FILTER (WHERE e.c)
                FROM r JOIN e USING (parent_hash, move_san)
                LEFT JOIN pp ON pp.x = r.parent_hash GROUP BY r.ply""").fetchall()
            en = con.execute("""SELECT p, SUM(total) FILTER (WHERE pp.x IS NOT NULL),
                SUM(total) FILTER (WHERE pp.x IS NULL) FROM en LEFT JOIN pp ON pp.x = en.x
                GROUP BY p""").fetchall()
            hz = con.execute(f"""SELECT SUM(total) FILTER (WHERE pp.x IS NOT NULL),
                SUM(total) FILTER (WHERE pp.x IS NULL) FROM d LEFT JOIN pp ON pp.x = d.x
                WHERE q = {cap + 1}""").fetchone()
            out[str(F)] = {
                "moves": {int(q): [int(v or 0) for v in rest] for q, *rest in mv},
                "ended": {int(p): [int(a or 0), int(b or 0)] for p, a, b in en},
                "horizon": [int(v or 0) for v in hz]}
    finally:
        con.close()
    return {"bucket": i, "split": out}


def report_mass_split(a) -> bool:
    """Per ply: moves played from pool parents along kept edges / below-floor edges
    (the aux 'other'), moves from positions that are not pool parents (outside),
    collision parents; games ended at a pool parent / elsewhere; horizon. Both
    floors in one pass, because survivors and pool parents are functions of the
    per-edge totals alone. Written to <work>/_mass_split.json."""
    import polars as pl
    import pool_from_book as P
    work = Path(a.work)
    prm = load_params(work)
    adir = Path(a.arrivals) if a.arrivals else work / "arrivals"
    coll = pl.read_parquet(Path(prm["book"]) / "_collisions.parquet")["parent_hash"].unique().to_list()
    rdir = work / "mass_split"
    rdir.mkdir(exist_ok=True)
    tasks = [(i, prm["book"], prm["events"], prm["elo_bands"], int(prm["max_ply"]), a.floors,
              str(adir), coll, a.threads, a.mem, str(a.tmp_dir))
             for i in range(512) if not (rdir / f"b{i:03d}.json").exists()]
    for r in P._pool_run(_split_bucket, tasks, a.workers, lambda r: f"bucket {r['bucket']}"):
        (rdir / f"b{r['bucket']:03d}.json").write_text(json.dumps(r))
    tot: dict = {}
    for i in range(512):
        r = json.loads((rdir / f"b{i:03d}.json").read_text())
        for F, s in r["split"].items():
            t = tot.setdefault(F, {"moves": {}, "ended": {}, "horizon": [0, 0]})
            for q, v in s["moves"].items():
                t["moves"][q] = [x + y for x, y in zip(t["moves"].get(q, [0] * 4), v)]
            for p, v in s["ended"].items():
                t["ended"][p] = [x + y for x, y in zip(t["ended"].get(p, [0, 0]), v)]
            t["horizon"] = [x + y for x, y in zip(t["horizon"], s["horizon"])]
    (work / "_mass_split.json").write_text(json.dumps(
        {"columns": {"moves": ["kept", "other_at_pool_parent", "outside", "collision"],
                     "ended": ["at_pool_parent", "elsewhere"],
                     "horizon": ["at_pool_parent", "elsewhere"]},
         "floors": tot}, indent=1))
    print(f"wrote {work / '_mass_split.json'}")
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("gate", choices=["term-vs-book", "reimpl", "conservation", "evals",
                                     "mass-split"])
    ap.add_argument("--work", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, default=Path("E:/chess/position-stats"))
    ap.add_argument("--buckets", nargs="*", type=int,
                    default=[0, 128, 156, 189, 200, 383, 511])
    ap.add_argument("--random-bucket", action="store_true",
                    help="Add one random bucket (seeded) to --buckets")
    ap.add_argument("--seed", type=int, default=20261003)
    ap.add_argument("--n", type=int, default=10_000, help="evals: edges to sample")
    ap.add_argument("--sample-buckets", type=int, default=8)
    ap.add_argument("--floors", nargs="+", type=int, default=[50, 20],
                    help="mass-split: the floors to classify by")
    ap.add_argument("--arrivals", type=Path, default=None,
                    help="mass-split: arrivals dir (default <work>/arrivals)")
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--mem", default="8GB")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--tmp-dir", type=Path, default=Path("D:/chess_duckdb_tmp"))
    a = ap.parse_args()
    if a.random_bucket:
        rest = [b for b in range(512) if b not in a.buckets]
        a.buckets = a.buckets + [random.Random(a.seed).choice(rest)]
    print(f"gate {a.gate}: buckets {a.buckets}", flush=True)
    fn = {"term-vs-book": gate_term_vs_book, "reimpl": gate_reimpl,
          "conservation": gate_conservation, "evals": gate_evals,
          "mass-split": report_mass_split}[a.gate]
    return 0 if fn(a) else 1


if __name__ == "__main__":
    sys.exit(main())
