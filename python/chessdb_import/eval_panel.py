"""Step-2 validation panel: how good are ChessDB's scores next to cloud and fishnet, by source group and move
rank, against fresh deeper Stockfish 18 (engine_path.STOCKFISH, the project's engine; the first run, 2026-11-22, used
Stockfish 19, whose cp scale is ~1.47x SF18's, so that option is gone and its files stay apart)
(docs/move-menus-plan-2026-11-21.md; David 2026-11-21: step 2
waits for this panel).

Repeats the 13 Nov overlap test (docs/position-evaluation-ensemble-handoff-2026-11-13.md: SF18 d22, 200
positions, 5 groups) larger, independent of the pilot that set its rules, and per move:
  groups   cloud_deep (cloud depth >= 22), cloud_shallow (< 22), nnue_once (no cloud, NNUE tier, 1 obs),
           nnue_repeated (>= 2 obs), pre_nnue (classical/early tier), gap (no eval-DB row: step 1's fills)
  sample   N per group (default 250 -> 1,500) of book positions with >= 2 games FOUND in ChessDB with >= 1
           scored move, from the buckets given (seed fixed); position EPD from the eval DB, or from the book
           for gap positions
  search   per position: SF18 at --depth on the position (MultiPV 1) and at --depth - 1 on the child after
           each of ChessDB's moves ranked 1-3 (that move's reference score, negated to the parent's mover);
           one search per process task, fresh hash (ucinewgame); results appended to a JSONL, resumable
  report   MAE vs SF18 per source and per 13-Nov rule within each group; per-move ChessDB error by rank; and
           sibling order agreement (does the source rank ChessDB's moves 1 vs 2 the way SF18 does)

Scales: SF and cloud/fishnet cp as is; ChessDB scores through eval_lookup_cdb.chessdb_white_cp's rule
(|s| >= 15000 -> ±2000) for positions, and capped at ±2000 per move; everything clipped to ±2000 before
errors, as the eval arrays store it.

Usage:
  python eval_panel.py sample --buckets 3 7 ... [--per-group 250] [--seed 20261122]
  python eval_panel.py search [--workers 10] [--depth 24]
  python eval_panel.py report
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(1, str(Path(__file__).resolve().parent.parent))
import engine_path  # noqa: E402
import import_menus as im  # noqa: E402

OUT = Path(os.environ.get("EVAL_PANEL_DIR", "E:/chess/eval-panel-20261122"))
EVAL_DB = Path("D:/chess/eval-db-p51-20261115")
MENUS = im.OUT
KEYS = Path("E:/chess/chessdb-scan-20261121/keys")
# The reference engine: the project's one Stockfish (engine_path), pinned by sha256 prefix. It is on the sources'
# cp scale. Stockfish 19 scores ~1.47x more extreme at the same depth (2026-11-22) and is not offered; its first
# run's files (search_d24.jsonl, report_d24.md, panel_scored_d24.parquet) stay in OUT under the untagged names.
ENGINES = {"sf18": (engine_path.STOCKFISH, "c86215fa", engine_path.STOCKFISH_NAME)}


def fname(kind: str, depth: int, engine: str, ext: str) -> Path:
    """Engine-tagged names (search_sf18_d24.jsonl, ...); the untagged names belong to the retired SF19 run."""
    return OUT / f"{kind}_{engine}_d{depth}.{ext}"
GROUPS = ("cloud_deep", "cloud_shallow", "nnue_once", "nnue_repeated", "pre_nnue", "gap")
CLIP = 2000


def clip(x):
    return None if x is None else max(-CLIP, min(CLIP, int(x)))


def cdb_white(score: int, white_to_move: bool) -> int:
    v = score if white_to_move else -score
    return (1 if v > 0 else -1) * CLIP if abs(v) >= 15000 else clip(v)


# ── sample ─────────────────────────────────────────────────────────────────────

def sample(buckets: list[int], per_group: int, seed: int) -> None:
    import duckdb
    f = OUT / "panel.parquet"
    if f.exists():
        raise SystemExit(f"{f} exists: delete it to resample")
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET threads=4; SET memory_limit='12GB'")
    parts = []
    for b in buckets:
        done = MENUS / "_done" / f"bkt{b:03d}.json"
        if not done.exists():
            raise SystemExit(f"bucket {b}: menus not done")
        pos, mv = (MENUS / k / f"bkt{b:03d}.parquet" for k in ("positions", "moves"))
        ev = EVAL_DB / f"bkt{b:03d}.parquet"
        con.execute(f"""CREATE OR REPLACE TEMP TABLE c AS
            SELECT p.lo, p.hi, p.games, p.white_to_move, e.epd, e.source, e.cp, e.mate, e.cloud_depth,
                   e.cloud_cp, e.cloud_mate, e.fishnet_cp, e.fishnet_mate, e.fishnet_tier, e.fishnet_n_tier,
                   CASE WHEN e.lo IS NULL THEN 'gap'
                        WHEN e.cloud_depth >= 22 THEN 'cloud_deep'
                        WHEN e.cloud_depth IS NOT NULL THEN 'cloud_shallow'
                        WHEN e.fishnet_tier = 'nnue' AND e.fishnet_n_tier = 1 THEN 'nnue_once'
                        WHEN e.fishnet_tier = 'nnue' THEN 'nnue_repeated'
                        WHEN e.fishnet_tier IS NOT NULL THEN 'pre_nnue' END AS grp
            FROM read_parquet('{pos.as_posix()}') p
            LEFT JOIN (SELECT position_hash AS lo, position_hash_hi AS hi, any_value(epd) AS epd,
                              any_value(source) AS source, any_value(cp) AS cp, any_value(mate) AS mate,
                              any_value(cloud_depth) AS cloud_depth, any_value(cloud_cp) AS cloud_cp,
                              any_value(cloud_mate) AS cloud_mate, any_value(fishnet_cp) AS fishnet_cp,
                              any_value(fishnet_mate) AS fishnet_mate, any_value(fishnet_tier) AS fishnet_tier,
                              any_value(fishnet_n_tier) AS fishnet_n_tier
                       FROM read_parquet('{ev.as_posix()}') WHERE NOT hash_ambiguous
                       GROUP BY 1, 2 HAVING count(DISTINCT epd) = 1) e USING (lo, hi)
            WHERE p.found AND p.n_moves >= 1""")
        for g in GROUPS:
            parts += [(b, *r) for r in con.execute(
                f"SELECT * FROM c WHERE grp = '{g}' ORDER BY hash(lo, hi, {seed}) LIMIT {per_group}").fetchall()]
    cols = ["bucket"] + [d[0] for d in con.execute("SELECT * FROM c LIMIT 0").description]
    import pandas as pd
    df = pd.DataFrame(parts, columns=cols)
    rng = random.Random(seed)
    keep = []
    for g in GROUPS:
        rows = df[df.grp == g]
        idx = list(rows.index)
        rng.shuffle(idx)
        keep += idx[:per_group]
        if len(idx) < per_group:
            print(f"warning: group {g} has only {len(idx)} candidates in these buckets")
    df = df.loc[sorted(keep)].reset_index(drop=True)
    # gap positions have no eval-DB EPD: take the book's
    gaps = df[df.epd.isna()]
    for b in sorted(gaps.bucket.unique()):
        keys = gaps[gaps.bucket == b][["lo", "hi"]]
        con.register("g", keys)
        from explorer_storage import relation
        files = sorted(im.BOOK.glob(f"ps/event=*/elo_band=*/bkt{b:03d}.parquet"))
        got = dict(((lo, hi), e) for lo, hi, e in con.execute(
            f"""SELECT parent_hash, parent_hash_hi, min(parent_epd) FROM {relation(files)}
                WHERE (parent_hash, parent_hash_hi) IN (SELECT lo, hi FROM g) GROUP BY 1, 2""").fetchall())
        con.unregister("g")
        for i in gaps[gaps.bucket == b].index:
            df.at[i, "epd"] = got.get((df.at[i, "lo"], df.at[i, "hi"]))
    if df.epd.isna().any():
        raise SystemExit(f"{int(df.epd.isna().sum())} sampled positions without an EPD")
    # ChessDB's top-3 moves for the sampled positions
    mv = []
    for b in sorted(df.bucket.unique()):
        con.register("s", df[df.bucket == b][["lo", "hi"]])
        mv += con.execute(f"""SELECT lo, hi, rank, uci, score FROM read_parquet('{(MENUS / "moves" / f"bkt{b:03d}.parquet").as_posix()}')
                              WHERE rank <= 3 AND (lo, hi) IN (SELECT lo, hi FROM s)""").fetchall()
        con.unregister("s")
    pd.DataFrame(mv, columns=["lo", "hi", "rank", "uci", "score"]).to_parquet(OUT / "panel_moves.parquet")
    df.to_parquet(f)
    (OUT / "panel.meta.json").write_text(json.dumps(dict(buckets=buckets, per_group=per_group, seed=seed,
                                         counts=df.grp.value_counts().to_dict(), moves=len(mv)), indent=1))
    print(df.grp.value_counts().to_string(), f"\n{len(mv)} ChessDB moves (ranks 1-3)")


# ── search ─────────────────────────────────────────────────────────────────────

def tasks(depth: int) -> list[dict]:
    import chess
    import pandas as pd
    df = pd.read_parquet(OUT / "panel.parquet")
    mv = pd.read_parquet(OUT / "panel_moves.parquet")
    out = []
    for r in df.itertuples():
        out.append(dict(id=f"{r.lo}:{r.hi}", kind="pos", fen=r.epd + " 0 1", depth=depth))
    epd = {(r.lo, r.hi): r.epd for r in df.itertuples()}
    for m in mv.itertuples():
        b = chess.Board(epd[(m.lo, m.hi)] + " 0 1")
        mo = chess.Move.from_uci(m.uci)
        if mo not in b.legal_moves:
            raise SystemExit(f"illegal ChessDB move {m.uci} in {b.fen()}")
        b.push(mo)
        out.append(dict(id=f"{m.lo}:{m.hi}:{m.rank}", kind="move", fen=b.fen(), depth=depth - 1))
    return out


def search_one(task: dict) -> dict:
    import chess
    import chess.engine
    eng = chess.engine.SimpleEngine.popen_uci(task["engine_path"])
    try:
        eng.configure({"Threads": 1, "Hash": 128})
        b = chess.Board(task["fen"])
        if b.is_game_over():
            res = b.outcome().winner
            cp = 0 if res is None else (CLIP if res == chess.WHITE else -CLIP)
            return dict(task, cp_white=cp, terminal=True, nodes=0, seconds=0.0)
        t = time.time()
        info = eng.analyse(b, chess.engine.Limit(depth=task["depth"]), game=object())
        s = info["score"].white()
        cp = clip(s.score(mate_score=100000))
        return dict(task, cp_white=cp, mate=s.mate(), seldepth=info.get("seldepth"), nodes=info.get("nodes"),
                    seconds=round(time.time() - t, 2), engine=eng.id.get("name"))
    finally:
        eng.quit()


def search(workers: int, depth: int, engine: str) -> None:
    path, prefix, _ = ENGINES[engine]
    h = hashlib.sha256(Path(path).read_bytes()).hexdigest()
    if not h.startswith(prefix):
        raise SystemExit(f"{path} is not the pinned {engine} (sha256 {h[:12]})")
    res = fname("search", depth, engine, "jsonl")
    done = set()
    if res.exists():
        for line in res.read_text(encoding="utf8").splitlines():
            if line.strip():
                done.add(json.loads(line)["id"])
    todo = [dict(t, engine_path=path) for t in tasks(depth) if t["id"] not in done]
    print(f"{len(todo)} searches to do ({len(done)} done), depth {depth}, {workers} workers", flush=True)
    t0, n = time.time(), 0
    with res.open("a", encoding="utf8") as f, cf.ProcessPoolExecutor(workers) as ex:
        it = iter(todo)
        live = {ex.submit(search_one, t) for t in [next(it) for _ in range(min(workers, len(todo)))]}
        while live:
            fin, live = cf.wait(live, return_when=cf.FIRST_COMPLETED)
            for x in fin:
                f.write(json.dumps(x.result()) + "\n")
                f.flush()
                n += 1
                nxt = next(it, None)
                if nxt is not None:
                    live.add(ex.submit(search_one, nxt))
            if n and n % 100 == 0:
                print(f"  {n}/{len(todo)} in {time.time() - t0:.0f}s", flush=True)


# ── report ─────────────────────────────────────────────────────────────────────

def report(depth: int, engine: str) -> None:
    import numpy as np
    import pandas as pd
    df = pd.read_parquet(OUT / "panel.parquet")
    mv = pd.read_parquet(OUT / "panel_moves.parquet")
    label = ENGINES[engine][2]
    rs = pd.read_json(fname("search", depth, engine, "jsonl"), lines=True)
    ref = {r.id: r.cp_white for r in rs.itertuples()}
    df["id"] = df.lo.astype(str) + ":" + df.hi.astype(str)
    df["sf"] = df.id.map(ref)
    best = mv[mv["rank"] == 1].set_index(["lo", "hi"]).score
    df["chessdb"] = [cdb_white(int(best[(r.lo, r.hi)]), bool(r.white_to_move)) for r in df.itertuples()]
    cv = lambda cp, mate: None if cp is None and mate is None else clip(cp if cp is not None and not np.isnan(cp) else np.sign(mate) * CLIP)  # noqa: E731
    df["cloud"] = [cv(a, b) if not (pd.isna(a) and pd.isna(b)) else None for a, b in zip(df.cloud_cp, df.cloud_mate)]
    df["fishnet"] = [cv(a, b) if not (pd.isna(a) and pd.isna(b)) else None for a, b in zip(df.fishnet_cp, df.fishnet_mate)]
    nnue = df.fishnet_tier == "nnue"

    def rule(r):
        c, n, d = r.cloud, (r.fishnet if r.fishnet_tier == "nnue" else None), r.chessdb
        if r.grp == "cloud_deep":
            return (c + n) / 2 if n is not None else c
        if r.grp == "cloud_shallow":
            if n is not None and d is not None:
                return float(np.median([c, n, d]))
            return (c + (n if n is not None else d)) / 2 if (n is not None or d is not None) else c
        if r.grp in ("nnue_once", "nnue_repeated"):
            return (2 * d + n) / 3
        return d
    df["rule_13nov"] = [rule(r) for r in df.itertuples()]
    df["current"] = [clip(r.cp if not pd.isna(r.cp) else np.sign(r.mate) * CLIP) if r.grp != "gap" else None
                     for r in df.itertuples()]
    cols = ("current", "cloud", "fishnet", "chessdb", "rule_13nov")
    lines = [f"# Eval panel vs {label} depth {depth}", "",
             f"{len(df)} positions; errors in cp against {label} (everything clipped to ±{CLIP}); a row missing a "
             "source skips that column. Means are pulled by a few decisive positions, so the median and the "
             "balanced-position tables come first.", ""]

    def table(title: str, keep, stat) -> None:
        lines.extend([title, "", "| group | n | current eval DB | cloud | fishnet | ChessDB | 13-Nov rule |",
                      "|---|---:|---:|---:|---:|---:|---:|"])
        for g in GROUPS:
            x = df[(df.grp == g) & df.sf.notna()]
            x = x[keep(x)]
            cell = lambda c: f"{stat((x[c].astype(float) - x.sf).abs().dropna()):.1f}" if x[c].notna().any() else "—"  # noqa: E731
            lines.append(f"| {g} | {len(x)} | " + " | ".join(cell(c) for c in cols) + " |")
        lines.append("")
    table("Median absolute error:", lambda x: x.sf.notna(), lambda e: e.median())
    table(f"Mean absolute error, balanced positions (|{label}| <= 300 cp):", lambda x: x.sf.abs() <= 300, lambda e: e.mean())
    table("Mean absolute error, all positions:", lambda x: x.sf.notna(), lambda e: e.mean())
    lines += [f"Scale: least-squares slope of {label} on each source through the origin (|{label}| <= 1000; 1.00 = same "
              "cp scale):", "", "| source | slope | n |", "|---|---:|---:|"]
    for c in ("cloud", "fishnet", "chessdb"):
        x = df[df.sf.notna() & df[c].notna() & (df.sf.abs() <= 1000)]
        a_, b_ = x[c].astype(float).values, x.sf.astype(float).values
        lines.append(f"| {c} | {(a_ @ b_) / (a_ @ a_):.2f} | {len(x)} |")
    # per move
    mv["id"] = mv.lo.astype(str) + ":" + mv.hi.astype(str) + ":" + mv["rank"].astype(str)
    mv["sf_child"] = mv.id.map(ref)
    wtm = dict(zip(zip(df.lo, df.hi), df.white_to_move))
    grp = dict(zip(zip(df.lo, df.hi), df.grp))
    mv["cdb"] = [cdb_white(int(s), wtm[(a, b)]) for a, b, s in zip(mv.lo, mv.hi, mv.score)]
    mv["grp"] = [grp[(a, b)] for a, b in zip(mv.lo, mv.hi)]
    lines += ["", f"ChessDB per-move scores vs {label} on the child (depth - 1), MAE cp:", "",
              "| group | rank 1 | rank 2 | rank 3 |", "|---|---:|---:|---:|"]
    for g in GROUPS:
        y = mv[(mv.grp == g) & mv.sf_child.notna()]
        cells = [f"{(y[y['rank'] == k].cdb - y[y['rank'] == k].sf_child).abs().mean():.1f} ({(y['rank'] == k).sum()})"
                 for k in (1, 2, 3)]
        lines.append(f"| {g} | " + " | ".join(cells) + " |")
    pair = mv[mv["rank"] <= 2].pivot_table(index=["lo", "hi"], columns="rank", values=["cdb", "sf_child"]).dropna()
    agree = ((pair[("cdb", 1)] - pair[("cdb", 2)]) * (pair[("sf_child", 1)] - pair[("sf_child", 2)]) >= 0)
    lines += ["", f"Sibling order (ChessDB rank 1 vs 2, White view sign of the difference) agrees with {label} at "
              f"{agree.mean():.1%} of {len(agree)} positions (ties count as agreement)."]
    fname("report", depth, engine, "md").write_text("\n".join(lines) + "\n", encoding="utf8")
    df.to_parquet(fname("panel_scored", depth, engine, "parquet"))
    print("\n".join(lines))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("phase", choices=["sample", "search", "report"])
    ap.add_argument("--buckets", type=int, nargs="*")
    ap.add_argument("--per-group", type=int, default=250)
    ap.add_argument("--seed", type=int, default=20261122)
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--depth", type=int, default=24)
    ap.add_argument("--engine", choices=sorted(ENGINES), default="sf18")
    a = ap.parse_args()
    if a.phase == "sample":
        sample(a.buckets, a.per_group, a.seed)
    elif a.phase == "search":
        search(a.workers, a.depth, a.engine)
    else:
        report(a.depth, a.engine)


if __name__ == "__main__":
    main()
