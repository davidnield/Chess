"""pool_from_book's per-bucket build equals a brute-force walk of the games.

A tiny synthetic book is written in the real layout (two slices, ply-keyed rows,
hive dirs, a collisions file, a mmap-able eval-array pair), from ~400 seeded
random games whose every ply is known. The builder's own arrivals partitioning
and per-bucket task run on it, and every output row is checked against values
computed game by game, which shares no code with the flow algebra:

  A  pool edges: sums over slices and plies at ply <= cap, ply = MIN, floor applied
  B  other_*: below-floor mass and edge count, es averaged AFTER the sigmoid over
     covered edges only, min/max, cov; MISSING never read as a value
  C  term_other_*: games whose walk stopped at x within the cap (incl. a knight
     round trip ending back on the root); term_normal/flag are 0
  D  horizon_*: games still going after `cap` plies, at their ply-cap position
  E  a collision parent is dropped as a parent but kept in the flows; a collision
     parent with a surviving edge raises
  F  missing arrivals make ended(x, p) negative, and the build raises
  G  a bucket no slice reaches writes empty outputs instead of failing

Run: .venv/Scripts/python.exe python/_test_pool_from_book.py
"""
from __future__ import annotations

import json
import math
import random
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

import chess
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import pool_from_book as P  # noqa: E402
from stage3_backwards_induction import LICHESS_CP_SCALE  # noqa: E402
from zobrist import zobrist_int64  # noqa: E402

CAP, FLOOR = 6, 3
SLICES = [("Blitz", 2000), ("Rapid", 2200)]
_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def make_games(rng: random.Random) -> list[dict]:
    games = []
    for g in range(400):
        b = chess.Board()
        n = rng.choice([1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 15])
        moves = []
        for _ in range(n):
            legal = sorted(b.san(m) for m in b.legal_moves)
            if not legal:
                break
            san = legal[min(int(rng.expovariate(1.0)), len(legal) - 1)]
            moves.append(san)
            b.push_san(san)
        games.append({"moves": moves, "res": rng.choice("WWDBB"),
                      "slice": SLICES[g % 2]})
    # A knight round trip ends back on the root at ply 4.
    games.append({"moves": ["Nf3", "Nf6", "Ng1", "Ng8"], "res": "D", "slice": SLICES[0]})
    return games


def walk(moves):
    """[(parent_hash, parent_epd, san, child_hash, ply)] and the position hashes."""
    b = chess.Board()
    out, hashes = [], [zobrist_int64(b)]
    for k, san in enumerate(moves, 1):
        ph, pe = zobrist_int64(b), b.epd()
        b.push_san(san)
        out.append((ph, pe, san, zobrist_int64(b), k))
        hashes.append(zobrist_int64(b))
    return out, hashes


def wdb(res: str) -> tuple[int, int, int]:
    return {"W": (1, 0, 0), "D": (0, 1, 0), "B": (0, 0, 1)}[res]


def write_book(root: Path, games: list[dict], coll: list[tuple[int, str]]) -> None:
    rows = defaultdict(lambda: [0, 0, 0, 0])
    for g in games:
        w, d, bl = wdb(g["res"])
        for ph, pe, san, ch, k in walk(g["moves"])[0]:
            r = rows[(g["slice"], ph, pe, san, ch, k)]
            r[0] += w; r[1] += d; r[2] += bl; r[3] += 1
    recs = [{"event": s[0], "elo_band": s[1], "parent_hash": ph, "parent_epd": pe,
             "move_san": san, "child_hash": ch, "ply": k, "white_wins": c[0],
             "draws": c[1], "black_wins": c[2], "total": c[3],
             "white_score_avg": (c[0] + 0.5 * c[1]) / c[3]}
            for (s, ph, pe, san, ch, k), c in rows.items()]
    df = pl.DataFrame(recs).with_columns(pl.col("ply").cast(pl.Int32),
                                         (pl.col("parent_hash") % 512).alias("_b"))
    for (ev, band, bk), part in df.group_by(["event", "elo_band", "_b"]):
        d = root / "ps" / f"event={ev}" / f"elo_band={band}"
        d.mkdir(parents=True, exist_ok=True)
        part.drop("_b").write_parquet(d / f"bkt{bk:03d}.parquet")
    pl.DataFrame({"parent_hash": [h for h, _ in coll], "parent_epd": [e for _, e in coll]}
                 ).write_parquet(root / "_collisions.parquet")


def write_evals(d: Path, children: list[int], rng: random.Random) -> dict[int, int]:
    d.mkdir(parents=True)
    known = {h: rng.choice([rng.randint(-400, 400), 2000, -2000])
             for h in children if rng.random() < 0.6}
    hs = np.array(sorted(known), dtype=np.int64)
    np.save(d / "eval_hash.npy", hs)
    np.save(d / "eval_cp.npy", np.array([known[h] for h in hs], dtype=np.int16))
    return known


def expected(games, coll_hashes, evals):
    edge = defaultdict(lambda: {"c": [0, 0, 0, 0], "ply": 99, "child": None, "epd": None})
    term = defaultdict(lambda: [0, 0, 0, 0])
    hor = defaultdict(lambda: [0, 0, 0, 0])
    for g in games:
        w, d, bl = wdb(g["res"])
        steps, hashes = walk(g["moves"])
        for ph, pe, san, ch, k in steps:
            if k <= CAP:
                e = edge[(ph, san)]
                for j, v in enumerate((w, d, bl, 1)):
                    e["c"][j] += v
                e["ply"] = min(e["ply"], k)
                e["child"], e["epd"] = ch, pe
        L = len(steps)
        if 1 <= L <= CAP:
            x = hashes[L]
            for j, v in enumerate((w, d, bl, 1)):
                term[x][j] += v
        if L >= CAP + 1:
            x = hashes[CAP]
            for j, v in enumerate((w, d, bl, 1)):
                hor[x][j] += v
    pool = {k: v for k, v in edge.items() if v["c"][3] >= FLOOR and k[0] not in coll_hashes}
    parents = {k[0] for k in pool}
    aux = {}
    for x in parents:
        below = [v for (h, _), v in edge.items() if h == x and v["c"][3] < FLOOR]
        cov = [(v["c"][3], 1 / (1 + math.exp(-LICHESS_CP_SCALE * evals[v["child"]])))
               for v in below if v["child"] in evals]
        ot = sum(v["c"][3] for v in below)
        ct = sum(t for t, _ in cov)
        aux[x] = {"other": [sum(v["c"][j] for v in below) for j in range(4)],
                  "edges": len(below),
                  "mean": sum(t * e for t, e in cov) / ct if ct else None,
                  "min": min(e for _, e in cov) if cov else None,
                  "max": max(e for _, e in cov) if cov else None,
                  "cov": ct / ot if ot else 0.0,
                  "term": term.get(x, [0, 0, 0, 0]), "hor": hor.get(x, [0, 0, 0, 0])}
    return pool, aux


def run_build(td: Path, book: Path, ev: Path, coll: list[int], adir: Path | None,
              tag: str) -> tuple[pl.DataFrame, pl.DataFrame, list[dict]]:
    work = td / tag
    (work / "buckets").mkdir(parents=True)
    events, bands = [s[0] for s in SLICES], [s[1] for s in SLICES]
    if adir is None:
        adir = P.arrivals_dir(work)
        P._arrivals_group((str(book), events, bands, list(range(512)), CAP, None,
                           str(adir / "_tmp_g000"), str(adir / "g000"), 2, "1GB",
                           str(td / "duck")))
    used = sorted({int(p.stem[3:]) for p in book.glob("ps/*/*/bkt*.parquet")}
                  | {int(p.name[3:]) for p in adir.glob("g*/cb=*")})
    stats = []
    for i in used:
        stats.append(P._bucket_task((i, str(book), events, bands, CAP, FLOOR, str(ev),
                                     str(work), str(adir), coll, P.root_hash(), 2, "1GB",
                                     str(td / "duck")))[1])
    e = pl.concat([pl.read_parquet(P.bucket_paths(work, i)["edges"]) for i in used])
    a = pl.concat([pl.read_parquet(P.bucket_paths(work, i)["aux"]) for i in used])
    return e, a, stats


def close(a, b) -> bool:
    return (a is None and b is None) or (a is not None and b is not None
                                         and abs(a - b) <= 1e-12)


def main() -> int:
    rng = random.Random(7)
    games = make_games(rng)
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        # Collision: a parent whose edges are all below the floor.
        cnt = defaultdict(int)
        for g in games:
            for ph, _, san, _, k in walk(g["moves"])[0]:
                if k <= CAP:
                    cnt[(ph, san)] += 1
        par_max = defaultdict(int)
        for (ph, _), c in cnt.items():
            par_max[ph] = max(par_max[ph], c)
        thin = sorted(h for h, m in par_max.items() if 1 < m < FLOOR)[0]
        fat = max(par_max, key=par_max.get)
        book = td / "book"
        write_book(book, games, [(thin, "a"), (thin, "b")])
        children = sorted({ch for g in games for *_, ch, _ in walk(g["moves"])[0]})
        evals = write_evals(td / "ev", children, rng)
        pool, aux = expected(games, {thin}, evals)

        e, a, stats = run_build(td, book, td / "ev", [thin], None, "w1")
        print("A  pool edges")
        got = {(r["parent_hash"], r["move_san"]): r for r in e.iter_rows(named=True)}
        check(set(got) == set(pool), f"edge set ({len(got)} vs {len(pool)} expected)")
        check(all([got[k][c] for c in P.SUMS] == pool[k]["c"] and got[k]["ply"] == pool[k]["ply"]
                  and got[k]["child_hash"] == pool[k]["child"]
                  and got[k]["parent_epd"] == pool[k]["epd"] for k in pool if k in got),
              "counts, MIN ply, child_hash and parent_epd")
        rows = {r["position_hash"]: r for r in a.iter_rows(named=True)}
        check(set(rows) == set(aux), f"one aux row per pool parent ({len(rows)})")
        print("B  other_*")
        check(all([rows[x][f"other_{c}"] for c in P.SUMS] == aux[x]["other"]
                  and rows[x]["other_edges"] == aux[x]["edges"] for x in aux),
              "below-floor mass and edge counts")
        check(all(close(rows[x]["other_eval_mean"], aux[x]["mean"])
                  and close(rows[x]["other_eval_min"], aux[x]["min"])
                  and close(rows[x]["other_eval_max"], aux[x]["max"])
                  and close(rows[x]["other_eval_cov"], aux[x]["cov"]) for x in aux),
              "eval mean/min/max/cov (sigmoid per edge, covered only)")
        check(any(aux[x]["mean"] is None and aux[x]["other"][3] > 0 for x in aux),
              "the fixture has an uncovered bucket (NULL mean exercised)")
        print("C  term_other_*")
        check(all([rows[x][f"term_other_{c}"] for c in P.SUMS] == aux[x]["term"] for x in aux),
              "ended mass per pool parent")
        check(all(rows[x][f"{g}_{c}"] == 0 for x in aux for g in ("term_normal", "term_flag")
                  for c in P.SUMS), "term_normal/term_flag are 0")
        root = P.root_hash()
        check(root in rows and rows[root]["term_other_total"] >= 1,
              "the round trip ended on the root counts there")
        print("D  horizon_*")
        check(all([rows[x][f"horizon_{c}"] for c in P.SUMS] == aux[x]["hor"] for x in aux),
              "horizon mass per pool parent")
        check(sum(r["horizon_total"] for r in rows.values()) > 0, "fixture has horizon mass")
        print("E  collisions")
        check(thin not in rows and all(k[0] != thin for k in got),
              "collision parent dropped from pool and aux")
        check(sum(s["coll_mass"] for s in stats) == sum(v for (h, _), v in cnt.items()
                                                         if h == thin),
              "its dropped mass is reported")
        try:
            run_build(td, book, td / "ev", [fat], P.arrivals_dir(td / "w1"), "w2")
            check(False, "a collision parent with a surviving edge raises")
        except RuntimeError as ex:
            check("floor" in str(ex), "a collision parent with a surviving edge raises")
        print("F  negative ended")
        empty = td / "noarr"
        (empty / "g000").mkdir(parents=True)
        try:
            run_build(td, book, td / "ev", [thin], empty, "w3")
            check(False, "missing arrivals raise")
        except RuntimeError as ex:
            check("NEGATIVE" in str(ex), "missing arrivals raise (ended < 0)")
        print("G  empty bucket")
        used = {int(p.stem[3:]) for p in book.glob("ps/*/*/bkt*.parquet")} | \
            {int(p.name[3:]) for p in P.arrivals_dir(td / "w1").glob("g*/cb=*")}
        free = min(set(range(512)) - used)
        _, st = P._bucket_task((free, str(book), [s[0] for s in SLICES],
                                [s[1] for s in SLICES], CAP, FLOOR, str(td / "ev"),
                                str(td / "w1"), str(P.arrivals_dir(td / "w1")), [thin],
                                root, 2, "1GB", str(td / "duck")))
        pths = P.bucket_paths(td / "w1", free)
        check(st["survivors"] == 0 and pl.read_parquet(pths["aux"]).height == 0
              and P._schema_list(pths["aux"])[0] == ("position_hash", "int64"),
              "an unreached bucket writes empty, typed outputs")

    failed = [label for ok, label in _checks if not ok]
    print(f"\n{len(_checks) - len(failed)}/{len(_checks)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
