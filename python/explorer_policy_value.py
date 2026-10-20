"""Holdout policy value of many books at once, read from an explorer month dir.

Course benchmarks goal 2 (performance), and item 7's yardstick without a per-game
replay. replay_holdout replays every game against every book in lockstep: exact, but
hours per colour for a handful of books, far too slow for the ~830 course books. An
explorer month (`explorer-extract month`) already aggregates the same games into
position edges, so a book's POLICY value -- the expected score of a player who follows
it against the holdout population -- can be read from it directly:

  our node u, book move m   V(u) = V(child after m). If no holdout game played m at
                            u, the mean score of the games at u (`unplayed`).
  opponent node v           V(v) = sum over replies r of n(v,r)/N(v) * V'(r), plus the
                            games that ENDED at v or ran past the explorer's ply cap
                            there, at their own mean score. V'(r) = V(child) when the
                            child is one of the book's positions; otherwise the mean
                            score of the games on edge (v, r) (`left`): the book ends
                            and play goes on like the population's.

Scores are from the book's side. Position-keyed, like the optimiser's model: a
transposition pools every path into it, where the lockstep replay's counts
(scratch budget_policy_value.py) are path-specific. A repetition back to a position
still on the walk's stack reads that edge's own mean.

THIN MOVES. A book move few holdout games played is judged on those few: a move 6
games of 2,737 chose, then 3, then 1 lost game, gives the whole subtree 0.0 at the
node's full weight (2026-10-20, White price book, 1.d4 b6). --shrink K pulls such a
node toward the mean of every game there by K pseudo-games,
    V(u) = (n_m * V(child) + K * mean(u)) / (n_m + K),   n_m = games that played m,
which trades that variance for a bias toward the population's own choice. Default 0
(raw). SE: sqrt(sum over leaves of w^2 * m~(1-m~)/n) with m~ = (s+1)/(n+2), so a
one-game leaf is not counted as certain.

Measured 2026-10-20 (Aug vs Sep 2026 split-half, ~830 course books, 1800+ B/R/C): the
raw value of a course book moves 0.003-0.004 rms between months, K=30 halves that, and
our own books (which play rarer moves) moved up to 0.03 raw but within ~0.005 at K=30.
For a weaker side's games (scratch side_explorer.py) raw values are far noisier (rms
0.010-0.013). K also changes WHAT is measured: with few holdout players on the book's
deep moves, K=30 values a book as if its first moves were played and the rest left to
the population -- partial memorisation -- while raw credits the few who followed it.
Report both. Against replay_holdout's lockstep counts this agreed for Black books and
read ~0.03 higher for White ones, whose 1.d4 lines transpose heavily (the replay
scorer splits a transposed child's games by its parents' totals, not by edge counts).

Positions past the explorer's --max-ply have no edges, so every book is cut at the
same depth: a deeper course gets no credit below it (it counts as `cap`).

    .venv/Scripts/python.exe python/explorer_policy_value.py \\
        --explorer E:/chess/holdout_explorer/ge1800_brc_2026_08_09 \\
        [--book LABEL=COLOUR=PATH ...] [--courses] [--out PATH]
"""
from __future__ import annotations

import argparse
import math
import sys
import time
from collections import defaultdict
from pathlib import Path

import duckdb
import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from repertoire_stats import ROOT_HASH, nsan  # noqa: E402

COURSES = Path("E:/chess/courses")
TERM_ENDED, TERM_HORIZON = 0, 1     # build_pooled_stats' term kinds
KINDS = ("left", "unplayed", "ended", "cap", "cycle", "shrunk")


def _p(path) -> str:
    return str(path).replace("\\", "/")


def read_book(path, colour: str) -> dict[int, str]:
    """Our-turn position -> book move (SAN, check marks stripped)."""
    d = (pl.read_parquet(path, columns=["position_hash", "side_to_move", "best_move"])
         .filter((pl.col("side_to_move") == colour) & pl.col("best_move").is_not_null()))
    return dict(zip(d["position_hash"].to_list(), [nsan(m) for m in d["best_move"].to_list()]))


class HoldoutIndex:
    """The explorer's edges and stops at the positions a set of books can reach,
    summed over event, rating band and month: the books' own moves and the totals at
    their positions (one scan), the replies at the positions those moves reach (a
    second), and the games that stop at any of them (the term tables)."""

    def __init__(self, explorer, books: list[dict[int, str]], threads: int = 8,
                 mem_gb: int = 16, log=print, months: list[str] | None = None):
        t0 = time.time()
        root = _p(explorer)
        if months:                      # "2026_8" -> month=2026_8, year=2026_month=8
            ps = "read_parquet([" + ", ".join(
                f"'{root}/month={m}/bkt=*/*.parquet'" for m in months) + "])"
            term = "read_parquet([" + ", ".join(
                "'{}/_term/year={}_month={}.term.parquet'".format(root, *m.split("_"))
                for m in months) + "])"
        else:
            ps = f"read_parquet('{root}/month=*/bkt=*/*.parquet')"
            term = f"read_parquet('{root}/_term/*.term.parquet')"
        pairs = {(h, s) for b in books for h, s in b.items()}
        bm = pl.DataFrame({"position_hash": [h for h, _ in pairs], "san": [s for _, s in pairs]},
                          schema={"position_hash": pl.Int64, "san": pl.Utf8})
        con = duckdb.connect()
        con.execute(f"SET threads={threads}; SET memory_limit='{mem_gb}GB'; "
                    "SET preserve_insertion_order=false")
        con.register("bm_df", bm.to_arrow())
        con.execute("CREATE TEMP TABLE bpos AS SELECT DISTINCT position_hash FROM bm_df")
        con.execute(f"""CREATE TEMP TABLE f AS
            SELECT parent_hash, move_san, child_hash, total, white_wins + 0.5 * draws AS ws
            FROM {ps} WHERE parent_hash IN (SELECT position_hash FROM bpos)""")
        our = con.execute("""
            SELECT f.parent_hash, b.san, MIN(f.child_hash) AS child,
                   SUM(f.total)::DOUBLE AS n, SUM(f.ws)::DOUBLE AS ws
            FROM f JOIN bm_df b ON f.parent_hash = b.position_hash
                               AND rtrim(f.move_san, '+#') = b.san
            WHERE f.child_hash IS NOT NULL
            GROUP BY 1, 2""").pl()
        tot = con.execute("SELECT parent_hash, SUM(total)::DOUBLE AS n, SUM(ws)::DOUBLE AS ws "
                          "FROM f GROUP BY 1").pl()
        con.execute("DROP TABLE f")
        log(f"  holdout: {our.height:,} of {bm.height:,} book moves played "
            f"({time.time() - t0:,.0f}s)")
        # opponent nodes: what our moves reach, and the start position (a Black book's)
        kids = pl.concat([our.select(pl.col("child").alias("h")),
                          pl.DataFrame({"h": [ROOT_HASH]}, schema={"h": pl.Int64})]).unique()
        con.register("kids_df", kids.to_arrow())
        opp = con.execute(f"""
            SELECT parent_hash, child_hash, SUM(total)::DOUBLE AS n,
                   SUM(white_wins + 0.5 * draws)::DOUBLE AS ws
            FROM {ps} WHERE parent_hash IN (SELECT h FROM kids_df) AND child_hash IS NOT NULL
            GROUP BY 1, 2 ORDER BY 1""").pl()
        stop_at = pl.concat([bm.select(pl.col("position_hash").alias("h")), kids]).unique()
        con.register("stop_df", stop_at.to_arrow())
        stops = con.execute(f"""
            SELECT position_hash, kind, SUM(total)::DOUBLE AS n,
                   SUM(white_wins + 0.5 * draws)::DOUBLE AS ws
            FROM {term} WHERE position_hash IN (SELECT h FROM stop_df)
            GROUP BY 1, 2""").pl()
        con.close()
        self.our = {(h, s): (c, n, w) for h, s, c, n, w in our.iter_rows()}
        self.tot = {h: (n, w) for h, n, w in tot.iter_rows()}
        self.op = opp["parent_hash"].to_numpy()
        self.oc = opp["child_hash"].to_numpy()
        self.on = opp["n"].to_numpy()
        self.ow = opp["ws"].to_numpy()
        self.stop: dict[int, list[float]] = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0])
        for h, k, n, w in stops.iter_rows():
            s = self.stop[h]
            j = 0 if k == TERM_ENDED else 2
            s[j] += n
            s[j + 1] += w
        log(f"  holdout: {len(self.op):,} replies at {kids.height:,} positions, "
            f"{stops.height:,} stop rows ({time.time() - t0:,.0f}s)")

    def replies(self, h: int) -> tuple[int, int]:
        return (int(np.searchsorted(self.op, h, "left")),
                int(np.searchsorted(self.op, h, "right")))

    def node_total(self, h: int) -> tuple[float, float]:
        """(games, white score sum) of every game at h: its edges and its stops."""
        n, w = self.tot.get(h, (0.0, 0.0))
        s = self.stop.get(h)
        if s is not None:
            n, w = n + s[0] + s[2], w + s[1] + s[3]
        return n, w


def policy_value(book: dict[int, str], colour: str, H: HoldoutIndex,
                 detail: bool = False, shrink: float = 0.0) -> dict | None:
    """The book's policy value on the holdout (see the module docstring) and where its
    games leave it: `mass` by kind sums to 1 with the book's moves weighted by reach.
    detail=True adds every visited position's value `V` and policy weight `w`."""
    white = colour == "white"

    def sc(n, w):                       # score sum from our side
        return w if white else n - w

    V: dict[int, float] = {}
    kids: dict[int, list] = {}          # node -> [(child, prob)]
    leaves: dict[int, list] = {}        # node -> [(prob, mean, games, kind)]
    ours: set[int] = set()
    state: dict[int, int] = {}          # 0 on the stack, 1 done
    order: list[int] = []

    def val(h: int, our: bool) -> float | None:
        if state.get(h) == 1:
            return V[h]
        state[h] = 0
        kl, ll = [], []
        if our:
            ours.add(h)
            e = H.our.get((h, book[h]))
            c = e[0] if e is not None else None
            v = None
            if c is not None and state.get(c) != 0:
                v = val(c, False)
            if v is not None and shrink > 0:
                nt, wt = H.node_total(h)
                a = e[1] / (e[1] + shrink)
                if nt > 0 and a < 1:
                    mu = sc(nt, wt) / nt
                    v = a * v + (1 - a) * mu
                    ll.append((1 - a, mu, nt, "shrunk"))
                    kl.append((c, a))
                else:
                    kl.append((c, 1.0))
            elif v is not None:
                kl.append((c, 1.0))
            elif e is not None:         # a repetition (or a child with no games): the edge
                n, w = e[1], e[2]
                v = sc(n, w) / n
                ll.append((1.0, v, n, "cycle"))
            else:                       # nobody played the book move here
                n, w = H.node_total(h)
                if n <= 0:
                    state[h] = 1
                    V[h] = None
                    return None
                v = sc(n, w) / n
                # no edges, games past the cap: the explorer's horizon, not a gap
                cap = h not in H.tot and (H.stop.get(h) or (0, 0, 0))[2] > 0
                ll.append((1.0, v, n, "cap" if cap else "unplayed"))
        else:
            lo, hi = H.replies(h)
            st = H.stop.get(h) or (0.0, 0.0, 0.0, 0.0)
            N = float(H.on[lo:hi].sum()) + st[0] + st[2]
            if N <= 0:
                state[h] = 1
                V[h] = None
                return None
            acc = 0.0
            for k in range(lo, hi):
                c, n, w = int(H.oc[k]), float(H.on[k]), float(H.ow[k])
                cv = None
                if c in book and state.get(c) != 0:
                    cv = val(c, True)
                if cv is not None:
                    acc += n * cv
                    kl.append((c, n / N))
                else:
                    s = sc(n, w)
                    acc += s
                    ll.append((n / N, s / n, n, "cycle" if c in book else "left"))
            for j, kind in ((0, "ended"), (2, "cap")):
                if st[j] > 0:
                    s = sc(st[j], st[j + 1])
                    acc += s
                    ll.append((st[j] / N, s / st[j], st[j], kind))
            v = acc / N
        V[h], kids[h], leaves[h] = v, kl, ll
        state[h] = 1
        order.append(h)
        return v

    if white and ROOT_HASH not in book:
        return None
    root_v = val(ROOT_HASH, white)
    if root_v is None:
        return None
    w = defaultdict(float)
    w[ROOT_HASH] = 1.0
    var = played = 0.0
    mass = dict.fromkeys(KINDS, 0.0)
    for h in reversed(order):
        wh = w[h]
        if not wh:
            continue
        if h in ours and kids[h]:
            played += wh
        for c, p in kids[h]:
            w[c] += wh * p
        for p, m, n, kind in leaves[h]:
            mass[kind] += wh * p
            mt = (m * n + 1) / (n + 2)
            var += (wh * p) ** 2 * mt * (1 - mt) / n
    return {"value": root_v, "se": math.sqrt(var), "moves_played": played,
            "positions_reached": sum(1 for h in ours if w[h] > 0),
            **{f"mass_{k}": mass[k] for k in KINDS},
            **({"V": V, "w": dict(w)} if detail else {})}


def course_specs(work: Path) -> list[tuple[str, str, str, str]]:
    cb = pl.read_parquet(work / "course_books.parquet",
                         columns=["course_id", "side", "course_name"])
    out = []
    for cid, side, name in cb.iter_rows():
        p = work / "books" / f"{cid}_{side}.parquet"
        if p.exists():
            out.append((name, side, str(p), cid))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--explorer", required=True, help="explorer-extract month output dir")
    ap.add_argument("--book", action="append", default=[], metavar="LABEL=COLOUR=PATH")
    ap.add_argument("--courses", action="store_true",
                    help="also score every course book under --work")
    ap.add_argument("--work", default=str(COURSES))
    ap.add_argument("--out", default=None, help="parquet of one row per book")
    ap.add_argument("--shrink", type=float, default=0.0,
                    help="pseudo-games pulling a thinly played book move toward the "
                         "node's mean (default 0 = raw; see THIN MOVES)")
    ap.add_argument("--months", default=None,
                    help="comma-separated month tags (2026_8,...) to read; default all")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--mem-gb", type=int, default=16)
    a = ap.parse_args()
    t0 = time.time()
    specs = []
    for b in a.book:
        label, colour, path = b.split("=", 2)
        specs.append((label, colour, path, None))
    if a.courses:
        specs += course_specs(Path(a.work))
    books = [read_book(p, c) for _l, c, p, _id in specs]
    print(f"{len(specs)} books, {sum(map(len, books)):,} book moves")
    H = HoldoutIndex(Path(a.explorer), books, a.threads, a.mem_gb,
                     months=a.months.split(",") if a.months else None)
    rows = []
    for (label, colour, path, cid), bk in zip(specs, books):
        r = policy_value(bk, colour, H, shrink=a.shrink)
        rows.append({"label": label, "colour": colour, "course_id": cid, "path": path,
                     "book_moves": len(bk), "shrink": a.shrink, **(r or {"value": None})})
    df = pl.DataFrame(rows, infer_schema_length=None)
    if a.out:
        df.write_parquet(a.out)
    with pl.Config(tbl_rows=60, tbl_width_chars=220, fmt_str_lengths=48):
        for colour in ("white", "black"):
            d = df.filter((pl.col("colour") == colour) & pl.col("value").is_not_null())
            if not d.height:
                continue
            print(f"\n{colour}: {d.height} books")
            print(d.sort("value", descending=True).select(
                pl.col("label").str.slice(0, 48), "book_moves",
                pl.col("value").round(4), pl.col("se").round(4),
                pl.col("moves_played").round(2), pl.col("mass_left").round(3),
                pl.col("mass_unplayed").round(3), pl.col("mass_cap").round(3)).head(40))
    print(f"done in {time.time() - t0:,.0f}s" + (f" -> {a.out}" if a.out else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
