"""The extract's mass accounting closes exactly: in == out + term + horizon.

Stage 3's opponent-node mean divides by a node's OUTGOING edge mass. Every game
that arrives at a node either leaves by a recorded edge, ends there, or runs past
the extraction horizon — so if those three do not add back up to the arriving
mass, some games are silently unaccounted for and the mean is computed over the
wrong denominator. That is the defect this whole refactor exists to fix, so the
identity is the gate on it, not a nice-to-have.

Per position p (measured on real games, not fixtures):

    in_mass(p)  ==  out_mass(p) + term(p) + horizon(p)

with the start position special-cased: nothing flows INTO it, so the arriving
mass is the number of kept games instead.

    kept  ==  out_mass(start) + term(start) + horizon(start)

TERM and HORIZON are deliberately separate. A game that ended is a fact whose
score is known; a game cut off at ply 30 is still running and must not be valued
as though someone resigned. Collapsing them is exactly the conflation
--reply-shrink's coverage c currently suffers from.

Usage:  python _test_extract_accounting.py [--games N] [--file PATH]
"""
from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from collections import Counter
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from build_pooled_stats import (SOURCE_ROOT, TERM_ABANDON, TERM_ENDED, TERM_FLAG,
                                TERM_HORIZON, TERM_NORMAL, TERM_OTHER,
                                _START_HASH, _term_reason, extract_file)

NAMES = {TERM_NORMAL: "Normal", TERM_FLAG: "Time forfeit",
         TERM_ABANDON: "Abandoned", TERM_OTHER: "other"}

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def find_source() -> Path | None:
    for year in sorted(SOURCE_ROOT.glob("year=*"), reverse=True):
        for mon in sorted(year.glob("month=*")):
            for ev in sorted(mon.glob("event=*")):
                fs = sorted(ev.glob("*.parquet"))
                if fs:
                    return fs[0]
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--games", type=int, default=20000)
    ap.add_argument("--file", default=None)
    args = ap.parse_args()

    src = Path(args.file) if args.file else find_source()
    if src is None or not src.exists():
        print(f"  SKIP: no source parquet under {SOURCE_ROOT}")
        print("\nALL PASS (0 checks — source data unavailable)")
        sys.exit(0)
    print(f"Source: {src}")
    print(f"Games:  {args.games:,}\n")

    tmp = Path(tempfile.mkdtemp(prefix="extract_acct_"))
    try:
        ps_p, cr_p, tm_p = (tmp / "a.ps.parquet", tmp / "a.crush.parquet",
                            tmp / "a.term.parquet")
        r = extract_file(src, ps_p, cr_p, min_elo=1800, max_ply=30, tiers=None,
                         limit_games=args.games, term_out=tm_p)
        print(f"  kept {r['kept']:,}/{r['games']:,}  ps_rows {r['ps_rows']:,}  "
              f"term_rows {r['term_rows']:,}  failed {r['failed']}\n")

        ps = pl.read_parquet(ps_p)
        tm = pl.read_parquet(tm_p)

        # A SAN parse failure records the plies before it but no terminal row —
        # we genuinely do not know where that game went. It would show up as an
        # unexplained deficit below, so require none in the sample.
        check(r["failed"] == 0,
              f"no SAN parse failures in the sample ({r['failed']})")

        out = (ps.group_by("parent_hash")
                 .agg(pl.col("total").sum().alias("out_mass"))
                 .rename({"parent_hash": "p"}))
        inn = (ps.group_by("child_hash")
                 .agg(pl.col("total").sum().alias("in_mass"))
                 .rename({"child_hash": "p"}))
        term = (tm.filter(pl.col("kind") == TERM_ENDED)
                  .group_by("position_hash").agg(pl.col("total").sum().alias("term"))
                  .rename({"position_hash": "p"}))
        hor = (tm.filter(pl.col("kind") == TERM_HORIZON)
                 .group_by("position_hash").agg(pl.col("total").sum().alias("horizon"))
                 .rename({"position_hash": "p"}))

        bal = (out.join(inn, on="p", how="full", coalesce=True)
                  .join(term, on="p", how="full", coalesce=True)
                  .join(hor, on="p", how="full", coalesce=True)
                  .fill_null(0))
        # Nothing flows into the start position; the kept games are its arrivals.
        bal = bal.with_columns(
            pl.when(pl.col("p") == _START_HASH)
              .then(pl.lit(r["kept"], dtype=pl.Int64))
              .otherwise(pl.col("in_mass")).alias("in_mass"))
        bal = bal.with_columns(
            (pl.col("in_mass") - pl.col("out_mass")
             - pl.col("term") - pl.col("horizon")).alias("resid"))

        n_pos = bal.height
        viol = bal.filter(pl.col("resid") != 0)
        check(viol.height == 0,
              f"in == out + term + horizon at every position "
              f"({n_pos:,} positions, {viol.height:,} violations)")

        # SCORE conservation — the check that mass conservation cannot make.
        # Every arriving game leaves exactly once, so its RESULT must appear
        # exactly once on the far side too. Mass alone would pass even if the
        # terminal rows carried the wrong results, or the right results attached
        # to the wrong positions; this pins the values, which is the whole claim
        # (terminations score 0.90 for White where continuing games score 0.51).
        # Exact arithmetic: wins are integers, draws contribute halves.
        def _score(df, key, alias):
            return (df.group_by(key)
                      .agg((pl.col("white_wins") + 0.5 * pl.col("draws")).sum()
                           .alias(alias))
                      .rename({key: "p"}))

        s_out = _score(ps, "parent_hash", "s_out")
        s_in = _score(ps.rename({"child_hash": "_c"}), "_c", "s_in")
        s_term = _score(tm.filter(pl.col("kind") == TERM_ENDED),
                        "position_hash", "s_term")
        s_hor = _score(tm.filter(pl.col("kind") == TERM_HORIZON),
                       "position_hash", "s_hor")
        sb = (s_out.join(s_in, on="p", how="full", coalesce=True)
                   .join(s_term, on="p", how="full", coalesce=True)
                   .join(s_hor, on="p", how="full", coalesce=True)
                   .fill_null(0.0))
        # Nothing flows into the start position; the kept games' own results are
        # what arrive there.
        kept_score = float((ps.filter(pl.col("parent_hash") == _START_HASH)["white_wins"]
                            + 0.5 * ps.filter(pl.col("parent_hash") == _START_HASH)["draws"]
                            ).sum())
        kept_score += float((tm.filter(pl.col("position_hash") == _START_HASH)["white_wins"]
                             + 0.5 * tm.filter(pl.col("position_hash") == _START_HASH)["draws"]
                             ).sum())
        sb = sb.with_columns(
            pl.when(pl.col("p") == _START_HASH).then(pl.lit(kept_score))
              .otherwise(pl.col("s_in")).alias("s_in"))
        sb = sb.with_columns(
            (pl.col("s_in") - pl.col("s_out") - pl.col("s_term")
             - pl.col("s_hor")).abs().alias("sres"))
        sviol = sb.filter(pl.col("sres") > 1e-9)
        check(sviol.height == 0,
              f"SCORE conserves at every position: in == out + term + horizon "
              f"({sb.height:,} positions, {sviol.height:,} violations)")
        if sviol.height:
            for row in (sviol.sort("sres", descending=True).head(5)
                            .iter_rows(named=True)):
                print(f"      p={row['p']}  in={row['s_in']} out={row['s_out']} "
                      f"term={row['s_term']} hor={row['s_hor']} "
                      f"resid={row['sres']}")
        if viol.height:
            print("    worst residuals:")
            for row in (viol.with_columns(pl.col("resid").abs().alias("a"))
                             .sort("a", descending=True).head(5).iter_rows(named=True)):
                print(f"      p={row['p']}  in={row['in_mass']} out={row['out_mass']} "
                      f"term={row['term']} horizon={row['horizon']} resid={row['resid']}")

        # Whole-population sanity: every kept game contributes exactly one terminal
        # row, so the two buckets must partition the games.
        tot_term = int(tm.filter(pl.col("kind") == TERM_ENDED)["total"].sum())
        tot_hor = int(tm.filter(pl.col("kind") == TERM_HORIZON)["total"].sum())
        check(tot_term + tot_hor == r["kept"],
              f"every kept game has exactly one terminal row: "
              f"{tot_term:,} ended + {tot_hor:,} horizon == {r['kept']:,} kept")

        check(int(tm["total"].sum()) == r["kept"], "term partial totals == kept games")

        # The reason dimension must round-trip against the source Termination
        # column. A mis-mapped code would silently mislabel the 13.6% of
        # terminations that are time forfeits — and since that whole dimension
        # exists to keep the clock separable from the board, a wrong mapping
        # would be worse than not carrying it.
        import pyarrow.parquet as pq
        want = Counter()
        n = 0
        for b in pq.ParquetFile(src).iter_batches(
                batch_size=50_000,
                columns=["termination", "mean_elo", "white_score"]):
            for rec in b.to_pylist():
                if n >= args.games:
                    break
                n += 1
                if rec["mean_elo"] is None or rec["mean_elo"] < 1800:
                    continue
                if rec["white_score"] is None:
                    continue
                want[_term_reason(rec["termination"])] += 1
            if n >= args.games:
                break
        got = {int(k): int(v) for k, v in
               tm.group_by("reason").agg(pl.col("total").sum())
                 .iter_rows()}
        check(got == dict(want),
              f"reason codes round-trip against source Termination "
              f"({ {NAMES.get(k, k): v for k, v in sorted(got.items())} })")

        print("\n  info  TERM by reason (share of games that ended in-tree):")
        ended = tm.filter(pl.col("kind") == TERM_ENDED)
        tot_e = max(int(ended["total"].sum()), 1)
        for code, cnt, w, d in sorted(
                ended.group_by("reason")
                     .agg(pl.col("total").sum().alias("t"),
                          pl.col("white_wins").sum().alias("w"),
                          pl.col("draws").sum().alias("d"))
                     .iter_rows()):
            print(f"          {NAMES.get(code, code):<14} {cnt:>7,}  "
                  f"{100*cnt/tot_e:>5.1f}%   white score "
                  f"{(w + 0.5*d)/max(cnt,1):.4f}")

        share = 100.0 * tot_term / max(r["kept"], 1)
        print(f"\n  info  ended within the recorded tree: {share:.1f}% of kept games "
              f"(the rest ran past the ply cap)")
        # These are the games Stage 3 currently drops from every opponent-node mean.
        reached = int(bal["in_mass"].sum())
        if reached:
            print(f"  info  TERM mass as a share of all arrivals: "
                  f"{100.0 * tot_term / reached:.2f}%")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
