"""Synthetic test for replay_holdout's IN-SAMPLE GUARD (2026-10-08) and the pool
meta build_pooled_stats now writes for it. Imports the production helpers.

The guard follows each book's provenance -- <book>.meta.json "input"/"stats" (a
pool) or "source_rep" (budget books) -- to <pool>.meta.json population.months, and
refuses replay months inside that range. A pool without a meta falls back to its
filename's _YYYY_YYYY_ tag as whole years.

Cases:
  A  pool meta 2013_1..2026_7: 2025-03 in sample; 2026-08 and 2026-09 not.
  B  no pool meta, dated name ..._2013_2024_...: 2024-12 in, 2025-01 out.
  C  budget book -> source_rep -> rep -> pool: resolves through the chain.
  D  book with no meta: range unknown (the CLI then refuses).
  E  build_pooled_stats.write_pool_meta from a _monthly dir round-trips into
     pool_month_range (and records the full month list).
  F  CLI: a book built on a pool covering 2025 is REFUSED (rc 2) for a 2025
     replay before any game is read; --allow-in-sample overrides. Skipped if the
     canonical source tree is absent.
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import polars as pl

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
from replay_holdout import (book_training_range, in_sample_months,  # noqa: E402
                            pool_month_range)
from build_pooled_stats import SOURCE_ROOT, write_pool_meta  # noqa: E402

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def meta(path: Path, d: dict) -> None:
    path.with_name(path.name + ".meta.json").write_text(json.dumps(d), encoding="utf-8")


def files(*ym):
    return [(f"src_{y}_{m}", y, m, "Blitz") for y, m in ym]


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        t = Path(td)

        # A
        pool_a = t / "position_stats_pooled_canon.parquet"
        meta(pool_a, {"population": {"months": ["2013_1", "2026_7"]}})
        rep_a = t / "rep_a.parquet"
        meta(rep_a, {"input": str(pool_a)})
        rng = book_training_range(rep_a)
        check(rng == ((2013, 1), (2026, 7)), f"A: range from pool meta ({rng})")
        check(in_sample_months(files((2025, 3)), rng) == [(2025, 3)],
              "A: 2025-03 is in sample")
        check(in_sample_months(files((2026, 8), (2026, 9)), rng) == [],
              "A: 2026-08 and 2026-09 are out of sample")
        check(in_sample_months(files((2026, 7)), rng) == [(2026, 7)],
              "A: the last month itself is in sample (inclusive)")

        # B
        pool_b = t / "position_stats_pooled_ge1800_2013_2024_brc.parquet"
        rng_b = pool_month_range(pool_b)
        check(rng_b == ((2013, 1), (2024, 12)), f"B: dated-name fallback ({rng_b})")
        check(in_sample_months(files((2024, 12), (2025, 1)), rng_b) == [(2024, 12)],
              "B: 2024-12 in, 2025-01 out")

        # C
        rep_c = t / "c0_white.parquet"
        meta(rep_c, {"input": str(pool_b)})
        book_c = t / "dp_white_b400.parquet"
        meta(book_c, {"source_rep": str(rep_c), "method": "dp"})
        check(book_training_range(book_c) == ((2013, 1), (2024, 12)),
              "C: budget book resolves through source_rep")
        book_c2 = t / "trunc_white_b400.parquet"
        meta(book_c2, {"stats": str(pool_a)})
        check(book_training_range(book_c2) == ((2013, 1), (2026, 7)),
              "C: a 'stats' key resolves too")

        # D
        check(book_training_range(t / "orphan.parquet") is None,
              "D: no meta -> unknown")

        # E
        part = t / "_pooled_partials_x" / "_monthly"
        part.mkdir(parents=True)
        for y, m in ((2013, 1), (2013, 10), (2026, 7), (2020, 2)):
            (part / f"year={y}_month={m}.ps.parquet").write_bytes(b"")
            (part / f"year={y}_month={m}.term.parquet").write_bytes(b"")
        ps_out = t / "position_stats_pooled_x.parquet"
        aux_out = t / "position_stats_aux_pooled_x.parquet"
        ps_out.write_bytes(b""); aux_out.write_bytes(b"")
        args = SimpleNamespace(events=["Blitz", "Rapid", "Classical"], min_elo=1800,
                               min_games=50)
        write_pool_meta(ps_out, aux_out, part, args)
        check(pool_month_range(ps_out) == ((2013, 1), (2026, 7)),
              f"E: written meta round-trips ({pool_month_range(ps_out)})")
        m = json.loads(Path(str(ps_out) + ".meta.json").read_text(encoding="utf-8"))
        check(m["population"]["n_months"] == 4
              and m["population"]["month_list"][1] == "2013_10",
              "E: month list sorted numerically (2013_10 after 2013_1, before 2020_2)")
        check(Path(str(aux_out) + ".meta.json").exists(), "E: aux sidecar meta written")

        # F
        if not Path(SOURCE_ROOT).exists():
            print("  SKIP  F: canonical source tree absent")
        else:
            rep_f = t / "rep_f.parquet"
            pl.DataFrame({"position_hash": [1], "side_to_move": ["white"],
                          "value": [0.5], "best_move": ["e4"]}).write_parquet(rep_f)
            meta(rep_f, {"input": str(pool_a)})
            cmd = [sys.executable, str(HERE / "replay_holdout.py"), "--repertoire",
                   str(rep_f), "--perspective", "white", "--start-year", "2025",
                   "--end-year", "2025", "--months", "1", "--limit-games", "1"]
            r = subprocess.run(cmd, capture_output=True, text=True)
            check(r.returncode == 2 and "in-sample" in r.stdout,
                  f"F: in-sample replay refused (rc {r.returncode})")
            r2 = subprocess.run(cmd + ["--allow-in-sample"], capture_output=True,
                                text=True)
            check(r2.returncode == 0 and "WARNING (--allow-in-sample)" in r2.stdout,
                  f"F: --allow-in-sample proceeds with a warning (rc {r2.returncode})")

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
