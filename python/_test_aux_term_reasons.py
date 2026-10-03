"""Excluding time-forfeit flags must fail loudly on a pooled-reason aux sidecar.

pool_from_book derives terminations from the move flows, which carry no
Termination header, so the whole ended mass lands in term_other_* and its aux
meta says term_reasons='pooled'. On such a sidecar Stage 3's --no-aux-term-flags
and budget_core's term_flags=False would drop term_flag_* — all zeros — and the
time forfeits they were meant to remove would stay in. Silent no-op; so both
consumers raise instead.

  A  no meta / split meta: flags may be excluded (the old sidecars keep working)
  B  pooled meta: check_aux_term_flags raises for term_flags=False only
  C  an unreadable meta raises rather than guessing 'split'
  D  budget_core.load_aux_rows raises up front, and stamps rows it returns
  E  budget_core.aux_row_buckets raises on a stamped pooled row with
     term_flags=False, and is unchanged with term_flags=True
  F  the Stage 3 CLI exits non-zero with --no-aux-term-flags on a pooled sidecar

Run: .venv/Scripts/python.exe python/_test_aux_term_reasons.py
"""
from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import chess
import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
import budget_core as bc  # noqa: E402
from stage3_backwards_induction import (aux_term_reasons,  # noqa: E402
                                        check_aux_term_flags, zobrist_int64)

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def raises(fn, *args, **kw) -> str | None:
    try:
        fn(*args, **kw)
    except ValueError as e:
        return str(e)
    return None


GROUPS = ("term_normal", "term_flag", "term_other", "horizon")


def aux_frame(h: int) -> pl.DataFrame:
    row = {"position_hash": h}
    for g in GROUPS:
        for c in ("total", "white_wins", "draws", "black_wins"):
            row[f"{g}_{c}"] = 0
    row.update(term_other_total=10, term_other_white_wins=6, term_other_draws=2,
               term_other_black_wins=2, other_total=5, other_white_wins=2,
               other_draws=1, other_black_wins=2, other_edges=3,
               other_eval_mean=0.55, other_eval_min=0.4, other_eval_max=0.7,
               other_eval_cov=0.8)
    return pl.DataFrame([row]).with_columns(pl.col("other_edges").cast(pl.Int32))


def main() -> int:
    root = zobrist_int64(chess.Board())
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        aux = td / "aux.parquet"
        aux_frame(root).write_parquet(aux)
        meta = Path(str(aux) + ".meta.json")

        print("A  no meta / split meta")
        check(aux_term_reasons(aux) == "split", "no meta reads as 'split'")
        check(raises(check_aux_term_flags, aux, False) is None,
              "no meta: excluding flags is allowed")
        meta.write_text(json.dumps({"term_reasons": "split"}), encoding="utf-8")
        check(raises(check_aux_term_flags, aux, False) is None,
              "split meta: excluding flags is allowed")

        print("B  pooled meta")
        meta.write_text(json.dumps({"term_reasons": "pooled", "producer": "pool_from_book"}),
                        encoding="utf-8")
        msg = raises(check_aux_term_flags, aux, False)
        check(msg is not None and "pooled" in msg, "term_flags=False raises, naming 'pooled'")
        check(raises(check_aux_term_flags, aux, True) is None, "term_flags=True passes")

        print("C  unreadable meta")
        meta.write_text("{not json", encoding="utf-8")
        check(raises(aux_term_reasons, aux) is not None, "corrupt meta raises")
        meta.write_text(json.dumps({"term_reasons": "pooled"}), encoding="utf-8")

        print("D  budget_core.load_aux_rows")
        check(raises(bc.load_aux_rows, aux, None, False) is not None,
              "load_aux_rows(term_flags=False) raises on pooled")
        rows = bc.load_aux_rows(aux, [root])
        check(set(rows) == {root} and rows[root][bc.TERM_REASONS_KEY] == "pooled",
              "rows are stamped with term_reasons='pooled'")

        print("E  budget_core.aux_row_buckets")
        r = rows[root]
        check(raises(bc.aux_row_buckets, r, True, False) is not None,
              "term_flags=False on a pooled row raises")
        plain = {k: v for k, v in r.items() if k != bc.TERM_REASONS_KEY}
        check(bc.aux_row_buckets(r, True, True) == bc.aux_row_buckets(plain, True, True),
              "term_flags=True: the stamp changes nothing")
        check(raises(bc.aux_row_buckets, plain, True, False) is None,
              "an unstamped row (old loaders) still accepts term_flags=False")

        print("F  Stage 3 CLI")
        pool = td / "pool.parquet"
        b = chess.Board()
        c = b.copy()
        c.push_san("e4")
        pl.DataFrame([{"parent_hash": zobrist_int64(b), "move_san": "e4",
                       "parent_epd": b.epd(), "child_hash": zobrist_int64(c), "ply": 1,
                       "white_wins": 60, "draws": 10, "black_wins": 30, "total": 100,
                       "event": "Pooled", "elo_band": 0, "white_score_avg": 0.65}]
                     ).with_columns(pl.col("ply").cast(pl.Int32)).write_parquet(pool)
        stage3 = Path(__file__).parent / "stage3_backwards_induction.py"
        base = [sys.executable, str(stage3), "--input", str(pool), "--output",
                str(td / "rep.parquet"), "--aux-stats", str(aux),
                "--perspective", "white"]
        p = subprocess.run(base + ["--no-aux-term-flags"], capture_output=True, text=True,
                           encoding="utf-8", errors="replace")
        check(p.returncode != 0 and "pooled" in (p.stderr + p.stdout),
              f"--no-aux-term-flags exits non-zero naming 'pooled' (rc={p.returncode})")
        p = subprocess.run(base, capture_output=True, text=True, encoding="utf-8",
                           errors="replace")
        check("term_reasons='pooled'" not in (p.stderr + p.stdout),
              f"default flags do not trip the guard (rc={p.returncode})")

    failed = [label for ok, label in _checks if not ok]
    print(f"\n{len(_checks) - len(failed)}/{len(_checks)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
