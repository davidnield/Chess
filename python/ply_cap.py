"""Derive a lower coverage cap from a ply-keyed month (`explorer-extract month --ply-key`).

A ply-keyed month keeps `ply` in the ps key -- one row per (parent_hash,
move_san, event, elo_band, ply) -- and `end_ply` (the plies a game walked) in
the term key, so the coverage cap need not be fixed at extract time. For any
cap C up to the month's --max-ply N:

  ps    rows with ply <= C, re-aggregated on the 4-column key: counts summed,
        ply = MIN (the first ply the key is seen at), parent_epd and child_hash
        MIN. Equal to a direct --max-ply C month on every column but ply; there
        MIN over all occurrences is <= the direct month's MIN over chunk-first
        plies.

  term  ENDED    kind-0 rows with end_ply <= C, reason included, end_ply summed
                 away: exactly the direct run's ENDED rows.
        HORIZON  per position X: the games whose ply-C move reached X (ps rows
                 at ply C, by child_hash) minus the games that ENDED at X with
                 end_ply = C. A game that fails to parse after ply C walked ply
                 C, so it counts here exactly as it does in a direct run, which
                 stops at C and never sees the bad token.
                 The REASON of a horizon row is not recoverable: ps rows do not
                 carry the termination. Derived horizon rows therefore have
                 reason NULL, and a direct table is compared with its horizon
                 rows summed over reason (collapse_horizon). Nothing downstream
                 loses by it: merge_aux_stats sums horizon over reason by design.

Every function returns a SQL relation, so callers can digest, compare or COPY
it. `compare_explorer_outputs.py month --b-ply-cap C` uses reaggregate().

Usage:
    python ply_cap.py check-term <ply-keyed out> <reference term parquet> --month Y_M --cap C
        The derived cap-C term table against a direct run's (or a pilot's)
        term monthly, exact once its horizon rows are summed over reason.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import duckdb

SUMS = ("white_wins", "draws", "black_wins", "total")
TERM_OUT = ("position_hash", "kind", "reason", *SUMS)


def _p(path) -> str:
    return str(path).replace("\\", "/")


def reaggregate(rel: str, cap: int) -> str:
    """Ply-keyed ps rows at ply <= cap, on the 4-column key, in PS_COLS order."""
    return f"""(
        SELECT parent_hash, move_san, event, elo_band,
               MIN(parent_epd) AS parent_epd, MIN(child_hash) AS child_hash,
               MIN(child_eval) AS child_eval, MIN(ply) AS ply,
               SUM(white_wins)::BIGINT AS white_wins, SUM(draws)::BIGINT AS draws,
               SUM(black_wins)::BIGINT AS black_wins, SUM(total)::BIGINT AS total
        FROM {rel} WHERE ply <= {int(cap)}
        GROUP BY parent_hash, move_san, event, elo_band)"""


def derived_term(ps_rel: str, term_rel: str, cap: int) -> str:
    """The cap-C term table: ENDED exact, HORIZON per position (reason NULL)."""
    c = int(cap)
    s = ", ".join(f"SUM({x})::BIGINT AS {x}" for x in SUMS)
    diff = ", ".join(f"(r.{x} - COALESCE(e.{x}, 0))::BIGINT AS {x}" for x in SUMS)
    return f"""(
        SELECT position_hash, 0::INTEGER AS kind, reason, {s}
        FROM {term_rel} WHERE kind = 0 AND end_ply <= {c}
        GROUP BY position_hash, reason
        UNION ALL
        SELECT r.position_hash, 1::INTEGER AS kind, CAST(NULL AS INTEGER) AS reason, {diff}
        FROM (SELECT child_hash AS position_hash, {s} FROM {ps_rel}
              WHERE ply = {c} GROUP BY child_hash) r
        LEFT JOIN (SELECT position_hash, {s} FROM {term_rel}
                   WHERE kind = 0 AND end_ply = {c} GROUP BY position_hash) e
          USING (position_hash)
        WHERE r.total - COALESCE(e.total, 0) > 0)"""


def collapse_horizon(term_rel: str) -> str:
    """A direct run's term table as a derivation can know it: ENDED rows as they
    are, HORIZON rows summed over reason with reason NULL."""
    sums = ", ".join(f"SUM({x})::BIGINT" for x in SUMS)
    return f"""(
        SELECT position_hash, kind, reason, {", ".join(SUMS)}
        FROM {term_rel} WHERE kind = 0
        UNION ALL
        SELECT position_hash, 1::INTEGER, CAST(NULL AS INTEGER), {sums}
        FROM {term_rel} WHERE kind = 1 GROUP BY position_hash)"""


def ply_keyed_rels(out: Path, tag: str) -> tuple[str, str]:
    """(ps, term) relations of a ply-keyed month output dir."""
    y, m = (int(x) for x in tag.split("_"))
    ps = f"read_parquet('{_p(out)}/month={y}_{m}/bkt=*/*.parquet', hive_partitioning=false)"
    term = f"read_parquet('{_p(out)}/_term/year={y}_month={m}.term.parquet')"
    return ps, term


def check_term(con, out: Path, ref: Path, tag: str, cap: int) -> tuple[bool, str]:
    ps, term = ply_keyed_rels(out, tag)
    d = derived_term(ps, term, cap)
    r = collapse_horizon(f"read_parquet('{_p(ref)}')")
    cols = ", ".join(TERM_OUT)
    lines = []
    dig = {}
    for name, rel in (("derived", d), ("reference", r)):
        n, h, t, hz = con.execute(
            f"SELECT COUNT(*), COALESCE(SUM(hash({cols}))::HUGEINT, 0), SUM(total), "
            f"SUM(total) FILTER (WHERE kind = 1) FROM {rel}").fetchone()
        dig[name] = (int(n), int(h))
        lines.append(f"  {name:<9} {int(n):>14,} rows, total {int(t or 0):>16,}, "
                     f"horizon {int(hz or 0):>14,}")
    ok = dig["derived"] == dig["reference"]
    if not ok:
        for a, b, label in ((d, r, "derived only"), (r, d, "reference only")):
            rows = con.execute(f"SELECT {cols} FROM {a} EXCEPT ALL SELECT {cols} FROM {b} "
                               f"LIMIT 10").fetchall()
            lines.append(f"  {label}: {rows}")
    return ok, "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("mode", choices=["check-term"])
    ap.add_argument("plykeyed", type=Path)
    ap.add_argument("reference", type=Path)
    ap.add_argument("--month", required=True, metavar="Y_M")
    ap.add_argument("--cap", type=int, required=True)
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--mem", default="4GB")
    ap.add_argument("--tmp-dir", type=Path, default=None)
    a = ap.parse_args()
    con = duckdb.connect()
    con.execute(f"SET threads={a.threads}")
    con.execute(f"SET memory_limit='{a.mem}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET enable_progress_bar=false")
    if a.tmp_dir:
        a.tmp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{_p(a.tmp_dir)}'")
    t0 = time.time()
    ok, log = check_term(con, a.plykeyed, a.reference, a.month, a.cap)
    print(f"cap {a.cap}: derived term table vs {a.reference.name} (horizon summed over reason)")
    print(log)
    print(f"\n{'IDENTICAL' if ok else 'MISMATCH'} ({time.time() - t0:,.0f}s)")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
