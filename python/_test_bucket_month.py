"""bucket_month writes exactly what backfill_epd writes, from an EPD-complete month.

B1 extracts every EPD itself, so its consolidated month skips the replay and only
needs bucketing. The A2 merge must read that month without knowing which tool
made it, so the bar is identity with the backfill, not "looks the same":

  * the _test_backfill_epd GAMES fixture, built EPD-complete (CUTOFF >= 30) and
    bucketed here, equals the backfill's output on the same games built with
    CUTOFF=4: the same bucket dirs, the same rows in each bucket (every column,
    parent_epd and ply included), the same bucket-file schema (plain string),
    and the same 26-field manifest schema in the same order and types;
  * the manifest's shared fields agree, its replay and quarantine fields are
    0 / 0.0 / "{}", the sentinel holds the manifest, and the work dir is gone;
  * a month with any NULL parent_epd is refused with the count, and nothing is
    promoted;
  * a planted parity violation is fatal (no replay means no collision can
    explain it), and nothing is promoted;
  * a planted second EPD for one hash is reported in _conflicts and is NOT fatal;
  * a finished month is skipped on a re-run.

Run: .venv/Scripts/python.exe python/_test_bucket_month.py
"""
from __future__ import annotations

import contextlib
import io
import json
import shutil
import sys
import tempfile
from pathlib import Path

import chess
import polars as pl
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))
import _test_backfill_epd as tb
import bucket_month as bm

YEAR, MONTH, BUCKETS = tb.YEAR, tb.MONTH, tb.BUCKETS
TAG = f"{YEAR}_{MONTH}"
KEY = tb.KEY
SUMS = ("total", "white_wins", "draws", "black_wins")

_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> bool:
    _checks.append((bool(ok), label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")
    return bool(ok)


def run_bm(monthly: Path, out: Path, work: Path):
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            man = bm.bucket_month(monthly, YEAR, MONTH, out, work, BUCKETS,
                                  tb.WORKERS, tb.THREADS, tb.MEM, work / "_duck")
        return man, buf.getvalue(), None
    except Exception as exc:                                       # noqa: BLE001
        return None, buf.getvalue(), exc


def rows_of(cutoff: int) -> list[dict]:
    """The GAMES fixture with parent_epd kept through `cutoff`."""
    saved = tb.CUTOFF
    tb.CUTOFF = cutoff
    try:
        rows, _ = tb.build_rows(tb.GAMES)
    finally:
        tb.CUTOFF = saved
    return rows


def buckets_of(out: Path) -> dict[str, pl.DataFrame]:
    return {d.name: pl.concat([pl.read_parquet(f) for f in sorted(d.glob("*.parquet"))])
            .sort(KEY) for d in sorted((out / f"month={TAG}").glob("bkt=*"))}


def nothing_promoted(out: Path) -> bool:
    return (not (out / f"month={TAG}").exists()
            and not (out / f"_month={TAG}.DONE").exists()
            and not (out / "_manifest" / f"month={TAG}.parquet").exists())


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="test_bucket_month_"))
    try:
        full = tb.write_month(tmp / "m_full", rows_of(30))
        cut = tb.write_month(tmp / "m_cut", rows_of(4))
        check(pl.read_parquet(full)["parent_epd"].null_count() == 0
              and pl.read_parquet(cut)["parent_epd"].null_count() > 0,
              "the fixture: CUTOFF=30 is EPD-complete, CUTOFF=4 is not")

        # ── identity with the backfill ────────────────────────────────────────
        print("\nbucket_month vs backfill_epd on the same games")
        out_a, work_a = tmp / "o_bm", tmp / "w_bm"
        man_a, log_a, err_a = run_bm(full, out_a, work_a)
        check(err_a is None, f"bucket_month completes ({err_a})")
        out_b, work_b = tmp / "o_bf", tmp / "w_bf"
        man_b, _, err_b = tb.run(cut, out_b, work_b)
        check(err_b is None, f"backfill_epd completes ({err_b})")
        a, b = buckets_of(out_a), buckets_of(out_b)
        check(a and list(a) == list(b),
              f"the same {len(a)} bucket dirs: {sorted(a)}")
        check(all(a[k].equals(b[k]) for k in a),
              "every bucket holds the same rows, on every column (parent_epd and "
              "ply included)")
        fa = next((out_a / f"month={TAG}").glob("bkt=*/*.parquet"))
        fb = next((out_b / f"month={TAG}").glob("bkt=*/*.parquet"))
        sa, sb = pq.read_schema(fa), pq.read_schema(fb)
        check(sa.equals(sb) and list(sa.names) == list(bf_cols())
              and str(sa.field("move_san").type) == "string",
              f"the same bucket-file schema, PS_COLS order, plain string: {sa.names}")
        names_a = sorted(p.name for p in (out_a / f"month={TAG}").rglob("*"))
        names_b = sorted(p.name for p in (out_b / f"month={TAG}").rglob("*"))
        check(names_a == names_b, "the same file names inside the month dir")

        ma = pq.read_schema(out_a / "_manifest" / f"month={TAG}.parquet")
        mb = pq.read_schema(out_b / "_manifest" / f"month={TAG}.parquet")
        want = [(n, t) for n, t in bm.MANIFEST_FIELDS]
        got_a = [(f.name, str(f.type)) for f in ma]
        got_b = [(f.name, str(f.type)) for f in mb]
        check(len(want) == 26 and got_a == want and got_b == want,
              "both manifests have the same 26 fields, in order, with the same types")
        ra = pq.read_table(out_a / "_manifest" / f"month={TAG}.parquet").to_pylist()[0]
        rb = pq.read_table(out_b / "_manifest" / f"month={TAG}.parquet").to_pylist()[0]
        shared = ("year", "month", "files", "bytes", "rows", "ply1_games", "total",
                  "white_wins", "draws", "black_wins", "buckets")
        check(all(ra[k] == rb[k] for k in shared if k != "bytes"),
              f"and agree on {', '.join(k for k in shared if k != 'bytes')}")
        zero = ("edges_replayed", "positions_resolved", "mismatches", "unresolved",
                "quarantine_edges", "quarantine_rows", "quarantine_total",
                "quarantine_white_wins", "quarantine_draws", "quarantine_black_wins",
                "unreachable_positions")
        check(all(ra[k] == 0 for k in zero) and ra["replays_per_sec"] == 0.0
              and ra["quarantine_by_reason"] == "{}" and ra["conflicts"] == 0,
              "bucket_month's replay and quarantine fields are 0 / 0.0 / {}")
        sent = out_a / f"_month={TAG}.DONE"
        check(sent.exists() and json.loads(sent.read_text()) == man_a == ra,
              "the sentinel is the manifest as JSON, written for the month")
        check(not (work_a / f"month={TAG}").exists()
              and not (out_a / f"_tmp_month={TAG}").exists()
              and not (out_a / "_quarantine").exists()
              and not (out_a / "_conflicts").exists(),
              "no work dir, no _tmp_month, no quarantine and no conflict report left")

        # ── the gates ─────────────────────────────────────────────────────────
        print("\nthe gates")
        out_n, work_n = tmp / "o_null", tmp / "w_null"
        _, _, err_n = run_bm(cut, out_n, work_n)
        n_null = pl.read_parquet(cut)["parent_epd"].null_count()
        check(err_n is not None and f"{n_null:,} input rows have a NULL parent_epd"
              in str(err_n) and nothing_promoted(out_n),
              f"a month with {n_null} NULL EPDs is refused with the count, "
              f"nothing promoted")

        df = pl.read_parquet(full)
        flip = df.with_row_index().with_columns(
            pl.when(pl.col("index") == 0).then(pl.col("ply") + 1)
            .otherwise(pl.col("ply")).alias("ply")).drop("index")
        d_p = tmp / "m_parity"
        d_p.mkdir()
        p_p = d_p / full.name
        flip.write_parquet(p_p, compression="zstd")
        out_p, work_p = tmp / "o_parity", tmp / "w_parity"
        _, _, err_p = run_bm(p_p, out_p, work_p)
        check(err_p is not None and "parity" in str(err_p) and nothing_promoted(out_p),
              f"a planted parity violation is fatal, nothing promoted ({err_p})")

        start = chess.Board()
        other = chess.Board()
        for san in "e4 e5".split():
            other.push(other.parse_san(san))
        h0 = tb.zobrist_int64(start)
        first = df.with_row_index().filter(pl.col("parent_hash") == h0)["index"][0]
        twin = df.with_row_index().with_columns(
            pl.when(pl.col("index") == first).then(pl.lit(other.epd()))
            .otherwise(pl.col("parent_epd")).alias("parent_epd")).drop("index")
        check(df.filter(pl.col("parent_hash") == h0).height >= 2,
              "the twin fixture: the start position has more than one row")
        d_t = tmp / "m_twin"
        d_t.mkdir()
        p_t = d_t / full.name
        twin.write_parquet(p_t, compression="zstd")
        out_t, work_t = tmp / "o_twin", tmp / "w_twin"
        man_t, _, err_t = run_bm(p_t, out_t, work_t)
        rep = out_t / "_conflicts" / f"month={TAG}" / bm.CONFLICT_FILE
        conf = pl.read_parquet(rep) if rep.exists() else None
        check(err_t is None and man_t and man_t["conflicts"] == 1,
              f"a second EPD for one hash is NOT fatal ({err_t})")
        check(conf is not None and conf.height == 1 and conf["hash"][0] == h0
              and conf["kind"][0] == bm.CONFLICT_KIND
              and sorted([conf["epd_a"][0], conf["epd_b"][0]])
              == sorted([start.epd(), other.epd()]),
              "and it is reported: the hash, both EPDs, kind 'parent-epd'")
        check(pq.read_schema(rep).equals(tb.bf._CONFLICT_SCHEMA) if conf is not None
              else False, "in the backfill's conflict schema")

        # ── resume ────────────────────────────────────────────────────────────
        man_r, log_r, err_r = run_bm(full, out_a, work_a)
        check(err_r is None and man_r == {} and "already done" in log_r,
              "a finished month is skipped on a re-run")

        # ── compare_explorer_outputs month, on outputs whose truth is known ──
        print("\ncompare_explorer_outputs month")
        rc, log = compare("month", out_b, out_a, "--ply-le")
        check(rc == 0 and "IDENTICAL" in log,
              f"backfill vs bucket_month: identical, ply parity and B<=A hold (rc {rc})")
        rc, log = compare("month", out_a, out_t)
        check(rc == 2 and "0 bug, 2 collision, 0 quarantine" in log,
              f"a second EPD for one hash: its row on each side is a collision, "
              f"exit 2 (rc {rc})")
        bug = tmp / "o_bug"
        shutil.copytree(out_a, bug)
        f0 = sorted((bug / f"month={TAG}").glob("bkt=*/*.parquet"))[0]
        d0 = pl.read_parquet(f0)
        d0.with_columns((pl.col("total") + pl.when(pl.int_range(pl.len()) == 0)
                         .then(1).otherwise(0)).alias("total")).write_parquet(f0)
        rc, log = compare("month", out_a, bug)
        check(rc == 1 and "bug" in log, f"a changed count is a bug, exit 1 (rc {rc})")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"\n{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


def bf_cols() -> tuple[str, ...]:
    return tb.bf.PS_COLS


def compare(mode: str, a: Path, b: Path, *extra: str) -> tuple[int, str]:
    """compare_explorer_outputs as a subprocess: (exit code, its output)."""
    import subprocess
    p = subprocess.run([sys.executable, str(Path(__file__).parent /
                                            "compare_explorer_outputs.py"),
                        mode, str(a), str(b), "--month", TAG, "--nbuckets", str(BUCKETS),
                        "--tmp-dir", str(a.parent / "_cmp_tmp"), *extra],
                       capture_output=True, text=True, timeout=600)
    return p.returncode, p.stdout + p.stderr


if __name__ == "__main__":
    main()
