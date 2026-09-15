"""Sub-bucketed monthly consolidation produces exactly the single-query monthly.

consolidate_monthly(sub_buckets=K) splits each month's GROUP BY into K disjoint
key-hash slices and concatenates the parts, because a banded explorer month
(43-47 GB of ps partials) no longer fits one GROUP BY. Every way to get this
wrong is quiet: a slice predicate on a column outside the key, a part counted
twice after a resume, or a concatenation that drops a part all leave a
well-formed monthly. So it is checked against the K = 1 path on hand-built
partials, through the real consolidate_monthly:

  * equivalence — K = 1 and K = 4 give the same keys and the same sums, for ps
    and term, with negative and positive hashes and keys split across files;
  * any_value columns — each output value occurs among that key's own inputs,
    and parent_epd is never lost to a NULL from a deeper ply;
  * term slices on position_hash — every part row satisfies its predicate;
  * resume — finished parts are kept, a stale .tmp and another K's part dir are
    discarded, and no key is duplicated;
  * fail-fast — a failing month stops the run before the next month is built
    (the old loop ran every queued month first and logged nothing for hours);
  * sizing — 'auto' rounds up to a power of two; GB vs GiB as DuckDB reads them.

Run: .venv/Scripts/python.exe python/_test_consolidate_subbuckets.py
"""
from __future__ import annotations

import argparse
import contextlib
import io
import random
import shutil
import sys
import tempfile
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).parent))
from build_pooled_stats import (SUB_BUCKET_BASE_BYTES, SUB_BUCKET_BYTES_FACTOR,
                                SUB_BUCKET_MAX_INPUT_BYTES, _consolidate_sub_bucket,
                                _mem_bytes, _run_isolated, consolidate_monthly,
                                consolidation_spec, resolve_sub_buckets,
                                sub_buckets_arg)

K = 4
THREADS, MEM = 2, "1GB"
PS_KEY = ["parent_hash", "move_san", "event", "elo_band"]
TERM_KEY = ["position_hash", "kind", "reason"]
SUMS = ["white_wins", "draws", "black_wins", "total"]
ANY = ["parent_epd", "child_hash", "child_eval", "ply"]
PS_SCHEMA = {"parent_hash": pl.Int64, "move_san": pl.Utf8, "event": pl.Utf8,
             "elo_band": pl.Int64, "parent_epd": pl.Utf8, "child_hash": pl.Int64,
             "child_eval": pl.Int32, "ply": pl.Int32, "white_wins": pl.Int64,
             "draws": pl.Int64, "black_wins": pl.Int64, "total": pl.Int64}
TERM_SCHEMA = {"position_hash": pl.Int64, "kind": pl.Int32, "reason": pl.Int32,
               "white_wins": pl.Int64, "draws": pl.Int64, "black_wins": pl.Int64,
               "total": pl.Int64}
PS_MONTHLY = "year=2025_month=1.ps.parquet"
TERM_MONTHLY = "year=2025_month=1.term.parquet"
_checks: list[tuple[bool, str]] = []


def check(ok: bool, label: str) -> None:
    _checks.append((ok, label))
    print(f"  {'PASS' if ok else 'FAIL'}  {label}")


def make_partials(pdir: Path, seed: int = 7) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Three chunk files for 2025-01 (Blitz, Rapid, Blitz). The two Blitz chunks
    share ~a third of their keys, with different sums and, for some keys,
    different ply/EPD — a transposition reached at ply <= 16 in one chunk and
    deeper in the other."""
    rng = random.Random(seed)
    hashes = ([rng.getrandbits(64) - 2**63 for _ in range(40)]
              + [-(2**63), -9, -8, -1, 0, 1, 7, 8, 2**63 - 1])
    moves = ["e4", "d4", "Nf3", "c4", "O-O", "exd8=Q+"]
    bands = [1600, 2000, 2500]
    ps_all, term_all = [], []
    for c, ev in enumerate(["Blitz", "Rapid", "Blitz"]):
        stem = f"year=2025_month=1_event={ev}_part-0_c{c:03d}"
        keys = rng.sample([(h, mv, b) for h in hashes for mv in moves for b in bands], 300)
        rows = []
        for h, mv, b in keys:
            ply = rng.randint(1, 30)
            w, d, bl = rng.randint(0, 9), rng.randint(0, 4), rng.randint(0, 9)
            rows.append({"parent_hash": h, "move_san": mv, "event": ev, "elo_band": b,
                         "parent_epd": f"epd{h}" if ply <= 16 else None,
                         "child_hash": (h * 31 + moves.index(mv)) % (2**63),
                         "child_eval": rng.choice([None, rng.randint(-300, 300)]),
                         "ply": ply, "white_wins": w, "draws": d, "black_wins": bl,
                         "total": w + d + bl})
        ps = pl.DataFrame(rows, schema=PS_SCHEMA)
        ps.write_parquet(pdir / f"{stem}.ps.parquet")
        ps_all.append(ps)

        tkeys = rng.sample([(h, k, r) for h in hashes for k in (0, 1) for r in (0, 1, 2)], 120)
        term = pl.DataFrame(
            [{"position_hash": h, "kind": k, "reason": r, "white_wins": rng.randint(0, 5),
              "draws": rng.randint(0, 5), "black_wins": rng.randint(0, 5),
              "total": rng.randint(15, 20)} for h, k, r in tkeys], schema=TERM_SCHEMA)
        term.write_parquet(pdir / f"{stem}.term.parquet")
        term_all.append(term)
    return pl.concat(ps_all), pl.concat(term_all)


def main() -> None:
    print("=" * 70)
    print("SUB-BUCKETED MONTHLY CONSOLIDATION")
    print("=" * 70)
    tmp = Path(tempfile.mkdtemp(prefix="subbuckets_"))
    try:
        src = tmp / "src"
        src.mkdir()
        ps_in, term_in = make_partials(src)
        for name in ("k1", "k4", "resume"):
            shutil.copytree(src, tmp / name)
        spill = tmp / "spill"
        duck_tmp = str(spill / "_merge_duckdb_tmp")

        split = ps_in.group_by(PS_KEY).len().filter(pl.col("len") > 1).height
        check(split > 20 and (ps_in["parent_hash"] < 0).any() and (ps_in["parent_hash"] > 0).any(),
              f"fixture: {split} ps keys split across files, hashes of both signs")

        # ── equivalence ──────────────────────────────────────────────────────
        m1 = consolidate_monthly(tmp / "k1", THREADS, MEM, spill, ("ps", "term"), sub_buckets=1)
        m4 = consolidate_monthly(tmp / "k4", THREADS, MEM, spill, ("ps", "term"), sub_buckets=K)
        a_ps = pl.read_parquet(m1 / PS_MONTHLY).sort(PS_KEY)
        b_ps = pl.read_parquet(m4 / PS_MONTHLY).sort(PS_KEY)
        check(a_ps.select(PS_KEY + SUMS).equals(b_ps.select(PS_KEY + SUMS)),
              f"ps: K=1 and K={K} give identical keys and sums ({b_ps.height} rows)")
        check(a_ps.schema == b_ps.schema, "ps: identical columns, order and types")
        n_keys = ps_in.select(PS_KEY).unique().height
        check(b_ps.height == n_keys == b_ps.select(PS_KEY).unique().height,
              f"ps: exactly one row per distinct input key ({n_keys})")
        check(b_ps.select(SUMS).sum().row(0) == ps_in.select(SUMS).sum().row(0),
              "ps: all four summed columns conserved against the partials")
        a_t = pl.read_parquet(m1 / TERM_MONTHLY).sort(TERM_KEY)
        b_t = pl.read_parquet(m4 / TERM_MONTHLY).sort(TERM_KEY)
        check(a_t.equals(b_t) and b_t.height == term_in.select(TERM_KEY).unique().height,
              f"term: K=1 and K={K} identical, one row per key ({b_t.height})")
        leftovers = [p.name for p in m4.iterdir()
                     if p.name.startswith("_tmp_") or p.name.endswith(".tmp")]
        check(not leftovers, f"no part dir or .tmp survives a finished month {leftovers or ''}")

        # ── any_value columns ────────────────────────────────────────────────
        for col in ANY:
            fill = "<null>" if col == "parent_epd" else -(10**9)
            out = b_ps.select(PS_KEY + [col]).with_columns(pl.col(col).fill_null(fill))
            inp = ps_in.select(PS_KEY + [col]).with_columns(pl.col(col).fill_null(fill)).unique()
            check(out.join(inp, on=PS_KEY + [col], how="semi").height == out.height,
                  f"any_value({col}) holds a value from that key's own inputs")
        with_epd = ps_in.filter(pl.col("parent_epd").is_not_null()).select(PS_KEY).unique()
        lost = (b_ps.join(with_epd, on=PS_KEY, how="semi")
                .filter(pl.col("parent_epd").is_null()).height)
        check(lost == 0 and with_epd.height > 0,
              f"parent_epd kept for all {with_epd.height} keys where any input row has one")

        # ── term slices on position_hash ─────────────────────────────────────
        grp, sums = consolidation_spec("term")
        check(grp.split(",")[0].strip() == "position_hash",
              "term's first key column, the one sliced on, is position_hash")
        term_files = sorted(str(p) for p in src.glob("*.term.parquet"))
        wrong = total = 0
        for j in range(K):
            part = tmp / f"term-slice-{j}.parquet"
            _run_isolated(_consolidate_sub_bucket, (grp, sums, term_files, "position_hash",
                                                    K, j, str(part), THREADS, MEM, duck_tmp))
            hashes = pl.read_parquet(part)["position_hash"].to_list()
            total += len(hashes)
            # Python's % is floor-mod: the bucket DuckDB's double-modulo computes.
            wrong += sum(1 for h in hashes if h % K != j)
        check(wrong == 0 and total == b_t.height,
              f"term slice j holds only position_hash mod {K} == j; slices cover all {total} keys")

        # ── resume inside a month ────────────────────────────────────────────
        rdir = tmp / "resume"
        rm = rdir / "_monthly"
        rm.mkdir()
        grp, sums = consolidation_spec("ps")
        ps_files = sorted(str(p) for p in rdir.glob("*.ps.parquet"))
        part_dir = rm / f"_tmp_year=2025_month=1.ps.k{K}"
        part_dir.mkdir()
        for j in (0, 1):
            _run_isolated(_consolidate_sub_bucket,
                          (grp, sums, ps_files, "parent_hash", K, j,
                           str(part_dir / f"part-{j:04d}.parquet"), THREADS, MEM, duck_tmp))
        (part_dir / "part-0002.parquet.tmp").write_bytes(b"half-written by a killed worker")
        other_k = rm / "_tmp_year=2025_month=1.ps.k2"
        other_k.mkdir()
        (other_k / "part-0000.parquet").write_bytes(b"sliced two ways, not four")
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            consolidate_monthly(rdir, THREADS, MEM, spill, ("ps",), sub_buckets=K)
        log = buf.getvalue()
        kept = log.count("kept from an earlier run")
        check(kept == 2, f"resume keeps the 2 finished parts and builds only the other {K - 2} "
                         f"(kept {kept})")
        check(not other_k.exists() and "discarding" in log,
              "a part dir sliced with a different K is discarded, not mixed in")
        r_ps = pl.read_parquet(rm / PS_MONTHLY).sort(PS_KEY)
        check(r_ps.select(PS_KEY + SUMS).equals(a_ps.select(PS_KEY + SUMS))
              and r_ps.height == n_keys,
              "resumed month equals the K=1 monthly: no key lost, none duplicated")
        check(not part_dir.exists(), "part dir removed once the monthly is written")

        # ── fail-fast ────────────────────────────────────────────────────────
        good = src / "year=2025_month=1_event=Blitz_part-0_c000.ps.parquet"
        for nsub in (1, 2):
            ff = tmp / f"failfast_k{nsub}"
            ff.mkdir()
            (ff / "year=2025_month=1_event=Blitz_part-0_c000.ps.parquet").write_bytes(
                b"PAR1 truncated, not a parquet file PAR1")
            shutil.copy(good, ff / "year=2025_month=2_event=Blitz_part-0_c000.ps.parquet")
            raised = False
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    consolidate_monthly(ff, THREADS, MEM, spill, ("ps",), sub_buckets=nsub)
            except Exception:                                        # noqa: BLE001
                raised = True
            check(raised and not (ff / "_monthly" / "year=2025_month=2.ps.parquet").exists(),
                  f"sub_buckets={nsub}: a failing month raises before the next month is built")

        # ── sizing ───────────────────────────────────────────────────────────
        gb = 10**9
        check(resolve_sub_buckets("auto", 0, "48GB") == 1
              and resolve_sub_buckets("auto", gb // 2, "48GB") == 1,
              "auto: a small month runs as one query")
        # 46.91 GB is 2026-01. Memory alone would need 13 -> 16; the slice-size cap
        # (<= 1.5 GB of input per slice) needs 32.
        check(resolve_sub_buckets("auto", int(46.91 * gb), "48GB") == 32,
              "auto: a 47 GB month at --mem 48GB is speed-bound -> 32")
        # At 16 GB (PICOBYTE) memory binds: 12 GB usable -> 45 -> 64.
        check(resolve_sub_buckets("auto", int(44.67 * gb), "16GB") == 64,
              "auto: a 45 GB month at --mem 16GB is memory-bound -> 64")
        k = resolve_sub_buckets("auto", 3 * SUB_BUCKET_MAX_INPUT_BYTES + 1, "48GB")
        check(k == 4, f"auto: rounds up to a power of two (3 slices' worth + 1 byte -> {k})")
        k = resolve_sub_buckets("auto", int(20 * gb), "12GB")
        check(SUB_BUCKET_BASE_BYTES + SUB_BUCKET_BYTES_FACTOR * 20 * gb / k <= 12 * gb
              and k & (k - 1) == 0, f"auto: the chosen K ({k}) fits the stated memory model")
        check(resolve_sub_buckets(3, 10**12, "48GB") == 3, "an explicit K is used as given")
        check(_mem_bytes("24GB") == 24 * 10**9 and _mem_bytes("24GiB") == 24 * 2**30
              and _mem_bytes("500mb") == 500 * 10**6,
              "memory sizes: GB = 1000^3, GiB = 1024^3, case-insensitive")
        try:
            sub_buckets_arg("0")
            rejected = False
        except argparse.ArgumentTypeError:
            rejected = True
        check(sub_buckets_arg("AUTO") == "auto" and sub_buckets_arg("16") == 16 and rejected,
              "--sub-buckets accepts auto or a positive integer, rejects 0")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    n_fail = sum(1 for ok, _ in _checks if not ok)
    print(f"{'ALL PASS' if n_fail == 0 else f'{n_fail} FAILURES'} ({len(_checks)} checks)")
    sys.exit(0 if n_fail == 0 else 1)


if __name__ == "__main__":
    main()
